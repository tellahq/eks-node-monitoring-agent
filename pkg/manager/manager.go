package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
	"github.com/aws/eks-node-monitoring-agent/api/monitor/resource"
	"github.com/aws/eks-node-monitoring-agent/pkg/observer"
)

var (
	conditionCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "problem_condition_count"},
		[]string{"severity", "reason"},
	)
	conditionTypeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "fatal_condition_gauge"},
		[]string{"type"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		conditionCount,
		conditionTypeGauge,
	)
}

// defaultRecoveryThreshold is how long a monitor must go without emitting
// a Fatal condition before the manager auto-recovers its managed condition
// back to Healthy. Sized at ~6 polling cycles (poll interval is 5s) so
// brief transients during reconcile don't bounce a True/False flap, and
// well below Karpenter NodeRepair's per-condition tolerationDuration
// (which is on the order of minutes for most conditions) so a transient
// False that resolves quickly doesn't false-positive trigger node
// replacement.
const defaultRecoveryThreshold = 30 * time.Second

// MonitorManager manages the lifecycle of monitors and routes their notifications
type MonitorManager struct {
	nodeName          string
	monitors          map[string]monitor.Monitor
	conditionTypeMap  map[string]corev1.NodeConditionType
	conditionCountMap map[string]int64
	observers         map[string]observer.Observer
	notifyChan        chan notification
	exporter          Exporter

	// monitorFatalAt tracks the most recent time each monitor emitted a
	// Fatal condition that was actually delivered to the exporter (i.e.
	// not suppressed by MinOccurrences). Used by the auto-recovery path:
	// a monitor that's been quiet for `recoveryThreshold` after its last
	// Fatal triggers SetHealthy on its managed condition.
	//
	// Protected by monitorFatalAtMu. In practice everything that touches
	// it runs from a single goroutine inside runLoop, but exporting
	// SetRecoveryThreshold and the public-ness of MonitorManager mean a
	// future caller could race; the lock is cheap insurance.
	monitorFatalAt    map[string]time.Time
	monitorFatalAtMu  sync.Mutex
	recoveryThreshold time.Duration
}

type notification struct {
	monitorName string
	condition   monitor.Condition
}

// NewMonitorManager creates a new monitor manager
func NewMonitorManager(nodeName string, exporter Exporter) *MonitorManager {
	return &MonitorManager{
		nodeName:          nodeName,
		monitors:          make(map[string]monitor.Monitor),
		conditionTypeMap:  make(map[string]corev1.NodeConditionType),
		conditionCountMap: make(map[string]int64),
		observers:         make(map[string]observer.Observer),
		notifyChan:        make(chan notification, 100),
		exporter:          exporter,
		monitorFatalAt:    make(map[string]time.Time),
		recoveryThreshold: defaultRecoveryThreshold,
	}
}

// Register registers a monitor with the manager.
//
// Each monitor must own its conditionType uniquely: the auto-recovery path
// and the Fatal-counting Prometheus gauge both assume one-monitor-per-
// conditionType (otherwise two monitors mapped to the same conditionType
// could fight over the recovery timestamp + gauge state). Register rejects
// double-registrations and conditionType collisions to make the invariant
// load-bearing.
func (m *MonitorManager) Register(ctx context.Context, mon monitor.Monitor, conditionType corev1.NodeConditionType) error {
	if _, exists := m.monitors[mon.Name()]; exists {
		return fmt.Errorf("monitor %q is already registered", mon.Name())
	}
	for existingName, existingType := range m.conditionTypeMap {
		if existingType == conditionType {
			return fmt.Errorf("conditionType %q is already owned by monitor %q", conditionType, existingName)
		}
	}
	m.monitors[mon.Name()] = mon
	m.conditionTypeMap[mon.Name()] = conditionType
	return mon.Register(ctx, makeManagerWrapper(m, mon))
}

// SetRecoveryThreshold overrides the auto-recovery quiet-period threshold
// (default: defaultRecoveryThreshold). Primarily exposed so tests can drive
// recovery without burning real wall-clock time.
func (m *MonitorManager) SetRecoveryThreshold(d time.Duration) {
	m.recoveryThreshold = d
}

// Start starts all observers and begins processing notifications
func (m *MonitorManager) Start(ctx context.Context) error {
	logger := log.FromContext(ctx)

	// Start all observers
	for id, obs := range m.observers {
		obsLogger := logger.WithValues("observer", id)
		obsLogger.Info("starting observer")
		go func(o observer.Observer, l logr.Logger) {
			obsCtx := log.IntoContext(ctx, l)
			if err := o.Init(obsCtx); err != nil {
				l.Error(err, "observer failed")
			}
		}(obs, obsLogger)
	}

	// Process notifications
	return m.runLoop(ctx)
}

func (m *MonitorManager) runLoop(ctx context.Context) error {
	logger := log.FromContext(ctx)

	// Poll ticker for periodic condition checks
	pollTicker := time.NewTicker(5 * time.Second)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pollTicker.C:
			// Poll monitors for their current conditions. After processing
			// each monitor's response, decide whether to auto-recover its
			// managed condition: if the monitor did NOT emit a delivered
			// Fatal this cycle, and we previously recorded a Fatal for it,
			// and enough wall-clock time has elapsed since that Fatal, flip
			// the managed condition back to Healthy. This restores the
			// missing Fatal -> True transition path. The predicate is
			// "no Fatal this cycle" (not "no conditions at all") because
			// non-Fatal severities (Info/Warning) only produce events and
			// don't write the managed condition — so their presence isn't
			// evidence of an unhealthy managed-condition state.
			for _, mon := range m.monitors {
				conds := mon.Conditions()
				sawFatalDelivered := false
				for _, cond := range conds {
					delivered, err := m.exportCondition(ctx, mon.Name(), cond)
					if err != nil {
						logger.Error(err, "failed to export condition", "source", mon.Name(), "condition", cond)
					}
					if delivered && cond.Severity == monitor.SeverityFatal {
						sawFatalDelivered = true
					}
				}
				if sawFatalDelivered {
					m.recordFatalAt(mon.Name(), time.Now())
				} else {
					m.maybeAutoRecover(ctx, mon.Name())
				}
			}
		case notif := <-m.notifyChan:
			delivered, err := m.exportCondition(ctx, notif.monitorName, notif.condition)
			if err != nil {
				logger.Error(err, "failed to export condition",
					"monitor", notif.monitorName,
					"condition", notif.condition,
				)
			}
			if delivered && notif.condition.Severity == monitor.SeverityFatal {
				m.recordFatalAt(notif.monitorName, time.Now())
			}
		}
	}
}

// recordFatalAt notes that the named monitor emitted a delivered Fatal at
// the given time. The auto-recovery path treats this as the start of the
// quiet-period clock; the next clean polling cycle (no Fatal) at least
// recoveryThreshold later flips the managed condition back to Healthy.
func (m *MonitorManager) recordFatalAt(monitorName string, t time.Time) {
	m.monitorFatalAtMu.Lock()
	defer m.monitorFatalAtMu.Unlock()
	m.monitorFatalAt[monitorName] = t
}

// maybeAutoRecover flips the managed condition for the named monitor back to
// Healthy if it previously emitted Fatal and has been quiet for at least
// recoveryThreshold. No-op if the monitor never emitted Fatal, or if not
// enough time has passed since the most recent Fatal.
func (m *MonitorManager) maybeAutoRecover(ctx context.Context, monitorName string) {
	logger := log.FromContext(ctx)

	m.monitorFatalAtMu.Lock()
	fatalAt, ok := m.monitorFatalAt[monitorName]
	m.monitorFatalAtMu.Unlock()
	if !ok {
		return
	}
	if time.Since(fatalAt) < m.recoveryThreshold {
		return
	}
	conditionType, ok := m.conditionTypeMap[monitorName]
	if !ok {
		// Defensive: a monitor in monitorFatalAt without a conditionType
		// means Register's invariants were violated. Should be unreachable
		// given Register populates both maps atomically; log and bail.
		logger.Info("monitor has Fatal timestamp but no conditionType mapping; skipping recovery",
			"monitor", monitorName,
		)
		return
	}
	if err := m.exporter.SetHealthy(ctx, conditionType); err != nil {
		logger.Error(err, "failed to auto-recover condition",
			"monitor", monitorName,
			"conditionType", conditionType,
		)
		return
	}
	logger.Info("auto-recovered managed condition to healthy",
		"monitor", monitorName,
		"conditionType", conditionType,
		"quietFor", time.Since(fatalAt).Round(time.Second).String(),
	)
	m.monitorFatalAtMu.Lock()
	delete(m.monitorFatalAt, monitorName)
	m.monitorFatalAtMu.Unlock()
	conditionTypeGauge.WithLabelValues(string(conditionType)).Set(0)
}

// exportCondition forwards a condition from a monitor to the exporter,
// applying the MinOccurrences gate first. Returns (delivered, err) where
// delivered=true iff the condition was actually sent to the exporter
// (i.e. not suppressed by MinOccurrences). Callers use this to decide
// whether to update bookkeeping that depends on the condition having
// actually been delivered (e.g. monitorFatalAt for the auto-recovery
// timer).
func (m *MonitorManager) exportCondition(ctx context.Context, monitorName string, condition monitor.Condition) (bool, error) {
	logger := log.FromContext(ctx).WithValues("source", monitorName, "condition", condition)

	// track condition metrics
	conditionCount.WithLabelValues(string(condition.Severity), condition.Reason).Add(1)

	conditionType, ok := m.conditionTypeMap[monitorName]
	if !ok {
		return false, fmt.Errorf("missing condition type mapping for monitor: %s", monitorName)
	}
	logger = logger.WithValues("conditionType", conditionType)

	// Skip requests for conditions that have not met their minimum occurrences
	if m.conditionCountMap[condition.Reason] < condition.MinOccurrences {
		logger.Info("condition has not met MinOccurrences", "occurrences", m.conditionCountMap[condition.Reason])
		m.conditionCountMap[condition.Reason] += 1
		return false, nil
	}
	m.conditionCountMap[condition.Reason] = 0

	if err := m.SendCondition(ctx, condition, conditionType); err != nil {
		return false, err
	}
	return true, nil
}

// SendCondition sends a condition to the exporter based on severity
func (m *MonitorManager) SendCondition(ctx context.Context, condition monitor.Condition, conditionType corev1.NodeConditionType) error {
	log.FromContext(ctx).Info("sending condition to exporter", "condition", condition, "conditionType", conditionType)
	switch condition.Severity {
	case monitor.SeverityInfo:
		return m.exporter.Info(ctx, condition, conditionType)
	case monitor.SeverityWarning:
		return m.exporter.Warning(ctx, condition, conditionType)
	case monitor.SeverityFatal:
		for _, cType := range m.conditionTypeMap {
			if conditionType == cType {
				conditionTypeGauge.WithLabelValues(string(cType)).Set(1.0)
			}
		}
		return m.exporter.Fatal(ctx, condition, conditionType)
	default:
		return fmt.Errorf("invalid condition severity: %q", condition.Severity)
	}
}

// Subscribe implements the monitor.Manager interface for resource subscriptions
func (m *MonitorManager) Subscribe(rType resource.Type, rParts []resource.Part) (<-chan string, error) {
	rID := resourceID(rType, rParts)
	obs, ok := m.observers[rID]
	if !ok {
		constructor, ok := observer.ObserverConstructorMap[rType]
		if !ok {
			return nil, fmt.Errorf("the resource type %q was not handled", rType)
		}
		var err error
		if obs, err = constructor(rParts); err != nil {
			return nil, err
		}
		m.observers[rID] = obs
	}
	return obs.Subscribe(), nil
}

// makeManagerWrapper creates a wrapper that implements monitor.Manager for a specific monitor
func makeManagerWrapper(monMgr *MonitorManager, mon monitor.Monitor) *managerWrapper {
	return &managerWrapper{
		MonitorManager: monMgr,
		notifyFunc: func(ctx context.Context, condition monitor.Condition) error {
			notif := notification{
				monitorName: mon.Name(),
				condition:   condition,
			}
			select {
			case monMgr.notifyChan <- notif:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
}

var _ monitor.Manager = (*managerWrapper)(nil)

// managerWrapper implements the Manager interface from the monitoring API
// package which scopes the notify call to the manager.
type managerWrapper struct {
	*MonitorManager
	notifyFunc func(ctx context.Context, condition monitor.Condition) error
}

func (m *managerWrapper) Notify(ctx context.Context, cond monitor.Condition) error {
	return m.notifyFunc(ctx, cond)
}

// resourceID creates a unique identifier for a resource subscription
func resourceID(rType resource.Type, rParts []resource.Part) string {
	id := string(rType)
	for _, part := range rParts {
		id += "-" + string(part)
	}
	return id
}
