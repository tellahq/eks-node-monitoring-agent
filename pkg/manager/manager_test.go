package manager_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
	"github.com/aws/eks-node-monitoring-agent/api/monitor/resource"
	"github.com/aws/eks-node-monitoring-agent/pkg/manager"
)

type mockMonitor struct {
	registerFunc func(ctx context.Context, mgr monitor.Manager) error
}

func (m *mockMonitor) Name() string                    { return "mock" }
func (m *mockMonitor) Conditions() []monitor.Condition { return []monitor.Condition{} }
func (m *mockMonitor) Register(ctx context.Context, mgr monitor.Manager) error {
	return m.registerFunc(ctx, mgr)
}

// notifyChanBufferSize is sized at the worst case for the slowest test in
// this file: ~12 seconds of test runtime, ~2 polling cycles (5s each), each
// emitting up to ~3 conditions per monitor — so an unread chan would back up
// at most ~6-10 items. 256 is comfortably more than that, with no realistic
// risk of test deadlock. The earlier unbuffered version would block runLoop
// on the first unread Fatal.
const notifyChanBufferSize = 256

func NewManagerWithExporterFuncs(fns ...func(*mockExporter)) (*manager.MonitorManager, *mockExporter) {
	mockExp := &mockExporter{
		notifyChan: make(chan struct{}, notifyChanBufferSize),
	}
	for _, fn := range fns {
		fn(mockExp)
	}
	mockManager := manager.NewMonitorManager("mock", mockExp)
	return mockManager, mockExp
}

// NewManagerWithOptions is a test helper for cases that need to construct
// the MonitorManager with non-default options (e.g. a tighter recovery
// threshold). Mirrors NewManagerWithExporterFuncs but plumbs through opts.
func NewManagerWithOptions(opts ...manager.Option) (*manager.MonitorManager, *mockExporter) {
	mockExp := &mockExporter{notifyChan: make(chan struct{}, notifyChanBufferSize)}
	mockManager := manager.NewMonitorManager("mock", mockExp, opts...)
	return mockManager, mockExp
}

// mockExporter implements manager.Exporter for tests. Info/Warning/Fatal push
// onto notifyChan so tests can synchronously block-and-wait for an export.
// Healthy increments healthyCount atomically; tests assert on the counter
// value, which is more reliable than a buffered-channel idiom that loses
// signal under racy schedules and can hide regressions where Healthy is
// called too often.
type mockExporter struct {
	notifyChan    chan struct{}
	healthyCount  atomic.Int64
	healthyByType sync.Map // map[corev1.NodeConditionType]int64
}

func (e *mockExporter) notify() error {
	e.notifyChan <- struct{}{}
	return nil
}
func (e *mockExporter) Info(context.Context, monitor.Condition, corev1.NodeConditionType) error {
	return e.notify()
}
func (e *mockExporter) Warning(context.Context, monitor.Condition, corev1.NodeConditionType) error {
	return e.notify()
}
func (e *mockExporter) Fatal(context.Context, monitor.Condition, corev1.NodeConditionType) error {
	return e.notify()
}
func (e *mockExporter) Healthy(_ context.Context, ct corev1.NodeConditionType) error {
	e.healthyCount.Add(1)
	v, _ := e.healthyByType.LoadOrStore(ct, new(int64))
	atomic.AddInt64(v.(*int64), 1)
	return nil
}

// healthyCalls returns the total number of Healthy invocations across all
// condition types.
func (e *mockExporter) healthyCalls() int64 {
	return e.healthyCount.Load()
}

func TestManager_Notification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
	defer cancel()

	mockMon := &mockMonitor{
		registerFunc: func(ctx context.Context, mgr monitor.Manager) error {
			go mgr.Notify(ctx, monitor.Condition{
				Reason:   "ExampleReason",
				Severity: monitor.SeverityFatal,
			})
			return nil
		},
	}
	mMgr, mockExp := NewManagerWithExporterFuncs()
	if err := mMgr.Register(ctx, mockMon, "MockPassed"); err != nil {
		t.Fatal(err)
	}
	go mMgr.Start(ctx)

	select {
	case <-mockExp.notifyChan:
		// Notification was received by the exporter — the manager correctly
		// routed the condition from the monitor through to the exporter.
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestManager_MinOccurrences(t *testing.T) {
	t.Run("Met", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
		defer cancel()

		mockMon := &mockMonitor{
			registerFunc: func(ctx context.Context, mgr monitor.Manager) error {
				go mgr.Notify(ctx, monitor.Condition{
					Reason:         "ExampleReason",
					Severity:       monitor.SeverityFatal,
					MinOccurrences: 0,
				})
				return nil
			},
		}
		mMgr, mockExp := NewManagerWithExporterFuncs()
		if err := mMgr.Register(ctx, mockMon, "MockPassed"); err != nil {
			t.Fatal(err)
		}
		go mMgr.Start(ctx)

		select {
		case <-mockExp.notifyChan:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	})

	t.Run("NotMet", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
		defer cancel()

		mockMon := &mockMonitor{
			registerFunc: func(ctx context.Context, mgr monitor.Manager) error {
				go mgr.Notify(ctx, monitor.Condition{
					Reason:         "ExampleReason",
					Severity:       monitor.SeverityFatal,
					MinOccurrences: 2,
				})
				return nil
			},
		}
		mMgr, mockExp := NewManagerWithExporterFuncs()
		if err := mMgr.Register(ctx, mockMon, "MockPassed"); err != nil {
			t.Fatal(err)
		}
		go mMgr.Start(ctx)

		select {
		case n := <-mockExp.notifyChan:
			t.Fatalf("expected no events on channel but got %+v", n)
		case <-ctx.Done():
			// expected to timeout because min occurrences was not met.
		}
	})
}

// this tests the creation of an observable resource from end to end.
func TestManager_CreateObserver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mMgr, _ := NewManagerWithExporterFuncs()
	_, err := mMgr.Subscribe(resource.ResourceTypeFile, []resource.Part{"/tmp/foobar"})
	assert.NoError(t, err)
	assert.NoError(t, mMgr.Start(ctx))
}

// TestManager_AutoRecoveryAfterFatalGoesQuiet verifies the manager flips a
// managed condition back to Healthy when a monitor that previously emitted
// Fatal subsequently does not emit Fatal for at least the recovery
// threshold. This is the missing Fatal -> True transition path: monitors
// signal recovery via absence of Fatal (see
// monitors/nvidia/dcgm/dcgm_reconcile.go: success returns nil/nil), and the
// framework now interprets that absence after a quiet period as "recovered"
// rather than "no signal".
//
// Test takes ~5s because the manager's poll ticker is hardcoded at 5s.
func TestManager_AutoRecoveryAfterFatalGoesQuiet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Monitor pushes a single Fatal via Notify on Register, then its
	// Conditions() returns [] forever. After the recovery threshold, the
	// manager's poll loop should auto-call SetHealthy.
	mockMon := &mockMonitor{
		registerFunc: func(ctx context.Context, mgr monitor.Manager) error {
			go func() {
				_ = mgr.Notify(ctx, monitor.Condition{
					Reason:   "DCGMError",
					Severity: monitor.SeverityFatal,
				})
			}()
			return nil
		},
	}

	mMgr, mockExp := NewManagerWithOptions(manager.WithRecoveryThreshold(50 * time.Millisecond))

	if err := mMgr.Register(ctx, mockMon, "AcceleratedHardwareReady"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = mMgr.Start(ctx) }()

	// Drain the initial Fatal.
	select {
	case <-mockExp.notifyChan:
	case <-ctx.Done():
		t.Fatal("Fatal was never delivered to exporter:", ctx.Err())
	}

	// Then expect Healthy on a subsequent poll cycle (after recovery
	// threshold elapses and the poll ticker fires — within ~5-10s).
	for {
		if mockExp.healthyCalls() >= 1 {
			return
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("auto-recovery never fired (Healthy not called): %v", ctx.Err())
		}
	}
}

// TestManager_NoRecoveryWhileMonitorStillReportingFatal asserts the manager
// does NOT auto-recover while the monitor is still reporting a Fatal on each
// poll. monitorFatalAt should keep getting bumped, preventing the quiet
// period from elapsing.
func TestManager_NoRecoveryWhileMonitorStillReportingFatal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	mockMon := &chattyFatalMonitor{}

	mMgr, mockExp := NewManagerWithOptions(manager.WithRecoveryThreshold(50 * time.Millisecond))

	if err := mMgr.Register(ctx, mockMon, "AcceleratedHardwareReady"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = mMgr.Start(ctx) }()

	// Wait the full test window. Healthy should never be called — the
	// monitor keeps emitting Fatal so the recovery clock keeps resetting.
	gotFatal := false
	deadline := time.NewTimer(11 * time.Second)
	defer deadline.Stop()
loop:
	for {
		select {
		case <-mockExp.notifyChan:
			gotFatal = true
		case <-deadline.C:
			break loop
		case <-ctx.Done():
			break loop
		}
	}
	if !gotFatal {
		t.Fatalf("never received a Fatal — test setup wrong")
	}
	if calls := mockExp.healthyCalls(); calls != 0 {
		t.Fatalf("Healthy was called %d times while monitor was still reporting Fatal — auto-recovery should not fire", calls)
	}
}

// TestManager_AutoRecoveryReArmsAfterRecovery verifies a Fatal -> recover
// -> Fatal -> recover cycle works correctly. The first recovery clears
// monitorFatalAt; the second Fatal must re-arm it so the second recovery
// fires after another full quiet period (not immediately).
func TestManager_AutoRecoveryReArmsAfterRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	mon := &switchableMonitor{}
	mMgr, mockExp := NewManagerWithOptions(manager.WithRecoveryThreshold(50 * time.Millisecond))

	if err := mMgr.Register(ctx, mon, "AcceleratedHardwareReady"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = mMgr.Start(ctx) }()

	// Phase 1: monitor returns Fatal — wait for a delivered Fatal. Drain
	// notifyChan to empty so phase 3's check below can't be confused by
	// leftover buffered notifications from this phase.
	mon.setEmit("fatal")
	if err := waitFor(ctx, func() bool { return mockExp.healthyCalls() == 0 && mockExp.fatalDelivered() }, 8*time.Second); err != nil {
		t.Fatalf("phase 1: never observed Fatal delivery: %v", err)
	}
	mockExp.drainNotify()

	// Phase 2: monitor goes quiet — first recovery should fire.
	mon.setEmit("none")
	wantCalls := int64(1)
	if err := waitFor(ctx, func() bool { return mockExp.healthyCalls() >= wantCalls }, 10*time.Second); err != nil {
		t.Fatalf("phase 2: first recovery never fired: %v", err)
	}
	mockExp.drainNotify()

	// Phase 3: monitor goes Fatal again — recovery clock must re-arm. The
	// drain above guarantees fatalDelivered() can only succeed via a fresh
	// Fatal emitted in this phase, not via stale signal from phase 1.
	mon.setEmit("fatal")
	if err := waitFor(ctx, func() bool { return mockExp.fatalDelivered() }, 8*time.Second); err != nil {
		t.Fatalf("phase 3: never observed re-Fatal delivery: %v", err)
	}

	// Phase 4: monitor quiet again — second recovery should fire (counter > 1).
	mon.setEmit("none")
	wantCalls = mockExp.healthyCalls() + 1
	if err := waitFor(ctx, func() bool { return mockExp.healthyCalls() >= wantCalls }, 10*time.Second); err != nil {
		t.Fatalf("phase 4: second recovery never fired (re-arm broken): %v", err)
	}
}

// chattyFatalMonitor returns a Fatal condition every time it's polled.
type chattyFatalMonitor struct{}

func (m *chattyFatalMonitor) Name() string { return "chatty" }
func (m *chattyFatalMonitor) Conditions() []monitor.Condition {
	return []monitor.Condition{{Reason: "DCGMError", Severity: monitor.SeverityFatal}}
}
func (m *chattyFatalMonitor) Register(ctx context.Context, mgr monitor.Manager) error {
	return nil
}

// switchableMonitor lets a test toggle what the monitor emits between
// polls. emit values: "fatal" -> [Fatal], "none" -> [].
type switchableMonitor struct {
	emit atomic.Value // string
}

func (m *switchableMonitor) setEmit(v string)                                     { m.emit.Store(v) }
func (m *switchableMonitor) Name() string                                         { return "switchable" }
func (m *switchableMonitor) Register(context.Context, monitor.Manager) error { return nil }
func (m *switchableMonitor) Conditions() []monitor.Condition {
	v, _ := m.emit.Load().(string)
	if v == "fatal" {
		return []monitor.Condition{{Reason: "DCGMError", Severity: monitor.SeverityFatal}}
	}
	return nil
}

// waitFor polls predicate every 100ms until true or timeout.
func waitFor(ctx context.Context, predicate func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if predicate() {
			return nil
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// fatalDelivered does a non-blocking receive on notifyChan and returns
// true iff at least one delivery was buffered. Tests that need to observe
// "Fatal was delivered IN THIS PHASE" should call drainNotify between
// phases — otherwise this can falsely succeed from a stale buffered
// notification left over by an earlier phase.
func (e *mockExporter) fatalDelivered() bool {
	select {
	case <-e.notifyChan:
		return true
	default:
		return false
	}
}

// drainNotify empties notifyChan without blocking. Use between phases of
// a multi-phase test to ensure fatalDelivered() in the next phase only
// observes signal emitted during that phase.
func (e *mockExporter) drainNotify() {
	for {
		select {
		case <-e.notifyChan:
		default:
			return
		}
	}
}
