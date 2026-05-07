package manager_test

import (
	"context"
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

func NewManagerWithExporterFuncs(fns ...func(*mockExporter)) (*manager.MonitorManager, *mockExporter) {
	mockExp := &mockExporter{
		notifyChan:     make(chan struct{}),
		setHealthyChan: make(chan struct{}, 1),
	}
	for _, fn := range fns {
		fn(mockExp)
	}
	mockManager := manager.NewMonitorManager("mock", mockExp)
	return mockManager, mockExp
}

type mockExporter struct {
	// notifyChan receives a struct{} for every Info/Warning/Fatal call.
	notifyChan chan struct{}
	// setHealthyChan receives a struct{} for every SetHealthy call. Buffered
	// (size 1) so tests can assert "got at least one" without racing against
	// the send. Tests select on this channel to verify the auto-recovery
	// path fired.
	setHealthyChan chan struct{}
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
func (e *mockExporter) SetHealthy(context.Context, corev1.NodeConditionType) error {
	select {
	case e.setHealthyChan <- struct{}{}:
	default:
		// non-blocking — tests only need to know SetHealthy fired at least
		// once, and the manager polls on a ticker so subsequent calls are
		// expected.
	}
	return nil
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
// Fatal subsequently returns no conditions for at least the recovery
// threshold. This is the missing Fatal -> True transition path: monitors
// signal recovery via absence of conditions (see
// monitors/nvidia/dcgm/dcgm_reconcile.go: success path returns nil/nil), and
// the framework now interprets that absence after a quiet period as
// "recovered" rather than "no signal".
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

	mMgr, mockExp := NewManagerWithExporterFuncs()
	// Tighten the threshold so we don't have to wait the default 30s; the
	// poll cycle is still 5s so a recovery in the second poll is the fastest
	// observable signal.
	mMgr.SetRecoveryThreshold(50 * time.Millisecond)

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

	// Then expect SetHealthy on a subsequent poll cycle (after recovery
	// threshold elapses and the poll ticker fires — within ~5-10s).
	select {
	case <-mockExp.setHealthyChan:
		// pass
	case <-ctx.Done():
		t.Fatal("auto-recovery never fired (SetHealthy not called):", ctx.Err())
	}
}

// TestManager_NoRecoveryWhileMonitorStillReportingFatal asserts that the
// manager does NOT auto-recover while the monitor is still reporting a Fatal
// on each poll. monitorFatalAt should keep getting bumped, preventing the
// quiet-period from elapsing.
func TestManager_NoRecoveryWhileMonitorStillReportingFatal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	// Monitor that returns Fatal on every Conditions() poll (steady-state
	// genuine failure).
	mockMon := &chattyFatalMonitor{}

	mMgr, mockExp := NewManagerWithExporterFuncs()
	mMgr.SetRecoveryThreshold(50 * time.Millisecond)

	if err := mMgr.Register(ctx, mockMon, "AcceleratedHardwareReady"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = mMgr.Start(ctx) }()

	// Drain Fatal notifications as they arrive (the polling loop sends one
	// every 5s). We expect at least one within the test window.
	gotFatal := false
	for {
		select {
		case <-mockExp.notifyChan:
			gotFatal = true
		case <-mockExp.setHealthyChan:
			t.Fatal("SetHealthy was called while monitor was still reporting Fatal — auto-recovery should not fire")
		case <-ctx.Done():
			if !gotFatal {
				t.Fatal("never received a Fatal — test setup wrong:", ctx.Err())
			}
			return
		}
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
