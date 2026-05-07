package manager_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
	"github.com/aws/eks-node-monitoring-agent/pkg/manager"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeEventRecorder struct {
	record.EventRecorder

	events corev1.EventList
}

func (r *fakeEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	r.events.Items = append(r.events.Items, corev1.Event{
		Type:    eventtype,
		Reason:  reason,
		Message: message,
	})
}

func TestNodeExporter_EventsRecordedImmediately(t *testing.T) {
	ctx := context.TODO()

	fakeClient := fake.NewFakeClient()
	nodeName := "test-node"
	initialNode := corev1.Node{
		ObjectMeta: v1.ObjectMeta{Name: nodeName},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               corev1.NodeReady,
					Reason:             "Ready",
					Status:             corev1.ConditionTrue,
					Message:            "Hello, world",
					LastHeartbeatTime:  metav1.Now(),
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
	if err := fakeClient.Create(ctx, &initialNode); err != nil {
		t.Fatalf("failed to create initial node: %v", err)
	}

	var recorder fakeEventRecorder

	nodeExporter := manager.NewNodeExporter(
		&initialNode,
		fakeClient,
		&recorder,
		map[corev1.NodeConditionType]manager.NodeConditionConfig{
			corev1.NodeReady: {
				ReadyReason:  "Ready",
				ReadyMessage: "Test Ready",
			},
		},
	)

	testConditionType := corev1.NodeConditionType("Test")
	testCondition := monitor.Condition{
		Reason:  "TestReason",
		Message: "TestMessage",
	}
	if err := nodeExporter.Info(ctx, testCondition, testConditionType); err != nil {
		t.Fatal(err)
	}
	if err := nodeExporter.Warning(ctx, testCondition, testConditionType); err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []string{corev1.EventTypeNormal, corev1.EventTypeWarning} {
		var found bool
		for _, event := range recorder.events.Items {
			if event.Reason == string(testConditionType) &&
				event.Message == fmt.Sprintf("%s: %s", testCondition.Reason, testCondition.Message) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("failed to verify event %s - %+v was present: %+v", eventType, testCondition, recorder.events)
		}
	}
}

func TestNodeExporter_ConditionReportedAfterTick(t *testing.T) {
	ctx := context.TODO()

	fakeClient := fake.NewFakeClient()
	nodeName := "test-node"
	initialNode := corev1.Node{
		ObjectMeta: v1.ObjectMeta{Name: nodeName},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               corev1.NodeReady,
					Reason:             "Ready",
					Status:             corev1.ConditionTrue,
					Message:            "Hello, world",
					LastHeartbeatTime:  metav1.Now(),
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
	if err := fakeClient.Create(ctx, &initialNode); err != nil {
		t.Fatalf("failed to create initial node: %v", err)
	}

	var recorder fakeEventRecorder

	nodeExporter := manager.NewNodeExporter(
		&initialNode,
		fakeClient,
		&recorder,
		map[corev1.NodeConditionType]manager.NodeConditionConfig{
			corev1.NodeReady: {
				ReadyReason:  "Ready",
				ReadyMessage: "Test Ready",
			},
		},
	)

	heartbeatChan := make(chan time.Time)
	reportChan := make(chan time.Time)

	go nodeExporter.RunWithTickers(ctx, heartbeatChan, reportChan)

	monitorCondition := monitor.Condition{
		Reason:  "TestReason",
		Message: "TestMessage",
	}
	conditionType := corev1.NodeConditionType("TestType")
	expectedCondition := corev1.NodeCondition{
		Type:    conditionType,
		Reason:  monitorCondition.Reason,
		Message: monitorCondition.Message,
		Status:  corev1.ConditionFalse,
	}

	if err := nodeExporter.Fatal(ctx, monitorCondition, conditionType); err != nil {
		t.Fatal(err)
	}

	nodeKey := client.ObjectKeyFromObject(&initialNode)

	var node corev1.Node
	if err := fakeClient.Get(ctx, nodeKey, &node); err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	if nodeHasCondition(node, expectedCondition) {
		t.Fatalf("node condition updated before report tick")
	}

	reportChan <- time.Now()

	if err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, true, func(ctx context.Context) (done bool, err error) {
		if err := fakeClient.Get(ctx, nodeKey, &node); err != nil {
			return false, fmt.Errorf("failed to get node: %v", err)
		}
		if !nodeHasCondition(node, expectedCondition) {
			t.Logf("node condition not found: %+v: %+v", expectedCondition, node.Status.Conditions)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("failed to verify node condition: %v", err)
	}
}

func nodeHasCondition(node corev1.Node, condition corev1.NodeCondition) bool {
	for _, c := range node.Status.Conditions {
		if isConditionEqual(c, condition) {
			return true
		}
	}
	return false
}

func isConditionEqual(l corev1.NodeCondition, r corev1.NodeCondition) bool {
	// ignore timestamps
	return l.Type == r.Type &&
		l.Status == r.Status &&
		l.Message == r.Message &&
		l.Reason == r.Reason
}

func TestNodeExporter_LastTransitionTimeFlapping(t *testing.T) {
	ctx := context.TODO()
	fakeClient := fake.NewFakeClient()
	nodeName := "test-node"
	initialNode := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:               "AcceleratedHardwareReady",
					Status:             corev1.ConditionTrue,
					Reason:             "Healthy",
					Message:            "All good",
					LastHeartbeatTime:  metav1.Now(),
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
	if err := fakeClient.Create(ctx, &initialNode); err != nil {
		t.Fatalf("failed to create initial node: %v", err)
	}

	nodeExporter := manager.NewNodeExporter(
		&initialNode,
		fakeClient,
		record.NewFakeRecorder(100),
		map[corev1.NodeConditionType]manager.NodeConditionConfig{
			"AcceleratedHardwareReady": {
				ReadyReason:  "Healthy",
				ReadyMessage: "All good",
			},
		},
	)

	heartbeatChan := make(chan time.Time)
	reportChan := make(chan time.Time)
	go nodeExporter.RunWithTickers(ctx, heartbeatChan, reportChan)

	conditionType := corev1.NodeConditionType("AcceleratedHardwareReady")

	// 1. Report first fatal error
	err1 := monitor.Condition{
		Reason:   "ErrorA",
		Message:  "MessageA",
		Severity: monitor.SeverityFatal,
	}
	if err := nodeExporter.Fatal(ctx, err1, conditionType); err != nil {
		t.Fatal(err)
	}

	// Capture the transition time
	reportChan <- time.Now()

	// Wait a bit for the report to process
	time.Sleep(time.Millisecond * 100)

	var node corev1.Node
	if err := fakeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		t.Fatal(err)
	}

	var ltt1 time.Time
	for _, c := range node.Status.Conditions {
		if c.Type == conditionType {
			ltt1 = c.LastTransitionTime.Time
			break
		}
	}
	if ltt1.IsZero() {
		t.Fatal("LastTransitionTime not set")
	}
	t.Logf("LTT1: %v", ltt1)

	// Wait a bit to ensure 'now' changes (metav1.Now() has 1-second resolution)
	time.Sleep(time.Millisecond * 1100)

	// 2. Report second fatal error with different message
	err2 := monitor.Condition{
		Reason:   "ErrorB",
		Message:  "MessageB",
		Severity: monitor.SeverityFatal,
	}
	if err := nodeExporter.Fatal(ctx, err2, conditionType); err != nil {
		t.Fatal(err)
	}

	reportChan <- time.Now()
	time.Sleep(time.Millisecond * 200)

	if err := fakeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		t.Fatal(err)
	}

	var ltt2 time.Time
	for _, c := range node.Status.Conditions {
		if c.Type == conditionType {
			ltt2 = c.LastTransitionTime.Time
			break
		}
	}
	t.Logf("LTT2: %v", ltt2)

	if ltt2.After(ltt1) {
		t.Errorf("LastTransitionTime flapped! It should have been preserved because status didn't change from False. ltt1: %v, ltt2: %v", ltt1, ltt2)
	}
	if !ltt2.Equal(ltt1) {
		t.Errorf("LastTransitionTime changed! It should have been identical. ltt1: %v, ltt2: %v", ltt1, ltt2)
	}

	// Verify the message was still updated to the latest one
	var latestMessage string
	for _, c := range node.Status.Conditions {
		if c.Type == conditionType {
			latestMessage = c.Message
			break
		}
	}
	if latestMessage != "MessageA; MessageB" {
		t.Errorf("Message was not updated to latest or aggregated. expected: MessageA; MessageB, got: %s", latestMessage)
	}

	// 3. Report same error again - should not duplicate in message
	if err := nodeExporter.Fatal(ctx, err1, conditionType); err != nil {
		t.Fatal(err)
	}
	reportChan <- time.Now()
	time.Sleep(time.Millisecond * 200)

	if err := fakeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		t.Fatal(err)
	}

	for _, c := range node.Status.Conditions {
		if c.Type == conditionType {
			latestMessage = c.Message
			break
		}
	}
	// It should still be "MessageA; MessageB" because MessageA is already contained in it.
	if latestMessage != "MessageA; MessageB" {
		t.Errorf("Message was incorrectly updated with duplicates or cleared. expected: MessageA; MessageB, got: %s", latestMessage)
	}
}

// TestNodeExporter_SetHealthyResetsAfterFatal verifies that SetHealthy flips a
// condition that's currently False back to its configured ready state with
// Status=ConditionTrue. This is the missing transition path that left
// AcceleratedHardwareReady stuck False after transient DCGM probe failures.
func TestNodeExporter_SetHealthyResetsAfterFatal(t *testing.T) {
	ctx := context.TODO()
	fakeClient := fake.NewFakeClient()
	nodeName := "test-node"
	initialNode := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{},
		},
	}
	if err := fakeClient.Create(ctx, &initialNode); err != nil {
		t.Fatalf("failed to create initial node: %v", err)
	}

	conditionType := corev1.NodeConditionType("AcceleratedHardwareReady")
	nodeExporter := manager.NewNodeExporter(
		&initialNode,
		fakeClient,
		record.NewFakeRecorder(100),
		map[corev1.NodeConditionType]manager.NodeConditionConfig{
			conditionType: {
				ReadyReason:  "NvidiaAcceleratedHardwareIsReady",
				ReadyMessage: "GPU is healthy",
			},
		},
	)

	heartbeatChan := make(chan time.Time)
	reportChan := make(chan time.Time)
	go nodeExporter.RunWithTickers(ctx, heartbeatChan, reportChan)

	// 1. Drive a Fatal so the local state goes False.
	fatalCond := monitor.Condition{
		Reason:   "DCGMError",
		Message:  "failed to initialize DCGM",
		Severity: monitor.SeverityFatal,
	}
	if err := nodeExporter.Fatal(ctx, fatalCond, conditionType); err != nil {
		t.Fatal(err)
	}
	reportChan <- time.Now()

	expectedFalse := corev1.NodeCondition{
		Type:    conditionType,
		Status:  corev1.ConditionFalse,
		Reason:  "DCGMError",
		Message: "failed to initialize DCGM",
	}
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		var n corev1.Node
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &n); err != nil {
			return false, err
		}
		return nodeHasCondition(n, expectedFalse), nil
	}); err != nil {
		t.Fatalf("Fatal condition was not reported to node: %v", err)
	}

	// 2. Now SetHealthy and confirm the condition flips back to True with the
	// configured ReadyReason/ReadyMessage.
	if err := nodeExporter.SetHealthy(ctx, conditionType); err != nil {
		t.Fatalf("SetHealthy failed: %v", err)
	}
	reportChan <- time.Now()

	expectedTrue := corev1.NodeCondition{
		Type:    conditionType,
		Status:  corev1.ConditionTrue,
		Reason:  "NvidiaAcceleratedHardwareIsReady",
		Message: "GPU is healthy",
	}
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		var n corev1.Node
		if err := fakeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &n); err != nil {
			return false, err
		}
		return nodeHasCondition(n, expectedTrue), nil
	}); err != nil {
		t.Fatalf("SetHealthy did not flip condition back to True: %v", err)
	}
}

// TestNodeExporter_SetHealthyPreservesTransitionTimeIfAlreadyTrue verifies that
// repeated SetHealthy calls on an already-True condition don't churn the
// LastTransitionTime — important so a happy-path monitor that's polled every
// 5s with no errors doesn't update the transition time on every cycle.
func TestNodeExporter_SetHealthyPreservesTransitionTimeIfAlreadyTrue(t *testing.T) {
	ctx := context.TODO()
	fakeClient := fake.NewFakeClient()
	nodeName := "test-node"
	initialNode := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
	}
	if err := fakeClient.Create(ctx, &initialNode); err != nil {
		t.Fatalf("failed to create initial node: %v", err)
	}

	conditionType := corev1.NodeConditionType("AcceleratedHardwareReady")
	nodeExporter := manager.NewNodeExporter(
		&initialNode,
		fakeClient,
		record.NewFakeRecorder(100),
		map[corev1.NodeConditionType]manager.NodeConditionConfig{
			conditionType: {
				ReadyReason:  "Healthy",
				ReadyMessage: "All good",
			},
		},
	)

	heartbeatChan := make(chan time.Time)
	reportChan := make(chan time.Time)
	go nodeExporter.RunWithTickers(ctx, heartbeatChan, reportChan)

	// Initial state from initializeManagedConditions is already True; flush it
	// to the API.
	if err := nodeExporter.SetHealthy(ctx, conditionType); err != nil {
		t.Fatal(err)
	}
	reportChan <- time.Now()
	time.Sleep(time.Millisecond * 100)

	var n corev1.Node
	if err := fakeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &n); err != nil {
		t.Fatal(err)
	}
	var ltt1 time.Time
	for _, c := range n.Status.Conditions {
		if c.Type == conditionType {
			ltt1 = c.LastTransitionTime.Time
		}
	}
	if ltt1.IsZero() {
		t.Fatal("transition time not set")
	}

	// Sleep past metav1.Now()'s 1s resolution, then SetHealthy again. The
	// transition time must NOT update because the status hasn't changed.
	time.Sleep(time.Millisecond * 1100)
	if err := nodeExporter.SetHealthy(ctx, conditionType); err != nil {
		t.Fatal(err)
	}
	reportChan <- time.Now()
	time.Sleep(time.Millisecond * 100)

	if err := fakeClient.Get(ctx, client.ObjectKey{Name: nodeName}, &n); err != nil {
		t.Fatal(err)
	}
	for _, c := range n.Status.Conditions {
		if c.Type == conditionType {
			if !c.LastTransitionTime.Time.Equal(ltt1) {
				t.Errorf("LastTransitionTime updated on a no-op SetHealthy: was %v, now %v", ltt1, c.LastTransitionTime.Time)
			}
		}
	}
}

// TestNodeExporter_SetHealthyUnknownConditionType ensures SetHealthy errors
// (rather than panicking or silently writing) when called for a condition
// type that wasn't registered with a NodeConditionConfig.
func TestNodeExporter_SetHealthyUnknownConditionType(t *testing.T) {
	ctx := context.TODO()
	fakeClient := fake.NewFakeClient()
	initialNode := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test-node"}}
	if err := fakeClient.Create(ctx, &initialNode); err != nil {
		t.Fatalf("failed to create initial node: %v", err)
	}

	nodeExporter := manager.NewNodeExporter(
		&initialNode,
		fakeClient,
		record.NewFakeRecorder(100),
		map[corev1.NodeConditionType]manager.NodeConditionConfig{
			"AcceleratedHardwareReady": {ReadyReason: "Healthy", ReadyMessage: "ok"},
		},
	)

	if err := nodeExporter.SetHealthy(ctx, "NotRegistered"); err == nil {
		t.Fatal("expected error for unregistered condition type, got nil")
	}
}
