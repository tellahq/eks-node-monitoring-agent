package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// reportInterval is the interval at which the local state is applied to the node.
	// This ensures that changes to multiple managed conditions within this time period are reported in a single API call.
	reportInterval = 15 * time.Second

	// heartbeatInterval is the interval at which managed condition heartbeat times are updated.
	heartbeatInterval = 5 * time.Minute
)

var _ Exporter = (*nodeExporter)(nil)

// NodeConditionConfig holds the ready state configuration for a node condition
type NodeConditionConfig struct {
	ReadyReason  string
	ReadyMessage string
}

// NewNodeExporter creates a new node exporter that updates Kubernetes node conditions
func NewNodeExporter(
	node *corev1.Node,
	kubeClient client.Client,
	recorder record.EventRecorder,
	managedConditionConfigs map[corev1.NodeConditionType]NodeConditionConfig,
) *nodeExporter {
	return &nodeExporter{
		nodeRef:                 makeNodeReference(node),
		nodeKey:                 client.ObjectKeyFromObject(node),
		kubeClient:              kubeClient,
		recorder:                recorder,
		managedConditionConfigs: managedConditionConfigs,
		managedConditions:       initializeManagedConditions(managedConditionConfigs),
		managedConditionsDirty:  true,
	}
}

// makeNodeReference returns an ObjectReference for the specified node that can be (re)used for event recordings.
func makeNodeReference(node *corev1.Node) *corev1.ObjectReference {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(fmt.Errorf("failed to add core v1 types to scheme: %v", err))
	}
	ref, err := reference.GetReference(scheme, node)
	if err != nil {
		panic(fmt.Errorf("failed to construct node reference: %v", err))
	}
	// remove the resource version, it's not useful to us
	ref.ResourceVersion = ""
	return ref
}

func initializeManagedConditions(conditionConfigs map[corev1.NodeConditionType]NodeConditionConfig) map[corev1.NodeConditionType]corev1.NodeCondition {
	managedConditions := make(map[corev1.NodeConditionType]corev1.NodeCondition)
	now := metav1.Now()
	for conditionType, conditionConfig := range conditionConfigs {
		managedConditions[conditionType] = corev1.NodeCondition{
			Type:               conditionType,
			Status:             corev1.ConditionTrue,
			Reason:             conditionConfig.ReadyReason,
			Message:            conditionConfig.ReadyMessage,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
		}
	}
	return managedConditions
}

// nodeExporter implements monitor.Exporter by exposing conditions onto the k8s node resource
type nodeExporter struct {
	kubeClient client.Client
	recorder   record.EventRecorder
	nodeRef    *corev1.ObjectReference
	nodeKey    client.ObjectKey

	// managedConditionConfigs holds the per-condition ready-state config
	// (ReadyReason/ReadyMessage) retained for SetHealthy.
	managedConditionConfigs map[corev1.NodeConditionType]NodeConditionConfig

	managedConditions      map[corev1.NodeConditionType]corev1.NodeCondition
	managedConditionsDirty bool
	managedConditionsLock  sync.Mutex
}

// Info records an event for the specified condition.
func (e *nodeExporter) Info(ctx context.Context, c monitor.Condition, conditionType corev1.NodeConditionType) error {
	e.recorder.Event(e.nodeRef, corev1.EventTypeNormal, string(conditionType), fmt.Sprintf("%s: %s", c.Reason, c.Message))
	return nil
}

// Warning records an event for the specified condition.
func (e *nodeExporter) Warning(ctx context.Context, c monitor.Condition, conditionType corev1.NodeConditionType) error {
	e.recorder.Event(e.nodeRef, corev1.EventTypeWarning, string(conditionType), fmt.Sprintf("%s: %s", c.Reason, c.Message))
	return nil
}

// Fatal updates the local state for the specified managed condition.
// The condition will be reported in the Node.Status.Conditions periodically.
func (e *nodeExporter) Fatal(ctx context.Context, monitorCondition monitor.Condition, conditionType corev1.NodeConditionType) error {
	e.managedConditionsLock.Lock()
	defer e.managedConditionsLock.Unlock()
	now := metav1.Now()
	newCondition := corev1.NodeCondition{
		Type:               conditionType,
		Reason:             monitorCondition.Reason,
		Message:            monitorCondition.Message,
		Status:             corev1.ConditionFalse,
		LastTransitionTime: now,
		LastHeartbeatTime:  now,
	}
	if oldCondition, ok := e.managedConditions[conditionType]; ok {
		// if the status has not changed, use the old transition time
		if oldCondition.Status == newCondition.Status {
			newCondition.LastTransitionTime = oldCondition.LastTransitionTime

			// aggregate messages if the status is the same (e.g. both False)
			// and ensure that we don't duplicate identical messages.
			if oldCondition.Message != "" && oldCondition.Message != newCondition.Message && !strings.Contains(oldCondition.Message, newCondition.Message) {
				newCondition.Message = oldCondition.Message + "; " + newCondition.Message
			} else if strings.Contains(oldCondition.Message, newCondition.Message) {
				// if the old message already contains the new one, preserve the old one (which might have other aggregated messages)
				newCondition.Message = oldCondition.Message
			}
		}
	}
	e.managedConditions[conditionType] = newCondition
	e.managedConditionsDirty = true
	return nil
}

// Healthy resets the managed condition for the given conditionType back to
// its configured ready state (Status: ConditionTrue, Reason/Message from the
// NodeConditionConfig passed to NewNodeExporter). Used by the manager's
// auto-recovery path to flip a previously-Fatal condition back to True once
// the underlying monitor has stopped reporting errors for the recovery
// threshold. If the local cache already reflects the configured ready state
// this is a pure no-op (no dirty bit, no apiserver patch on the next report
// tick) — a small but useful optimization since maybeAutoRecover may end up
// calling Healthy on every poll cycle for monitors that never go Fatal.
func (e *nodeExporter) Healthy(ctx context.Context, conditionType corev1.NodeConditionType) error {
	config, ok := e.managedConditionConfigs[conditionType]
	if !ok {
		return fmt.Errorf("no NodeConditionConfig registered for condition type %q", conditionType)
	}
	e.managedConditionsLock.Lock()
	defer e.managedConditionsLock.Unlock()

	// No-op fast path: local cache already matches the configured ready state.
	// Skip the write and don't dirty the condition map — there's nothing to
	// report to the apiserver.
	if old, ok := e.managedConditions[conditionType]; ok &&
		old.Status == corev1.ConditionTrue &&
		old.Reason == config.ReadyReason &&
		old.Message == config.ReadyMessage {
		return nil
	}

	now := metav1.Now()
	newCondition := corev1.NodeCondition{
		Type:               conditionType,
		Reason:             config.ReadyReason,
		Message:            config.ReadyMessage,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: now,
		LastHeartbeatTime:  now,
	}
	if old, ok := e.managedConditions[conditionType]; ok && old.Status == corev1.ConditionTrue {
		// Already True but reason/message drifted — preserve the existing
		// transition time so the recovery isn't recorded as a fresh
		// transition. The fast path above already handled the strict no-op
		// case; this branch only fires if reason or message changed.
		newCondition.LastTransitionTime = old.LastTransitionTime
	}
	e.managedConditions[conditionType] = newCondition
	e.managedConditionsDirty = true
	return nil
}

// Run starts the node exporter's background tasks
func (e *nodeExporter) Run(ctx context.Context) {
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()
	e.RunWithTickers(ctx, heartbeatTicker.C, reportTicker.C)
}

// RunWithTickers is a long-running loop that wakes up for heartbeat or report ticks, and terminates when the context is done.
// The ticker channels are exposed directly for testing.
func (e *nodeExporter) RunWithTickers(ctx context.Context, heartbeatTicker <-chan time.Time, reportTicker <-chan time.Time) {
	log.FromContext(ctx).Info("starting node exporter")
	for {
		select {
		case <-heartbeatTicker:
			e.updateHeartbeatTimes()
		case <-reportTicker:
			if err := e.reportManagedConditions(ctx); err != nil {
				log.FromContext(ctx).Error(err, "failed to report managed conditions")
			}
		case <-ctx.Done():
			return
		}
	}
}

// updateHeartbeatTimes sets the managed condition heartbeat times to the current time, and marks the local state as dirty.
// This causes all managed conditions to be reported the next time reportManagedConditions is called.
func (e *nodeExporter) updateHeartbeatTimes() {
	e.managedConditionsLock.Lock()
	defer e.managedConditionsLock.Unlock()
	now := metav1.Now()
	for condType := range e.managedConditions {
		cond := e.managedConditions[condType]
		cond.LastHeartbeatTime = now
		e.managedConditions[condType] = cond
	}
	e.managedConditionsDirty = true
}

// reportManagedConditions applies the managed conditions to the node with an "upsert" strategy.
// If the local state is not dirty, this is a no-op.
// If a managed condition does not exist, or has been removed by another API client, it will be appended to the node's conditions.
// If a managed condition already exists, it will be replaced by our local copy.
func (e *nodeExporter) reportManagedConditions(ctx context.Context) error {
	e.managedConditionsLock.Lock()
	defer e.managedConditionsLock.Unlock()
	if !e.managedConditionsDirty {
		return nil
	}
	log.FromContext(ctx).Info("reporting managed conditions")
	var oldNode corev1.Node
	if err := e.kubeClient.Get(ctx, e.nodeKey, &oldNode); err != nil {
		return err
	}
	newNode := oldNode.DeepCopy()
	conditions := newNode.Status.Conditions
	for _, managedCondition := range e.managedConditions {
		found := false
		for i, condition := range conditions {
			if managedCondition.Type == condition.Type {
				newNode.Status.Conditions[i] = managedCondition
				found = true
				break
			}
		}
		if !found {
			newNode.Status.Conditions = append(newNode.Status.Conditions, managedCondition)
		}
	}
	if err := e.kubeClient.Status().Patch(ctx, newNode, client.MergeFrom(&oldNode)); err != nil {
		return err
	}
	e.managedConditionsDirty = false
	log.FromContext(ctx).Info("reported node conditions")
	return nil
}
