package manager

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
)

// Exporter handles propagating conditions from monitors to external systems
type Exporter interface {
	// Info exports informational conditions
	Info(ctx context.Context, condition monitor.Condition, conditionType corev1.NodeConditionType) error

	// Warning exports warning conditions
	Warning(ctx context.Context, condition monitor.Condition, conditionType corev1.NodeConditionType) error

	// Fatal exports fatal conditions
	Fatal(ctx context.Context, condition monitor.Condition, conditionType corev1.NodeConditionType) error

	// SetHealthy resets a managed condition to its configured ready state
	// (Status: ConditionTrue, Reason/Message from NodeConditionConfig). Called
	// by the MonitorManager's auto-recovery path when a monitor that previously
	// reported Fatal goes quiet for the recovery threshold. Without this, a
	// Fatal condition stays False indefinitely even after the monitor recovers,
	// because monitors traditionally signal recovery via absence-of-condition.
	SetHealthy(ctx context.Context, conditionType corev1.NodeConditionType) error
}
