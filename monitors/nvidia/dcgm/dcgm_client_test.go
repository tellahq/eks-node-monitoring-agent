//go:build !darwin

package dcgm

import (
	"strings"
	"testing"
	"time"
)

// TestShouldSwallowInitError exercises the policy decision in isolation —
// the dcgmapi.Init wrapper itself can't be unit-tested (it dlopens
// libdcgmapi.so.4 and talks to a real nv-hostengine), but the predicate
// driving error suppression is pure logic and worth covering.
//
// Two windows apply:
//   - Boot grace: applies before any successful init, measured from
//     constructedAt.
//   - Runtime grace: applies after a previous successful init (and
//     subsequent shutdown), measured from lastShutdown.
//
// Cases below cover both windows in their open and closed states, the
// transition between them at first-successful-init, and the
// disabled-when-zero behavior.
func TestShouldSwallowInitError(t *testing.T) {
	construction := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name              string
		bootGrace         time.Duration
		runtimeGrace      time.Duration
		everInitialized   bool
		lastShutdownAfter time.Duration // offset from construction; ignored if !everInitialized
		nowAfter          time.Duration // offset from construction
		want              bool
		// wantDescriptorContains, when set, is asserted as a substring of the
		// returned window descriptor when want=true. Empty when want=false.
		wantDescriptorContains string
	}{
		{
			name:                   "boot grace open, never initialized — swallow",
			bootGrace:              5 * time.Minute,
			nowAfter:               90 * time.Second, // matches observed prod boot race
			want:                   true,
			wantDescriptorContains: "boot-grace",
		},
		{
			name:      "boot grace closed, never initialized — surface",
			bootGrace: 1 * time.Minute,
			nowAfter:  90 * time.Second,
			want:      false,
		},
		{
			name:      "boot grace zero (disabled) — surface immediately",
			bootGrace: 0,
			nowAfter:  1 * time.Second,
			want:      false,
		},
		{
			name:      "boot grace exactly at boundary — surface (Before is strict)",
			bootGrace: 5 * time.Minute,
			nowAfter:  5 * time.Minute,
			want:      false,
		},
		{
			name:                   "runtime grace open after first success — swallow",
			runtimeGrace:           1 * time.Minute,
			everInitialized:        true,
			lastShutdownAfter:      10 * time.Minute,
			nowAfter:               10*time.Minute + 30*time.Second,
			want:                   true,
			wantDescriptorContains: "runtime-grace",
		},
		{
			name:              "runtime grace closed — surface",
			runtimeGrace:      1 * time.Minute,
			everInitialized:   true,
			lastShutdownAfter: 10 * time.Minute,
			nowAfter:          12 * time.Minute,
			want:              false,
		},
		{
			name:              "runtime grace zero (disabled) — surface immediately",
			runtimeGrace:      0,
			everInitialized:   true,
			lastShutdownAfter: 10 * time.Minute,
			nowAfter:          10*time.Minute + 1*time.Second,
			want:              false,
		},
		{
			name:              "boot grace ignored once everInitialized — uses runtime grace",
			bootGrace:         1 * time.Hour,
			runtimeGrace:      30 * time.Second,
			everInitialized:   true,
			lastShutdownAfter: 5 * time.Minute,
			nowAfter:          6 * time.Minute, // 60s after lastShutdown
			want:              false,           // runtime grace = 30s, exceeded
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dcgmHelper{
				config: DCGMConfig{
					BootGracePeriod:           tt.bootGrace,
					InitializationGracePeriod: tt.runtimeGrace,
				},
				constructedAt:   construction,
				everInitialized: tt.everInitialized,
				lastShutdown:    construction.Add(tt.lastShutdownAfter),
			}
			got, descriptor := d.shouldSwallowInitError(construction.Add(tt.nowAfter))
			if got != tt.want {
				t.Errorf("shouldSwallowInitError swallow = %v, want %v (descriptor=%q)", got, tt.want, descriptor)
			}
			if tt.want && tt.wantDescriptorContains != "" {
				if !strings.Contains(descriptor, tt.wantDescriptorContains) {
					t.Errorf("descriptor = %q, want substring %q", descriptor, tt.wantDescriptorContains)
				}
			}
			if !tt.want && descriptor != "" {
				t.Errorf("descriptor = %q on no-swallow path; want empty", descriptor)
			}
		})
	}
}
