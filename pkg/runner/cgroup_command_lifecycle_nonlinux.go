//go:build !linux

package runner

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewCgroupCommandLifecycleFactory gives an error on non-Linux platforms,
// as per-action cgroup containment is supported only on Linux.
func NewCgroupCommandLifecycleFactory(parent string, cleanupTimeout time.Duration, clock clock.Clock) (CommandLifecycleFactory, error) {
	return nil, status.Error(codes.Unimplemented, "Per-action cgroup containment is supported only on Linux")
}
