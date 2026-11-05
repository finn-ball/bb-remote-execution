//go:build !linux

package runner

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewPIDNamespaceCommandLifecycleFactory gives an error on non-Linux platforms,
// as PID namespace containment is supported only on Linux.
func NewPIDNamespaceCommandLifecycleFactory(helper string, timeout time.Duration, clock clock.Clock) (CommandLifecycleFactory, error) {
	return nil, status.Error(codes.Unimplemented, "PID namespace containment is supported only on Linux")
}
