//go:build !linux

package runner

import (
	"syscall"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewPIDNamespaceCommandLifecycleFactory gives an error on non-Linux platforms,
// as PID namespace containment is supported only on Linux.
func NewPIDNamespaceCommandLifecycleFactory(helper string, timeout time.Duration, helperUID, helperGID uint32, actionAttributes *syscall.SysProcAttr, clock clock.Clock) (CommandLifecycleFactory, error) {
	return nil, status.Error(codes.Unimplemented, "PID namespace containment is supported only on Linux")
}
