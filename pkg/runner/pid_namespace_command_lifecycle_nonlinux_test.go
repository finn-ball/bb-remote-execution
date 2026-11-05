//go:build darwin || freebsd || windows

package runner_test

import (
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/testutil"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPIDNamespaceUnsupportedPlatform(t *testing.T) {
	_, err := runner.NewPIDNamespaceCommandLifecycleFactory("/helper", time.Second, clock.SystemClock)
	testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "PID namespace containment is supported only on Linux"), err)
}
