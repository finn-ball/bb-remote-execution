package runner

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type manualTimerClock struct {
	clock.Clock
	t        *testing.T
	duration time.Duration
	expired  chan time.Time
	created  bool
	stopped  bool
}

func (c *manualTimerClock) NewTimer(duration time.Duration) (clock.Timer, <-chan time.Time) {
	require.Equal(c.t, c.duration, duration)
	require.False(c.t, c.created)
	c.created = true
	c.t.Cleanup(func() { require.True(c.t, c.stopped) })
	return c, c.expired
}

func (c *manualTimerClock) Stop() bool {
	require.False(c.t, c.stopped)
	c.stopped = true
	return false
}

func cgroupWaitFixture(t *testing.T) (*cgroupCommandLifecycle, chan time.Time) {
	t.Helper()
	expired := make(chan time.Time, 1)
	manualClock := &manualTimerClock{
		t:        t,
		duration: time.Second,
		expired:  expired,
	}
	directory, err := os.Open(t.TempDir())
	require.NoError(t, err)
	return &cgroupCommandLifecycle{
		clock:          manualClock,
		cleanupTimeout: time.Second,
		directory:      directory,
		wait:           make(chan cgroupWaitResult, 1),
	}, expired
}

func TestCgroupCancellationWaitTimeout(t *testing.T) {
	l, expired := cgroupWaitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	expired <- time.Now()
	response, err := l.Wait(ctx, nil)
	require.Nil(t, response)
	var unknown *CompletionUnknownError
	require.ErrorAs(t, err, &unknown)
	testutil.RequireEqualStatus(t, status.Error(codes.DeadlineExceeded, "Cgroup teardown exceeded 1s"), unknown.Cause)
	// A later reaping notification cannot undo uncertain completion. Finish
	// must retain the cgroup and must not create a fresh cleanup deadline.
	l.wait <- cgroupWaitResult{reaped: true}
	require.Same(t, err, l.Finish(context.Background()))
	_, statErr := l.directory.Stat()
	require.ErrorIs(t, statErr, os.ErrClosed)
}

func TestCgroupCancellationReapedUsesOriginalDeadline(t *testing.T) {
	l, expired := cgroupWaitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	expected := &runner_pb.RunResponse{ExitCode: 127}
	l.wait <- cgroupWaitResult{response: expected, reaped: true}
	response, err := l.Wait(ctx, nil)
	require.NoError(t, err)
	require.Same(t, expected, response)
	expired <- time.Now()
	var unknown *CompletionUnknownError
	require.ErrorAs(t, l.Finish(context.Background()), &unknown)
	testutil.RequireEqualStatus(t, status.Error(codes.DeadlineExceeded, "Cgroup teardown exceeded 1s"), unknown.Cause)
}

func TestCgroupWaitRequiresReaping(t *testing.T) {
	l, expired := cgroupWaitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l.wait <- cgroupWaitResult{err: errors.New("wait failed")}
	_, err := l.Wait(ctx, nil)
	var unknown *CompletionUnknownError
	require.ErrorAs(t, err, &unknown)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Cgroup command wait did not confirm reaping: wait failed"), unknown.Cause)
	expired <- time.Now()
	require.Same(t, err, l.Finish(context.Background()))
}

func TestCgroupStartedWait(t *testing.T) {
	if exit := os.Getenv("BB_CGROUP_WAIT_EXIT"); exit != "" {
		if exit == "127" {
			os.Exit(127)
		}
		os.Exit(0)
	}
	for _, exit := range []string{"0", "127"} {
		t.Run(exit, func(t *testing.T) {
			l := &cgroupCommandLifecycle{}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCgroupStartedWait$")
			cmd.Env = append(os.Environ(), "BB_CGROUP_WAIT_EXIT="+exit)
			err := cmd.Start()
			l.Started(cmd, err)
			require.NoError(t, err)
			response, err := l.Wait(context.Background(), cmd)
			require.NoError(t, err)
			if exit == "127" {
				require.EqualValues(t, 127, response.ExitCode)
			} else {
				require.Zero(t, response.ExitCode)
			}
			require.True(t, l.reaped)
			require.NotNil(t, cmd.ProcessState)
		})
	}
	l := &cgroupCommandLifecycle{}
	l.Started(nil, errors.New("start failed"))
	require.Nil(t, l.wait)
}
