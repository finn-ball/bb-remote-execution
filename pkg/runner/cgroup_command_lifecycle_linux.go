package runner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cgroupWaitResult struct {
	response *runner_pb.RunResponse
	err      error
	reaped   bool
}

type cgroupCommandLifecycle struct {
	clock          clock.Clock
	path           string
	directory      *os.File
	cleanupTimeout time.Duration
	wait           chan cgroupWaitResult
	reaped         bool
	timer          clock.Timer
	timeout        <-chan time.Time
	completionErr  error
}

// NewCgroupCommandLifecycleFactory validates a delegated cgroup v2 parent. The
// runner must own its controls; actions must not have permission to migrate out.
// No fallback to process groups or uncontained execution is permitted.
func NewCgroupCommandLifecycleFactory(parent string, cleanupTimeout time.Duration, clock clock.Clock) (CommandLifecycleFactory, error) {
	if !filepath.IsAbs(parent) || cleanupTimeout <= 0 {
		return nil, status.Error(codes.InvalidArgument, "Cgroup parent must be absolute and cleanup timeout must be positive")
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(parent, &stat); err != nil {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to stat cgroup parent %q", parent)
	}
	if stat.Type != unix.CGROUP2_SUPER_MAGIC {
		return nil, status.Errorf(codes.InvalidArgument, "%q is not a cgroup v2 filesystem", parent)
	}
	factory := func() (CommandLifecycle, error) {
		name, err := os.MkdirTemp(parent, "attempt-")
		if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create action cgroup under %q", parent)
		}
		directory, err := os.Open(name)
		if err != nil {
			os.Remove(name)
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to open action cgroup %q", name)
		}
		return &cgroupCommandLifecycle{
			clock:          clock,
			path:           name,
			directory:      directory,
			cleanupTimeout: cleanupTimeout,
		}, nil
	}
	// Probe the actual facilities and permissions at startup. Do not merely check
	// for file existence: a read-only or incompletely delegated mount is unusable.
	probe, err := factory()
	if err != nil {
		return nil, err
	}
	if err := probe.Finish(context.Background()); err != nil {
		return nil, err
	}
	return factory, nil
}

func (l *cgroupCommandLifecycle) Configure(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Credential == nil || int(cmd.SysProcAttr.Credential.Uid) == os.Geteuid() {
		return status.Error(codes.InvalidArgument, "Cgroup containment requires an action UID different from the runner UID")
	}

	attributes := syscall.SysProcAttr{}
	if cmd.SysProcAttr != nil {
		attributes = *cmd.SysProcAttr
	}
	attributes.UseCgroupFD = true
	attributes.CgroupFD = int(l.directory.Fd())
	cmd.SysProcAttr = &attributes
	cmd.Cancel = func() error {
		if err := os.WriteFile(filepath.Join(l.path, "cgroup.kill"), []byte("1"), 0); err != nil {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to kill cgroup %q", l.path)
		}
		return nil
	}
	return nil
}

func (l *cgroupCommandLifecycle) Started(cmd *exec.Cmd, startErr error) {
	if startErr == nil {
		wait := make(chan cgroupWaitResult, 1)
		l.wait = wait
		go func() {
			response, err := waitForCommand(cmd)
			wait <- cgroupWaitResult{
				response: response,
				err:      err,
				reaped:   cmd.ProcessState != nil,
			}
		}()
	}
}

func (l *cgroupCommandLifecycle) startCleanupTimer() {
	if l.timer == nil {
		l.timer, l.timeout = l.clock.NewTimer(l.cleanupTimeout)
	}
}

func (l *cgroupCommandLifecycle) unknownCompletion(cause error) error {
	l.completionErr = &CompletionUnknownError{Cause: cause}
	return l.completionErr
}

func (l *cgroupCommandLifecycle) confirmReaped(result cgroupWaitResult) error {
	if !result.reaped {
		return l.unknownCompletion(status.Errorf(codes.Internal, "Cgroup command wait did not confirm reaping: %v", result.err))
	}
	l.reaped = true
	return nil
}

func (l *cgroupCommandLifecycle) cleanupExpired() error {
	return l.unknownCompletion(status.Errorf(codes.DeadlineExceeded, "Cgroup teardown exceeded %s", l.cleanupTimeout))
}

func (l *cgroupCommandLifecycle) Wait(ctx context.Context, cmd *exec.Cmd) (*runner_pb.RunResponse, error) {
	canceled := ctx.Done()
	if ctx.Err() != nil {
		canceled = nil
		l.startCleanupTimer()
	}
	for {
		select {
		case result := <-l.wait:
			if err := l.confirmReaped(result); err != nil {
				return nil, err
			}
			return result.response, result.err
		case <-canceled:
			canceled = nil
			l.startCleanupTimer()
		case <-l.timeout:
			return nil, l.cleanupExpired()
		}
	}
}

func (l *cgroupCommandLifecycle) Finish(ctx context.Context) error {
	defer l.directory.Close()
	defer func() {
		if l.timer != nil {
			l.timer.Stop()
		}
	}()
	if l.completionErr != nil {
		return l.completionErr
	}
	l.startCleanupTimer()
	select {
	case <-l.timeout:
		return l.cleanupExpired()
	default:
	}
	// Kill leftovers on success as well as cancellation. cgroup.kill operates on
	// the entire subtree atomically with respect to concurrent forks/migrations.
	if err := os.WriteFile(filepath.Join(l.path, "cgroup.kill"), []byte("1"), 0); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to kill cgroup %q", l.path)
	}
	if l.wait != nil && !l.reaped {
		select {
		case result := <-l.wait:
			if err := l.confirmReaped(result); err != nil {
				return err
			}
		case <-l.timeout:
			return l.cleanupExpired()
		}
	}
	ticker, ticks := l.clock.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-l.timeout:
			return l.cleanupExpired()
		default:
		}
		events, err := os.ReadFile(filepath.Join(l.path, "cgroup.events"))
		if err != nil {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to read cgroup %q", l.path)
		}
		if strings.Contains("\n"+string(events), "\npopulated 0\n") {
			if err := os.Remove(l.path); err != nil {
				return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove empty cgroup %q", l.path)
			}
			return nil
		}
		select {
		case <-l.timeout:
			return l.cleanupExpired()
		case <-ticks:
		}
	}
}
