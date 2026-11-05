package runner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner/pidnamespace"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type namespaceWaitResult struct {
	err    error
	reaped bool
}

type namespaceResult struct {
	value pidnamespace.Result
	err   error
}

type pidNamespaceCommandLifecycle struct {
	clock         clock.Clock
	helper        string
	timeout       time.Duration
	probe         bool
	childFiles    []*os.File
	lifetime      *os.File
	requestWriter *os.File
	resultReader  *os.File
	result        chan namespaceResult
	wait          chan namespaceWaitResult
	cmd           *exec.Cmd
	completionErr error
	helperReaped  bool
}

// NewPIDNamespaceCommandLifecycleFactory verifies actual namespace creation and
// proc mounting at startup. There is no fallback to uncontained execution.
func NewPIDNamespaceCommandLifecycleFactory(helper string, timeout time.Duration, clock clock.Clock) (CommandLifecycleFactory, error) {
	if !filepath.IsAbs(helper) || timeout <= 0 {
		return nil, status.Error(codes.InvalidArgument, "Namespace helper must be absolute and cleanup timeout must be positive")
	}
	if os.Geteuid() != 0 {
		return nil, status.Error(codes.FailedPrecondition, "PID namespace containment requires a privileged runner")
	}
	st, err := os.Stat(helper)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to stat namespace helper")
	}
	if !st.Mode().IsRegular() || st.Mode()&0o111 == 0 {
		return nil, status.Error(codes.InvalidArgument, "Namespace helper is not a regular executable")
	}
	factory := func() (CommandLifecycle, error) {
		return &pidNamespaceCommandLifecycle{
			clock:   clock,
			helper:  helper,
			timeout: timeout,
		}, nil
	}
	probe := &pidNamespaceCommandLifecycle{
		clock:   clock,
		helper:  helper,
		timeout: timeout,
		probe:   true,
	}
	ctx, cancel := clock.NewContextWithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, helper)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
	if err := probe.Configure(cmd); err != nil {
		probe.Finish(context.Background())
		return nil, err
	}
	err = cmd.Start()
	probe.Started(cmd, err)
	if err != nil {
		probe.Finish(context.Background())
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to start namespace capability probe")
	}
	_, runErr := probe.Wait(ctx, cmd)
	finishErr := probe.Finish(context.Background())
	if finishErr != nil {
		return nil, finishErr
	}
	if runErr != nil {
		return nil, util.StatusWrap(runErr, "Namespace capability probe failed")
	}
	return factory, nil
}

func (l *pidNamespaceCommandLifecycle) Configure(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Credential == nil || cmd.SysProcAttr.Credential.Uid == 0 {
		return status.Error(codes.InvalidArgument, "PID namespace containment requires explicit non-root action credentials")
	}
	attr := *cmd.SysProcAttr
	attr.Credential, attr.Chroot = nil, ""
	if !reflect.DeepEqual(attr, syscall.SysProcAttr{}) || len(cmd.ExtraFiles) != 0 {
		return status.Error(codes.InvalidArgument, "Unsupported action process attributes for PID namespace containment")
	}
	// Environ applies exec.Cmd's last-value-wins normalization. Validate first:
	// Environ intentionally discards errors, including NUL in shadowed entries.
	// On Linux the only other possible error is resolving Dir for implicit PWD.
	for _, entry := range cmd.Env {
		if strings.IndexByte(entry, 0) >= 0 {
			return status.Error(codes.Internal, "Failed to start process: exec: environment variable contains NUL")
		}
	}
	if cmd.Env == nil && cmd.Dir != "" {
		if _, err := filepath.Abs(cmd.Dir); err != nil {
			return status.Errorf(codes.Internal, "Failed to start process: %v", err)
		}
	}
	request := pidnamespace.Request{
		Path:       cmd.Path,
		Args:       cmd.Args,
		Env:        cmd.Environ(),
		Dir:        cmd.Dir,
		Chroot:     cmd.SysProcAttr.Chroot,
		Credential: cmd.SysProcAttr.Credential,
		Probe:      l.probe,
	}
	data, err := pidnamespace.Encode(request)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.ResourceExhausted, "Failed to encode namespace action request")
	}
	requestReader, requestWriter, err := os.Pipe()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create namespace request pipe")
	}
	l.childFiles = append(l.childFiles, requestReader)
	lifetimeReader, lifetimeWriter, err := os.Pipe()
	if err != nil {
		requestWriter.Close()
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create namespace lifetime pipe")
	}
	l.childFiles = append(l.childFiles, lifetimeReader)
	l.lifetime = lifetimeWriter
	resultReader, resultWriter, err := os.Pipe()
	if err != nil {
		requestWriter.Close()
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create namespace result pipe")
	}
	l.childFiles = append(l.childFiles, resultWriter)
	l.resultReader = resultReader
	l.result = make(chan namespaceResult, 1)
	go func() {
		var r pidnamespace.Result
		err := pidnamespace.Decode(resultReader, &r)
		resultReader.Close()
		l.result <- namespaceResult{value: r, err: err}
	}()
	l.requestWriter = requestWriter
	go func() {
		requestWriter.Write(data)
		requestWriter.Close()
	}()
	cmd.Path, cmd.Args, cmd.Dir, cmd.Env = l.helper, []string{l.helper}, "", []string{}
	cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWPID | syscall.CLONE_NEWNS}
	cmd.ExtraFiles = l.childFiles
	// Bound any exec-owned I/O copying as well as the namespace reaping wait.
	cmd.WaitDelay = l.timeout
	return nil
}

// Started closes child-only copies even if exec.Start fails. This is essential:
// the helper must observe EOF when the runner's sole lifetime writer closes.
func (l *pidNamespaceCommandLifecycle) Started(cmd *exec.Cmd, startErr error) {
	for _, f := range l.childFiles {
		f.Close()
	}
	l.childFiles = nil
	if startErr == nil {
		l.cmd = cmd
		l.wait = make(chan namespaceWaitResult, 1)
		go func() {
			err := cmd.Wait()
			l.wait <- namespaceWaitResult{err: err, reaped: cmd.ProcessState != nil}
		}()
	}
}

func (l *pidNamespaceCommandLifecycle) Wait(ctx context.Context, cmd *exec.Cmd) (*runner_pb.RunResponse, error) {
	var r namespaceResult
	resultCh, waitCh, canceled := l.result, l.wait, ctx.Done()
	var timer clock.Timer
	var timeout <-chan time.Time
	startTimer := func() {
		if timer == nil {
			timer, timeout = l.clock.NewTimer(l.timeout)
		}
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for resultCh != nil || waitCh != nil {
		select {
		case r = <-resultCh:
			resultCh = nil
			startTimer()
		case waited := <-waitCh:
			if !waited.reaped {
				l.completionErr = status.Errorf(codes.Internal, "Namespace helper wait did not confirm reaping: %v", waited.err)
				l.resultReader.Close()
				return nil, &CompletionUnknownError{Cause: l.completionErr}
			}
			l.helperReaped = true
			// A reported main result remains valid if init was killed afterwards.
			// Reaping init, rather than its exit status, confirms kernel teardown.
			waitCh = nil
			startTimer()
		case <-canceled:
			canceled = nil
			startTimer()
		case <-timeout:
			l.completionErr = status.Errorf(codes.DeadlineExceeded, "Namespace teardown exceeded %s", l.timeout)
			l.resultReader.Close()
			return nil, &CompletionUnknownError{Cause: l.completionErr}
		}
	}
	if r.err != nil && ctx.Err() != nil {
		return nil, util.StatusFromContext(ctx)
	}
	if r.err != nil {
		return nil, status.Errorf(codes.Internal, "Namespace helper did not report action result: %v", r.err)
	}
	if r.value.Error != "" {
		code := codes.Internal
		if r.value.InvalidArgument {
			code = codes.InvalidArgument
		}
		return nil, status.Errorf(code, "Namespace action setup failed: %s", r.value.Error)
	}
	usage, err := anypb.New(getPOSIXResourceUsageFromWait(&r.value.Usage, r.value.Status))
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to marshal POSIX resource usage")
	}
	return &runner_pb.RunResponse{
		ExitCode:      int64(r.value.Status.ExitStatus()),
		ResourceUsage: []*anypb.Any{usage},
	}, nil
}

func (l *pidNamespaceCommandLifecycle) Finish(context.Context) error {
	for _, f := range l.childFiles {
		f.Close()
	}
	l.childFiles = nil
	if l.lifetime != nil {
		l.lifetime.Close()
	}
	if l.requestWriter != nil {
		l.requestWriter.Close()
	}
	if l.resultReader != nil {
		defer l.resultReader.Close()
	}
	if l.cmd != nil {
		l.cmd.Process.Kill()
	}
	if l.completionErr != nil {
		return l.completionErr
	}
	if l.wait != nil && !l.helperReaped {
		// Also reap a successfully started helper if Wait was not called.
		timer, timeout := l.clock.NewTimer(l.timeout)
		defer timer.Stop()
		select {
		case waited := <-l.wait:
			if !waited.reaped {
				l.completionErr = status.Errorf(codes.Internal, "Namespace helper wait did not confirm reaping: %v", waited.err)
				return l.completionErr
			}
			l.helperReaped = true
		case <-timeout:
			l.completionErr = status.Errorf(codes.DeadlineExceeded, "Namespace helper could not be reaped within %s", l.timeout)
			return l.completionErr
		}
	}
	return nil
}
