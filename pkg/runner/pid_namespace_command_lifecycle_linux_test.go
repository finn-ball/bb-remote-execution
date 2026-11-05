package runner

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/runner/pidnamespace"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPIDNamespaceConfigurationValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		path    string
		timeout time.Duration
	}{
		"RelativeHelper": {"relative", time.Second},
		"ZeroTimeout":    {"/missing/helper", 0},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewPIDNamespaceCommandLifecycleFactory(tc.path, tc.timeout, 65533, 65533, &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}, clock.SystemClock)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Namespace helper must be absolute and cleanup timeout must be positive"), err)
		})
	}
	if os.Geteuid() != 0 {
		_, err := NewPIDNamespaceCommandLifecycleFactory("/missing/helper", time.Second, 65533, 65533, &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}, clock.SystemClock)
		testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "PID namespace containment requires a privileged runner"), err)
	} else {
		directory := t.TempDir()
		_, err := NewPIDNamespaceCommandLifecycleFactory(filepath.Join(directory, "missing"), time.Second, 65533, 65533, &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}, clock.SystemClock)
		testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to stat namespace helper: "), err)
		_, err = NewPIDNamespaceCommandLifecycleFactory(directory, time.Second, 65533, 65533, &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}, clock.SystemClock)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Namespace helper is not a regular executable"), err)
	}
	for name, tc := range map[string]struct {
		attributes *syscall.SysProcAttr
		message    string
	}{
		"MissingAttributes":  {nil, "PID namespace containment requires explicit non-root action credentials"},
		"MissingCredentials": {&syscall.SysProcAttr{}, "PID namespace containment requires explicit non-root action credentials"},
		"RootCredentials":    {&syscall.SysProcAttr{Credential: &syscall.Credential{}}, "PID namespace containment requires explicit non-root action credentials"},
		"Session":            {&syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 123}, Setsid: true}, "Unsupported action process attributes for PID namespace containment"},
	} {
		t.Run(name, func(t *testing.T) {
			l := &pidNamespaceCommandLifecycle{helperUID: 1000, helperGID: 1001}
			err := l.Configure(&exec.Cmd{SysProcAttr: tc.attributes})
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, tc.message), err)
			require.NoError(t, l.Finish(context.Background()))
		})
	}
}

func TestPIDNamespaceRequestAndStartFailure(t *testing.T) {
	l := &pidNamespaceCommandLifecycle{helper: "/absolute/helper", timeout: time.Second, helperUID: 1000, helperGID: 1001}
	credential := &syscall.Credential{Uid: 123, Gid: 456, Groups: []uint32{789, 456, 789}}
	attributes := &syscall.SysProcAttr{Credential: credential, Chroot: "/input"}
	cmd := &exec.Cmd{
		Path:        "/usr/bin/env",
		Args:        []string{"env", "--", "program", "one two", ""},
		Env:         []string{"PATH=/bin", "EMPTY=", "VALUE=one\ntwo"},
		Dir:         "/working",
		SysProcAttr: attributes,
	}
	expected := pidnamespace.Request{
		Path:       cmd.Path,
		Args:       cmd.Args,
		Env:        cmd.Env,
		Dir:        cmd.Dir,
		Chroot:     "/input",
		Credential: credential,
	}
	require.NoError(t, l.Configure(cmd))
	var actual pidnamespace.Request
	require.NoError(t, pidnamespace.Decode(cmd.ExtraFiles[0], &actual))
	require.Equal(t, expected, actual)
	require.Equal(t, l.helper, cmd.Path)
	require.Empty(t, cmd.Env)
	require.Equal(t, &syscall.SysProcAttr{
		Cloneflags:                 syscall.CLONE_NEWUSER | syscall.CLONE_NEWPID | syscall.CLONE_NEWNS,
		UidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: 1000, Size: 1}, {ContainerID: 123, HostID: 123, Size: 1}},
		GidMappings:                []syscall.SysProcIDMap{{ContainerID: 0, HostID: 1001, Size: 1}, {ContainerID: 456, HostID: 456, Size: 1}, {ContainerID: 789, HostID: 789, Size: 1}},
		GidMappingsEnableSetgroups: true,
		Credential:                 &syscall.Credential{Groups: []uint32{}},
	}, cmd.SysProcAttr)
	require.Equal(t, []uint32{789, 456, 789}, credential.Groups)
	require.Equal(t, "/input", attributes.Chroot)
	files := append([]*os.File{}, cmd.ExtraFiles...)
	l.Started(cmd, errors.New("start failed"))
	require.NoError(t, l.Finish(context.Background()))
	for _, f := range files {
		_, err := f.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
	}
}

func TestPIDNamespaceBlockedRequestWriterStartFailure(t *testing.T) {
	l := &pidNamespaceCommandLifecycle{helper: "/absolute/helper", timeout: time.Second, helperUID: 1000, helperGID: 1001}
	cmd := &exec.Cmd{Path: "/bin/true", Env: []string{"LARGE=" + strings.Repeat("x", 256*1024)}, SysProcAttr: &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 123, Gid: 456}}}
	require.NoError(t, l.Configure(cmd))
	l.Started(cmd, errors.New("start failed"))
	require.NoError(t, l.Finish(context.Background()))
	// Closing the pipe unblocks the pending Write even when no helper read it.
	for _, f := range []*os.File{l.requestWriter, cmd.ExtraFiles[0]} {
		_, err := f.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
	}
}

func namespaceWaitFixture(t *testing.T) *pidNamespaceCommandLifecycle {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		r.Close()
		w.Close()
	})
	return &pidNamespaceCommandLifecycle{clock: clock.SystemClock, timeout: time.Hour, resultReader: r, result: make(chan namespaceResult, 1), wait: make(chan namespaceWaitResult, 1)}
}

func TestPIDNamespaceMainResultRequiresHelperReaped(t *testing.T) {
	l := namespaceWaitFixture(t)
	l.result = make(chan namespaceResult)
	l.wait = make(chan namespaceWaitResult)
	done := make(chan error, 1)
	go func() {
		r, err := l.Wait(context.Background(), nil)
		if err == nil && r.ExitCode != 127 {
			err = errors.New("lost principal exit status")
		}
		done <- err
	}()
	l.result <- namespaceResult{value: pidnamespace.Result{Status: 127 << 8}}
	// The unbuffered send requires Wait to receive the reaping confirmation.
	// A helper killed after recording main completion still provides the kernel
	// teardown barrier. Do not replace the action status with init's SIGKILL.
	select {
	case <-done:
		t.Fatal("acknowledged while helper was not reaped")
	case l.wait <- namespaceWaitResult{err: &exec.ExitError{}, reaped: true}:
	}
	require.NoError(t, <-done)
}

func TestPIDNamespaceWaitTimeout(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		l := namespaceWaitFixture(t)
		expired := make(chan time.Time, 1)
		expired <- time.Unix(1000, 0)
		l.clock = &manualTimerClock{
			t:        t,
			duration: time.Hour,
			expired:  expired,
		}
		ctx, cancel := context.WithCancel(context.Background())
		if canceled {
			cancel()
		} else {
			l.result <- namespaceResult{}
		}
		_, err := l.Wait(ctx, nil)
		cancel()
		var unknown *CompletionUnknownError
		require.ErrorAs(t, err, &unknown)
		// Later reaping must never retroactively acknowledge this attempt.
		l.wait <- namespaceWaitResult{reaped: true}
		require.Error(t, l.Finish(context.Background()))
	}
}

func TestPIDNamespaceResultErrors(t *testing.T) {
	for _, tc := range []struct {
		result namespaceResult
		err    error
	}{
		{namespaceResult{err: errors.New("malformed or missing result")}, status.Error(codes.Internal, "Namespace helper did not report action result: malformed or missing result")},
		{namespaceResult{value: pidnamespace.Result{Error: "exec missing", InvalidArgument: true}}, status.Error(codes.InvalidArgument, "Namespace action setup failed: exec missing")},
		{namespaceResult{value: pidnamespace.Result{Error: "mount failed"}}, status.Error(codes.Internal, "Namespace action setup failed: mount failed")},
	} {
		l := namespaceWaitFixture(t)
		l.result <- tc.result
		l.wait <- namespaceWaitResult{reaped: true}
		_, err := l.Wait(context.Background(), nil)
		testutil.RequireEqualStatus(t, tc.err, err)
	}
}

func TestPIDNamespaceLargeArguments(t *testing.T) {
	l := &pidNamespaceCommandLifecycle{helper: "/absolute/helper", helperUID: 1000, helperGID: 1001}
	cmd := &exec.Cmd{
		Path:        "/bin/true",
		Args:        []string{"true"},
		SysProcAttr: &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 123, Gid: 456}},
	}
	// Below Linux's default 2 MiB execve() budget and 128 KiB per-string
	// limit, but above 1 MiB before JSON's sixfold escaping of '<'.
	for range 20 {
		cmd.Args = append(cmd.Args, strings.Repeat("<", 60000))
	}
	expected := append([]string{}, cmd.Args...)
	require.NoError(t, l.Configure(cmd))
	var request pidnamespace.Request
	require.NoError(t, pidnamespace.Decode(cmd.ExtraFiles[0], &request))
	require.Equal(t, expected, request.Args)
	l.Started(cmd, errors.New("start failed"))
	require.NoError(t, l.Finish(context.Background()))
}

func TestPIDNamespaceCanceledWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l := namespaceWaitFixture(t)
	l.result <- namespaceResult{err: errors.New("missing result")}
	l.wait <- namespaceWaitResult{reaped: true}
	_, err := l.Wait(ctx, nil)
	testutil.RequireEqualStatus(t, status.Error(codes.Canceled, "context canceled"), err)
}

func TestPIDNamespaceProtocolBounds(t *testing.T) {
	var result pidnamespace.Result
	input, err := os.Open("/dev/zero")
	require.NoError(t, err)
	defer input.Close()
	require.EqualError(t, pidnamespace.Decode(input, &result), "Namespace protocol message exceeds size limit")
}

func TestPIDNamespaceWaitErrorWithoutReaping(t *testing.T) {
	l := namespaceWaitFixture(t)
	l.result <- namespaceResult{}
	l.wait <- namespaceWaitResult{err: syscall.ECHILD}
	_, err := l.Wait(context.Background(), nil)
	var unknown *CompletionUnknownError
	require.ErrorAs(t, err, &unknown, "wait error without ProcessState must not acknowledge completion")
	require.Error(t, l.Finish(context.Background()))
}

func TestPIDNamespaceShadowedInvalidEnvironment(t *testing.T) {
	// Environ would discard this invalid entry because a later value shadows it.
	l := &pidNamespaceCommandLifecycle{helperUID: 1000, helperGID: 1001}
	cmd := &exec.Cmd{Env: []string{"KEY=bad\x00value", "KEY=valid"}, SysProcAttr: &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 123, Gid: 456}}}
	err := l.Configure(cmd)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to start process: exec: environment variable contains NUL"), err)
	require.Empty(t, l.childFiles)
	require.NoError(t, l.Finish(context.Background()))
}

func TestPIDNamespaceUserNamespaceValidation(t *testing.T) {
	identityError := status.Error(codes.InvalidArgument, "User namespace requires valid distinct nonzero helper and action identities and explicit supplementary groups")
	groupError := status.Error(codes.InvalidArgument, "User namespace requires valid nonzero action groups distinct from the helper group")
	for name, tc := range map[string]struct {
		modify      func(*syscall.Credential, *pidNamespaceCommandLifecycle)
		expectedErr error
	}{
		"ZeroHelperUID":        {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { ns.helperUID = 0 }, identityError},
		"ZeroHelperGID":        {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { ns.helperGID = 0 }, identityError},
		"SameUID":              {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { ns.helperUID = c.Uid }, identityError},
		"SamePrimaryGID":       {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { ns.helperGID = c.Gid }, groupError},
		"SameSupplementaryGID": {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { ns.helperGID = c.Groups[0] }, groupError},
		"ZeroPrimaryGID":       {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { c.Gid = 0 }, groupError},
		"ZeroSupplementaryGID": {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { c.Groups[0] = 0 }, groupError},
		"InvalidUID":           {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { c.Uid = ^uint32(0) }, identityError},
		"InvalidGID":           {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { c.Gid = ^uint32(0) }, groupError},
		"NoGroupReset":         {func(c *syscall.Credential, ns *pidNamespaceCommandLifecycle) { c.NoSetGroups = true }, identityError},
	} {
		t.Run(name, func(t *testing.T) {
			credential := &syscall.Credential{Uid: 123, Gid: 456, Groups: []uint32{789}}
			l := &pidNamespaceCommandLifecycle{helperUID: 1000, helperGID: 1001}
			tc.modify(credential, l)
			err := l.Configure(&exec.Cmd{SysProcAttr: &syscall.SysProcAttr{Credential: credential}})
			testutil.RequireEqualStatus(t, tc.expectedErr, err)
			require.Empty(t, l.childFiles)
			require.Nil(t, l.lifetime)
		})
	}
}
