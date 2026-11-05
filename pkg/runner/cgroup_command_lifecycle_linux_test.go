package runner_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCgroupHelper executes inside a real cgroup. Re-executing the test binary
// keeps these fixtures independent of setsid(1), shells, and other host tools.
func TestCgroupHelper(t *testing.T) {
	mode := os.Getenv("BB_CGROUP_TEST_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "leaf":
		time.Sleep(time.Minute)
	case "migration":
		if err := os.WriteFile(filepath.Join(os.Getenv("BB_TEST_CGROUP_PARENT"), "cgroup.procs"), []byte("0"), 0); err == nil {
			os.Exit(5)
		}
	case "session", "double_fork", "fork_race", "stress":
		count := 1
		if mode == "stress" {
			count = 100
		}
		for i := 0; i < count; i++ {
			childMode := "leaf"
			if mode == "double_fork" {
				childMode = "session"
			}
			if mode == "fork_race" {
				childMode = "stress"
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCgroupHelper$")
			cmd.Env = append(os.Environ(), "BB_CGROUP_TEST_HELPER="+childMode)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if err := cmd.Start(); err != nil {
				os.Exit(6)
			}
			cmd.Process.Release()
			if mode == "session" || mode == "stress" {
				if err := os.WriteFile(os.Getenv("BB_CGROUP_TEST_READY"), []byte("ready"), 0600); err != nil {
					os.Exit(7)
				}
			}
		}
		if mode == "double_fork" || mode == "fork_race" {
			// Confirm the grandchild exists, or that the concurrent fork loop started,
			// before the direct parent exits and cleanup begins.
			deadline := time.Now().Add(3 * time.Second)
			for {
				if _, err := os.Stat(os.Getenv("BB_CGROUP_TEST_READY")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					os.Exit(8)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	os.Exit(0)
}

func cgroupTestFactory(t *testing.T) (runner.CommandLifecycleFactory, string) {
	t.Helper()
	parent := os.Getenv("BB_TEST_CGROUP_PARENT")
	if parent == "" {
		t.Skip("requires an explicitly delegated real cgroup v2 parent")
	}
	// Never use or clean another test's cgroups or the integration runner's parent.
	owned, err := os.MkdirTemp(parent, "kernel-test-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.Remove(owned)) })
	factory, err := runner.NewCgroupCommandLifecycleFactory(owned, 3*time.Second, clock.SystemClock)
	require.NoError(t, err)
	return factory, owned
}

func cgroupTestCommand(t *testing.T, factory runner.CommandLifecycleFactory, mode string) (*exec.Cmd, runner.CommandLifecycle, context.Context, context.CancelFunc) {
	t.Helper()
	lifecycle, err := factory()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCgroupHelper$")
	readyDirectory, err := os.MkdirTemp("", "cgroup-helper-")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(readyDirectory, 0777))
	t.Cleanup(func() { os.RemoveAll(readyDirectory) })
	cmd.Env = append(os.Environ(), "BB_CGROUP_TEST_HELPER="+mode, "BB_CGROUP_TEST_READY="+filepath.Join(readyDirectory, "ready"))
	// The privileged runner owns the controls; the action cannot migrate out.
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}, Chroot: ""}
	original := cmd.SysProcAttr
	require.NoError(t, lifecycle.Configure(cmd))
	require.NotSame(t, original, cmd.SysProcAttr)
	require.False(t, original.UseCgroupFD)
	require.Equal(t, original.Credential, cmd.SysProcAttr.Credential)
	require.True(t, cmd.SysProcAttr.UseCgroupFD)
	return cmd, lifecycle, ctx, cancel
}

func TestCgroupKernelContainmentAndIsolation(t *testing.T) {
	factory, parent := cgroupTestFactory(t)
	holder, holderLifecycle, holderCtx, cancelHolder := cgroupTestCommand(t, factory, "leaf")
	startErr := holder.Start()
	holderLifecycle.Started(holder, startErr)
	require.NoError(t, startErr)
	defer func() {
		cancelHolder()
		holderLifecycle.Wait(holderCtx, holder)
		require.NoError(t, holderLifecycle.Finish(context.Background()))
	}()
	for _, mode := range []string{"session", "double_fork", "fork_race", "migration"} {
		t.Run(mode, func(t *testing.T) {
			cmd, lifecycle, ctx, cancel := cgroupTestCommand(t, factory, mode)
			defer cancel()
			startErr := cmd.Start()
			lifecycle.Started(cmd, startErr)
			require.NoError(t, startErr, "requires clone3 support and migration permissions")
			response, err := lifecycle.Wait(ctx, cmd)
			require.NoError(t, err)
			require.Zero(t, response.ExitCode)
			require.NoError(t, lifecycle.Finish(context.Background()))
			require.NoError(t, holder.Process.Signal(syscall.Signal(0)), "another action must remain alive")
			entries, err := os.ReadDir(parent)
			require.NoError(t, err)
			directories := 0
			for _, entry := range entries {
				if entry.IsDir() {
					directories++
				}
			}
			require.Equal(t, 1, directories, "only the unrelated holder cgroup remains")
		})
	}
	t.Run("cancel", func(t *testing.T) {
		cmd, lifecycle, ctx, cancel := cgroupTestCommand(t, factory, "leaf")
		startErr := cmd.Start()
		lifecycle.Started(cmd, startErr)
		require.NoError(t, startErr)
		cancel()
		response, err := lifecycle.Wait(ctx, cmd)
		require.NoError(t, err)
		require.EqualValues(t, -1, response.ExitCode)
		// Even an already canceled execution context cannot interrupt cleanup.
		require.NoError(t, lifecycle.Finish(context.Background()))
		require.NoError(t, holder.Process.Signal(syscall.Signal(0)))
	})
}

func TestCgroupRejectsOrdinaryFilesystem(t *testing.T) {
	directory := t.TempDir()
	_, err := runner.NewCgroupCommandLifecycleFactory(directory, time.Second, clock.SystemClock)
	testutil.RequireEqualStatus(t, status.Errorf(codes.InvalidArgument, "%q is not a cgroup v2 filesystem", directory), err)
}

func TestCgroupRejectsRunnerIdentity(t *testing.T) {
	factory, _ := cgroupTestFactory(t)
	for _, attributes := range []*syscall.SysProcAttr{nil, {}, {Credential: &syscall.Credential{Uid: uint32(os.Geteuid())}}} {
		lifecycle, err := factory()
		require.NoError(t, err)
		require.Error(t, lifecycle.Configure(&exec.Cmd{SysProcAttr: attributes}))
		require.NoError(t, lifecycle.Finish(context.Background()))
	}
}
