package runner_test

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/stretchr/testify/require"
)

// TestPIDNamespaceKernel requires a disposable privileged Linux environment.
// Opt in with bazel test //pkg/runner:pid_namespace_kernel_test
// --test_env=BB_PID_NAMESPACE_HELPER=/absolute/bb_action_init. The helper and
// action runfiles must be accessible to UID 65534.
func TestPIDNamespaceKernel(t *testing.T) {
	helper := os.Getenv("BB_PID_NAMESPACE_HELPER")
	if helper == "" {
		t.Skip("set BB_PID_NAMESPACE_HELPER to opt into real Linux namespace tests")
	}
	factory, err := runner.NewPIDNamespaceCommandLifecycleFactory(helper, 3*time.Second, clock.SystemClock)
	require.NoError(t, err)
	rf, err := runfiles.New()
	require.NoError(t, err)
	action, err := rf.Rlocation(os.Getenv("BB_PID_NAMESPACE_ACTION_RUNFILE_PATH"))
	require.NoError(t, err)
	root, err := os.MkdirTemp("/tmp", "bb-pidns-kernel-")
	require.NoError(t, err)
	defer os.RemoveAll(root)
	require.NoError(t, os.Chmod(root, 0o777))
	// Keep a broken process-group implementation from killing the test driver.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)
	defer signal.Stop(signals)
	start := func(t *testing.T, cmd *exec.Cmd) runner.CommandLifecycle {
		lifecycle, err := factory()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, lifecycle.Finish(context.Background())) })
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		require.NoError(t, lifecycle.Configure(cmd))
		err = cmd.Start()
		lifecycle.Started(cmd, err)
		require.NoError(t, err)
		return lifecycle
	}
	ready := func(file string) bool {
		st, err := os.Stat(file)
		return err == nil && st.Size() > 0
	}
	siblingFile := filepath.Join(root, "sibling")
	sibling := exec.Command(action, "leaf", siblingFile)
	start(t, sibling)
	require.Eventually(t, func() bool { return ready(siblingFile) }, 5*time.Second, time.Millisecond)
	for _, mode := range []string{"exit", "hold", "signal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			file := filepath.Join(root, mode)
			cmd := exec.CommandContext(ctx, action, mode, file)
			lifecycle := start(t, cmd)
			if mode == "hold" {
				require.Eventually(t, func() bool { return ready(file) }, 5*time.Second, time.Millisecond)
				cancel()
			}
			response, err := lifecycle.Wait(ctx, cmd)
			if mode == "hold" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Zero(t, response.ExitCode)
			}
			if mode != "signal" {
				f, err := os.Open(file)
				require.NoError(t, err)
				defer f.Close()
				// Wait must release the detached writer's lock before acknowledging
				// completion, without needing Finish to kill remaining processes.
				require.NoError(t, syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
			}
			before, err := os.Stat(siblingFile)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				after, err := os.Stat(siblingFile)
				return err == nil && after.Size() > before.Size()
			}, time.Second, time.Millisecond, "sibling stopped writing after another action completed")
		})
	}
	// Give the live sibling's signal handler time to expose an escaped signal.
	require.Never(t, func() bool {
		_, err := os.Stat(siblingFile + "-signaled")
		return !os.IsNotExist(err)
	}, 50*time.Millisecond, time.Millisecond, "action group signal reached sibling namespace")
}
