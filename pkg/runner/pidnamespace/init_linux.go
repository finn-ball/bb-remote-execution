package pidnamespace

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

// RunInit must be invoked only by the dedicated helper executable. The kernel
// kills every process in the PID namespace when this PID 1 exits, and does not
// finish reaping PID 1 in the parent until that teardown has completed.
func RunInit() {
	request := os.NewFile(3, "request")
	lifetime := os.NewFile(4, "lifetime")
	result := os.NewFile(5, "result")
	// Start this before decoding/setup: loss of the sole runner-side writer also
	// terminates a helper whose initialization has not completed yet.
	go func() {
		io.Copy(io.Discard, lifetime)
		os.Exit(1)
	}()
	finish := func(r Result) {
		if data, err := Encode(r); err == nil {
			result.Write(data)
		}
		result.Close()
	}
	fail := func(err error) {
		finish(Result{Error: err.Error()})
		os.Exit(1)
	}
	if os.Getpid() != 1 {
		fail(fmt.Errorf("Helper must run as namespace PID 1"))
	}
	// A process group inherited from outside this PID namespace appears as zero
	// here. kill(0, sig) still targets that outer group, including sibling actions.
	// Establish a private group before launching any untrusted process.
	if err := syscall.Setpgid(0, 0); err != nil {
		fail(fmt.Errorf("Failed to create action process group: %w", err))
	}
	var r Request
	if err := Decode(request, &r); err != nil {
		fail(err)
	}
	request.Close()
	if r.Credential == nil || r.Credential.Uid == 0 {
		fail(fmt.Errorf("Namespace actions require explicit non-root credentials"))
	}
	runtime.LockOSThread()
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		fail(fmt.Errorf("Failed to privatize mounts: %w", err))
	}
	if r.Chroot != "" {
		if err := syscall.Chroot(r.Chroot); err != nil {
			fail(fmt.Errorf("Failed to enter action chroot %q: %w", r.Chroot, err))
		}
		if err := os.Chdir("/"); err != nil {
			fail(fmt.Errorf("Failed to change directory to chroot root: %w", err))
		}
	}
	// Never create a directory or follow a symlink in shared action inputs.
	st, err := os.Lstat("/proc")
	if err != nil {
		fail(fmt.Errorf("Failed to stat /proc: %w", err))
	}
	if !st.IsDir() {
		fail(fmt.Errorf("Namespace /proc must be an existing real directory"))
	}
	if err := unix.Mount("proc", "/proc", "proc", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); err != nil {
		fail(fmt.Errorf("Failed to mount proc: %w", err))
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		fail(fmt.Errorf("Failed to disable privilege gains: %w", err))
	}
	if r.Probe {
		finish(Result{})
		os.Exit(0)
	}
	// Keep the helper's private descriptors out of the action. ProcAttr.Files
	// maps the standard streams, but does not close other inherited descriptors.
	syscall.CloseOnExec(4)
	syscall.CloseOnExec(5)
	process, err := os.StartProcess(r.Path, r.Args, &os.ProcAttr{
		Dir:   r.Dir,
		Env:   r.Env,
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Sys:   &syscall.SysProcAttr{Credential: r.Credential},
	})
	if err != nil {
		invalidArgument := errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOEXEC)
		finish(Result{Error: fmt.Sprintf("Failed to start action: %v", err), InvalidArgument: invalidArgument})
		os.Exit(1)
	}
	mainPID := process.Pid
	// This is the sole reaping owner, including orphans adopted while the main
	// process runs. Do not call process.Wait in addition to wait4.
	for {
		var status syscall.WaitStatus
		var usage syscall.Rusage
		pid, err := syscall.Wait4(-1, &status, 0, &usage)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			fail(fmt.Errorf("Failed to wait for action: %w", err))
		}
		if pid == mainPID {
			process.Release()
			finish(Result{Status: status, Usage: usage})
			os.Exit(0)
		}
	}
}
