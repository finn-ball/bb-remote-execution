package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mode, ready := os.Args[1], os.Args[2]
	switch mode {
	case "leaf":
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGUSR1)
		f, err := os.OpenFile(ready, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
		if err != nil {
			return err
		}
		// Retain the lock until the kernel closes this process's descriptors.
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			return err
		}
		ticker := time.NewTicker(time.Millisecond)
		for {
			if _, err := f.WriteString("x"); err != nil {
				return err
			}
			select {
			case <-signals:
				if err := os.WriteFile(ready+"-signaled", []byte("SIGUSR1"), 0o666); err != nil {
					return err
				}
			case <-ticker.C:
			}
		}
	case "exit", "hold":
		cmd := exec.Command(os.Args[0], "leaf", ready)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			return err
		}
		if err := cmd.Process.Release(); err != nil {
			return err
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			if st, err := os.Stat(ready); err == nil && st.Size() > 0 {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("detached descendant did not become ready")
			}
			time.Sleep(time.Millisecond)
		}
		if mode == "hold" {
			for {
				time.Sleep(time.Hour)
			}
		}
	case "signal":
		if os.Getpid() == 1 || os.Getppid() != 1 || os.Getuid() != 65534 || os.Getgid() != 65534 {
			return fmt.Errorf("incorrect action identity: pid=%d ppid=%d uid=%d gid=%d", os.Getpid(), os.Getppid(), os.Getuid(), os.Getgid())
		}
		for file, expected := range map[string]string{
			"/proc/1/status":    "NSpid:\t1\n",
			"/proc/self/status": "NoNewPrivs:\t1\n",
		} {
			data, err := os.ReadFile(file)
			if err != nil || !strings.Contains(string(data), expected) {
				return fmt.Errorf("unexpected %s: %s: %v", file, data, err)
			}
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGUSR1)
		if err := syscall.Kill(0, syscall.SIGUSR1); err != nil {
			return err
		}
		if syscall.Getpgrp() == 0 {
			return fmt.Errorf("action inherited an outer process group")
		}
		select {
		case <-signals:
		case <-time.After(time.Second):
			return fmt.Errorf("group signal did not reach sending action")
		}
	default:
		return fmt.Errorf("unknown action mode %q", mode)
	}
	return nil
}
