//go:build linux

// Package pidnamespace implements the private protocol between bb_runner and
// bb_action_init. These descriptors and messages are never action environment.
package pidnamespace

import (
	"encoding/json"
	"fmt"
	"io"
	"syscall"
)

// MaximumMessageSize bounds every private protocol message in bytes. Linux
// permits up to 6 MiB of argument and environment data on architectures with an
// 8 MiB _STK_LIM. JSON escaping may expand each byte sixfold; leave additional
// space for metadata. The kernel still enforces its own execve() limits.
const MaximumMessageSize = 48 * 1024 * 1024

// Request describes the action before the outer command becomes its supervisor.
type Request struct {
	Path       string
	Args       []string
	Env        []string
	Dir        string
	Chroot     string
	Credential *syscall.Credential
	Probe      bool
}

// Result records principal action completion or a helper setup error.
type Result struct {
	Error           string
	InvalidArgument bool
	Status          syscall.WaitStatus
	Usage           syscall.Rusage
}

// Decode reads one bounded message, requiring the sender to close its endpoint.
func Decode(r io.Reader, v any) error {
	data, err := io.ReadAll(io.LimitReader(r, MaximumMessageSize+1))
	if err != nil {
		return fmt.Errorf("Failed to read namespace protocol message: %w", err)
	}
	if len(data) > MaximumMessageSize {
		return fmt.Errorf("Namespace protocol message exceeds size limit")
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("Failed to decode namespace protocol message: %w", err)
	}
	return nil
}

// Encode serializes one message, rejecting requests beyond the protocol bound.
func Encode(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("Failed to encode namespace protocol message: %w", err)
	}
	if len(data) > MaximumMessageSize {
		return nil, fmt.Errorf("Namespace protocol message exceeds size limit")
	}
	return data, nil
}
