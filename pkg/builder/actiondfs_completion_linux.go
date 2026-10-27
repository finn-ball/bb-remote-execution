package builder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// Retries are only a bounded EBUSY backoff, never evidence of completion.
func actiondfsNormalUnmount(target string) error {
	for attempt := 0; ; attempt++ {
		err := unix.Unmount(target, 0)
		if !errors.Is(err, unix.EBUSY) || attempt == 99 {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A private zero-valued eventfd is signalled only by this superblock's final
// put_super, after callbacks and backing paths are released. Parent unmount
// alone is insufficient when real helpers have cloned the mount namespace.
func actiondfsWaitCompletion(fd int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("actiondfs final-superblock completion timed out; backing retained")
		}
		p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		_, err := unix.Poll(p, int((remaining+time.Millisecond-1)/time.Millisecond))
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if p[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return fmt.Errorf("invalid actiondfs completion event: %#x", p[0].Revents)
		}
		if p[0].Revents&unix.POLLIN == 0 {
			continue
		}
		var value [8]byte
		n, err := unix.Read(fd, value[:])
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n != 8 || binary.NativeEndian.Uint64(value[:]) != 1 {
			return fmt.Errorf("invalid actiondfs completion count")
		}
		return nil
	}
}
