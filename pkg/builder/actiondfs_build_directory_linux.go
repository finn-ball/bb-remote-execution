package builder

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/access"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actiondfsBuildDirectory struct {
	BuildDirectory
	directoryPath       string
	cache               *ActiondfsFileCache
	storage             blobstore.BlobAccess
	flush               StorageFlusher
	flushContext        context.Context
	mounted             bool
	unmounted           bool
	drained             bool
	directoryClosed     bool
	directoryCloseError error
	completionFD        int
	stagePath           string
	connection          net.Conn
	cancel              context.CancelFunc
	done                chan struct{}
	pinned              map[string]struct{}
}

// NewActiondfsBuildDirectory creates Linux actiondfs mounts backed by a local
// input file cache. Execution and output collection use the normal worker path.
func NewActiondfsBuildDirectory(directory filesystem.DirectoryCloser, directoryPath string, cache *ActiondfsFileCache, storage blobstore.BlobAccess, flush StorageFlusher) BuildDirectory {
	return &actiondfsBuildDirectory{
		BuildDirectory: NewNaiveBuildDirectory(directory, nil, nil, nil, storage),
		directoryPath:  directoryPath,
		cache:          cache,
		storage:        storage,
		completionFD:   -1,
		flush:          flush,
	}
}

func (d *actiondfsBuildDirectory) EnterBuildDirectory(name path.Component) (BuildDirectory, error) {
	if err := d.cache.CheckHealthy(); err != nil {
		return nil, err
	}
	child, err := d.BuildDirectory.EnterBuildDirectory(name)
	if err != nil {
		return nil, err
	}
	return &actiondfsBuildDirectory{
		BuildDirectory: child,
		directoryPath:  filepath.Join(d.directoryPath, name.String()),
		cache:          d.cache,
		storage:        d.storage,
		completionFD:   -1,
		flush:          d.flush,
	}, nil
}

func (d *actiondfsBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, root digest.Digest, monitor access.UnreadDirectoryMonitor) error {
	if err := d.cache.CheckHealthy(); err != nil {
		return err
	}
	if root.GetDigestFunction().GetEnumValue() != remoteexecution.DigestFunction_SHA256 {
		return status.Error(codes.InvalidArgument, "actiondfs only supports SHA-256")
	}
	if d.flush == nil {
		return status.Error(codes.FailedPrecondition, "actiondfs requires a per-slot output flusher")
	}
	if d.mounted {
		return status.Error(codes.FailedPrecondition, "actiondfs input root is already mounted")
	}
	// Until mount succeeds, setup owns these resources. Once mounted, only
	// closeMount may release them after final superblock completion.
	mountOwned := false
	stage, err := os.MkdirTemp(d.cache.stagePath, "action-")
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create actiondfs staging directory in %#v", d.cache.stagePath)
	}
	var connection net.Conn
	completionFD := -1
	defer func() {
		if !mountOwned {
			if completionFD >= 0 {
				unix.Close(completionFD)
			}
			if connection != nil {
				connection.Close()
			}
			os.RemoveAll(stage)
		}
	}()
	if err := os.Chmod(stage, 0o777); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to set actiondfs staging permissions for %#v", stage)
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create actiondfs fetch socket pair")
	}
	defer unix.Close(fds[0])
	file := os.NewFile(uintptr(fds[1]), "actiondfs-fetch")
	connection, err = net.FileConn(file)
	file.Close()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to open actiondfs fetch connection")
	}
	cachePath, err := d.cache.getInstancePath(root.GetInstanceName())
	if err != nil {
		return util.StatusWrap(err, "Failed to create actiondfs instance directory")
	}
	completionFD, err = unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Internal, "Failed to create actiondfs completion eventfd")
	}
	data := fmt.Sprintf("root=%s,root_size=%d,cas=%s,stage=%s,fetch_fd=%d,completion_fd=%d", root.GetHashString(), root.GetSizeBytes(), cachePath, stage, fds[0], completionFD)
	if strings.ContainsAny(cachePath+stage, ",\x00") {
		return status.Error(codes.InvalidArgument, "actiondfs paths contain mount option delimiters")
	}
	if err := unix.Mount("actiondfs", d.directoryPath, "actiondfs", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOATIME, data); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to mount actiondfs at %#v", d.directoryPath)
	}
	mountOwned = true
	d.mounted = true
	d.completionFD = completionFD
	d.cache.registerMount(stage, d.closeMount)
	d.stagePath = stage
	d.connection = connection
	fetchCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	d.cancel = cancel
	d.done = make(chan struct{})
	d.pinned = map[string]struct{}{}
	go func() {
		defer close(d.done)
		d.serveFetches(fetchCtx, root.GetDigestFunction(), errorLogger)
	}()
	// The old descriptor refers to the directory underneath the mount.
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(d.directoryPath))
	if err != nil {
		err = util.StatusWrapfWithCode(err, codes.Internal, "Failed to open mounted actiondfs directory %#v", d.directoryPath)
		if cleanupErr := d.closeMount(); cleanupErr != nil {
			return util.StatusFromMultiple([]error{err, cleanupErr})
		}
		return err
	}
	oldDirectory := d.BuildDirectory
	d.BuildDirectory = NewNaiveBuildDirectory(directory, nil, nil, nil, d.storage)
	d.flushContext = ctx
	if err := oldDirectory.Close(); err != nil {
		err = util.StatusWrapfWithCode(err, codes.Internal, "Failed to close directory beneath actiondfs mount at %#v", d.directoryPath)
		if closeErr := d.Close(); closeErr != nil {
			return util.StatusFromMultiple([]error{err, closeErr})
		}
		return err
	}
	return nil
}

func (d *actiondfsBuildDirectory) serveFetches(ctx context.Context, function digest.Function, errorLogger util.ErrorLogger) {
	// A partial or malformed exchange must never be reused. Pins remain owned by mount cleanup.
	defer d.connection.Close()
	for {
		var request [40]byte
		if _, err := io.ReadFull(d.connection, request[:]); err != nil {
			if ctx.Err() == nil && err != io.EOF {
				errorLogger.Log(util.StatusWrap(err, "Failed to read actiondfs fetch request"))
			}
			return
		}
		size := binary.LittleEndian.Uint64(request[32:])
		if size > math.MaxInt64 {
			errorLogger.Log(status.Error(codes.InvalidArgument, "actiondfs fetch size exceeds int64"))
			return
		}
		blobDigest, err := function.NewDigest(hex.EncodeToString(request[:32]), int64(size))
		if err == nil {
			err = d.cache.fetch(ctx, blobDigest, d.pinned)
		}
		var response [4]byte
		if err != nil {
			// Preserve the original CAS status through LocalBuildExecutor's
			// I/O error hook instead of reducing it to the process's EIO.
			errorLogger.Log(util.StatusWrap(err, "Failed to fetch actiondfs input"))
			errno := unix.EIO
			switch {
			case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded:
				errno = unix.ETIMEDOUT
			case errors.Is(err, context.Canceled), status.Code(err) == codes.Canceled:
				errno = unix.ECANCELED
			case status.Code(err) == codes.NotFound:
				errno = unix.ENOENT
			case status.Code(err) == codes.PermissionDenied:
				errno = unix.EACCES
			}
			binary.LittleEndian.PutUint32(response[:], uint32(-int32(errno)))
		}
		for remaining := response[:]; len(remaining) > 0; {
			n, err := d.connection.Write(remaining)
			if err != nil || n == 0 {
				return
			}
			remaining = remaining[n:]
		}
	}
}

func (d *actiondfsBuildDirectory) closeMount() error {
	if !d.unmounted {
		err := actiondfsNormalUnmount(d.directoryPath)
		if err != nil {
			return d.cache.failCleanup(util.StatusWrap(err, "Normal unmount failed; fetch service, pins and backing retained"))
		}
		d.unmounted = true
	}
	if !d.drained {
		err := actiondfsWaitCompletion(d.completionFD, 2*time.Minute)
		if err != nil {
			return d.cache.failCleanup(util.StatusWrap(err, "Final actiondfs teardown not confirmed; backing retained"))
		}
		d.drained = true
	}
	// No filesystem consumers remain, including clones in other helpers.
	if d.done != nil {
		d.cancel()
		d.connection.Close()
		<-d.done
		d.done = nil
	}
	if d.completionFD >= 0 {
		unix.Close(d.completionFD)
		d.completionFD = -1
	}
	if d.pinned != nil {
		d.cache.release(d.pinned)
		d.pinned = nil
	}
	err := os.RemoveAll(d.stagePath)
	if err != nil {
		return d.cache.failCleanup(util.StatusWrap(err, "Failed to remove drained actiondfs staging"))
	}
	d.cache.unregisterMount(d.stagePath)
	d.mounted = false
	return nil
}

func (d *actiondfsBuildDirectory) Close() error {
	if !d.directoryClosed {
		// Flush mounted-file readers before closing the installed root. The
		// slot flusher consumes all buffers even when ctx is canceled, and its
		// outer completion flush preserves upload errors for digest pruning.
		var errs []error
		if d.flushContext != nil {
			if err := d.flush(d.flushContext); err != nil {
				errs = append(errs, err)
			}
		}
		if err := d.BuildDirectory.Close(); err != nil {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			d.directoryCloseError = util.StatusFromMultiple(errs)
		}
		d.directoryClosed = true
	}
	var mountErr error
	if d.mounted {
		mountErr = d.closeMount()
	}
	if d.directoryCloseError == nil {
		return mountErr
	}
	if mountErr != nil {
		return util.StatusFromMultiple([]error{d.directoryCloseError, mountErr})
	}
	return d.directoryCloseError
}

func (d *actiondfsBuildDirectory) RemoveAll(name path.Component) error {
	if err := d.cache.CheckHealthy(); err != nil {
		return err
	}
	return d.BuildDirectory.RemoveAll(name)
}
