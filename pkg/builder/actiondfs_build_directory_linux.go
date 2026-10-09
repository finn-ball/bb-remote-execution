package builder

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

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
	directoryPath string
	cache         *ActiondfsFileCache
	storage       blobstore.BlobAccess
	mounted       bool
	stagePath     string
	connection    net.Conn
	cancel        context.CancelFunc
	done          chan struct{}
	pinned        map[string]struct{}
}

// NewActiondfsBuildDirectory creates Linux actiondfs mounts backed by a local
// input file cache. Execution and output collection use the normal worker path.
func NewActiondfsBuildDirectory(directory filesystem.DirectoryCloser, directoryPath string, cache *ActiondfsFileCache, storage blobstore.BlobAccess) BuildDirectory {
	return &actiondfsBuildDirectory{
		BuildDirectory: NewNaiveBuildDirectory(directory, nil, nil, nil, storage),
		directoryPath:  directoryPath,
		cache:          cache,
		storage:        storage,
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
	}, nil
}

func (d *actiondfsBuildDirectory) MergeDirectoryContents(ctx context.Context, errorLogger util.ErrorLogger, root digest.Digest, monitor access.UnreadDirectoryMonitor) error {
	if root.GetDigestFunction().GetEnumValue() != remoteexecution.DigestFunction_SHA256 {
		return status.Error(codes.InvalidArgument, "actiondfs only supports SHA-256")
	}
	if d.mounted {
		return status.Error(codes.FailedPrecondition, "actiondfs input root is already mounted")
	}
	stage, err := os.MkdirTemp(d.cache.stagePath, "action-")
	if err != nil {
		return err
	}
	if err := os.Chmod(stage, 0o777); err != nil {
		os.RemoveAll(stage)
		return err
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		os.RemoveAll(stage)
		return err
	}
	defer unix.Close(fds[0])
	file := os.NewFile(uintptr(fds[1]), "actiondfs-fetch")
	connection, err := net.FileConn(file)
	file.Close()
	if err != nil {
		os.RemoveAll(stage)
		return err
	}
	cachePath, err := d.cache.getInstancePath(root.GetInstanceName())
	if err != nil {
		connection.Close()
		os.RemoveAll(stage)
		return err
	}
	data := fmt.Sprintf("root=%s,root_size=%d,cas=%s,stage=%s,fetch_fd=%d", root.GetHashString(), root.GetSizeBytes(), cachePath, stage, fds[0])
	if strings.ContainsAny(cachePath+stage, ",\x00") {
		connection.Close()
		os.RemoveAll(stage)
		return status.Error(codes.InvalidArgument, "actiondfs paths contain mount option delimiters")
	}
	if err := unix.Mount("actiondfs", d.directoryPath, "actiondfs", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOATIME, data); err != nil {
		connection.Close()
		os.RemoveAll(stage)
		return util.StatusWrap(err, "Failed to mount actiondfs")
	}
	d.mounted = true
	d.stagePath = stage
	d.connection = connection
	fetchCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.done = make(chan struct{})
	d.pinned = map[string]struct{}{}
	go func() {
		defer close(d.done)
		stop := context.AfterFunc(fetchCtx, func() { connection.Close() })
		defer stop()
		d.serveFetches(fetchCtx, root.GetDigestFunction(), errorLogger)
	}()
	// The old descriptor refers to the directory underneath the mount.
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(d.directoryPath))
	if err != nil {
		d.closeMount()
		return err
	}
	d.BuildDirectory.Close()
	d.BuildDirectory = NewNaiveBuildDirectory(directory, nil, nil, nil, d.storage)
	return nil
}

func (d *actiondfsBuildDirectory) serveFetches(ctx context.Context, function digest.Function, errorLogger util.ErrorLogger) {
	for {
		var request [40]byte
		if _, err := io.ReadFull(d.connection, request[:]); err != nil {
			if ctx.Err() == nil && err != io.EOF {
				errorLogger.Log(util.StatusWrap(err, "Failed to read actiondfs fetch request"))
			}
			return
		}
		blobDigest, err := function.NewDigest(hex.EncodeToString(request[:32]), int64(binary.LittleEndian.Uint64(request[32:])))
		if err == nil {
			err = d.cache.fetch(ctx, blobDigest, d.pinned)
		}
		var response [4]byte
		if err != nil {
			// Preserve the original CAS status through LocalBuildExecutor's
			// I/O error hook instead of reducing it to the process's EIO.
			errorLogger.Log(util.StatusWrap(err, "Failed to fetch actiondfs input"))
			binary.LittleEndian.PutUint32(response[:], ^uint32(unix.EIO-1))
		}
		if _, err := d.connection.Write(response[:]); err != nil {
			return
		}
	}
}

func (d *actiondfsBuildDirectory) closeMount() error {
	d.cancel()
	d.connection.Close()
	<-d.done
	// Detach the action path; outstanding mappings retain their backing files
	// until Linux finishes releasing the old mount. Pending uploads likewise
	// retain open files and are flushed by StorageFlushingBuildExecutor.
	if err := unix.Unmount(d.directoryPath, unix.MNT_DETACH); err != nil {
		return d.cache.failCleanup(util.StatusWrap(err, "Failed to unmount actiondfs; backing files preserved"))
	}
	d.mounted = false
	d.cache.release(d.pinned)
	return os.RemoveAll(d.stagePath)
}

func (d *actiondfsBuildDirectory) Close() error {
	err := d.BuildDirectory.Close()
	if d.mounted {
		if mountErr := d.closeMount(); err == nil {
			err = mountErr
		}
	}
	return err
}

func (d *actiondfsBuildDirectory) RemoveAll(name path.Component) error {
	if err := d.cache.CheckHealthy(); err != nil {
		return err
	}
	return d.BuildDirectory.RemoveAll(name)
}
