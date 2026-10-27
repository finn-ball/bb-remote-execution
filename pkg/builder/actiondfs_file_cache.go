package builder

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ActiondfsFileCache stores verified input blobs in actiondfs's backing-file
// layout. It is a disposable worker cache, not a CAS service.
type ActiondfsFileCache struct {
	rootPath            string
	stagePath           string
	storage             blobstore.BlobAccess
	maxFiles            int
	maxSize             int64
	lock                sync.Mutex
	pins                map[string]int
	mounts              map[string]func() error
	files               map[string]int64
	size                int64
	eviction            eviction.Set[string]
	downloads           [256]*semaphore.Weighted
	downloadConcurrency *semaphore.Weighted
	cleanupError        error
	inputFetchTimeout   time.Duration
}

// NewActiondfsFileCache creates a bounded cache in a dedicated directory.
// The directory must not be shared with another worker.
func NewActiondfsFileCache(rootPath string, maxFiles int, maxSize int64, storage blobstore.BlobAccess, downloadConcurrency *semaphore.Weighted, inputFetchTimeout time.Duration) (*ActiondfsFileCache, error) {
	rootPath = filepath.Clean(rootPath)
	if !filepath.IsAbs(rootPath) || rootPath == "/" || maxFiles <= 0 || maxSize <= 0 {
		return nil, status.Error(codes.InvalidArgument, "actiondfs requires an absolute cache path and positive cache limits")
	}
	// Staging left after a crash may still belong to an attached mount.
	if entries, err := os.ReadDir(filepath.Join(rootPath, "stage")); err == nil {
		if len(entries) != 0 {
			return nil, status.Error(codes.FailedPrecondition, "actiondfs stage directory is not empty; recover previous mounts before restarting")
		}
	} else if !os.IsNotExist(err) {
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to inspect actiondfs backing directory %#v", filepath.Join(rootPath, "stage"))
	}
	cache := &ActiondfsFileCache{
		rootPath:            filepath.Join(rootPath, "blobs"),
		stagePath:           filepath.Join(rootPath, "stage"),
		storage:             storage,
		downloadConcurrency: downloadConcurrency,
		inputFetchTimeout:   inputFetchTimeout,
		maxFiles:            maxFiles,
		maxSize:             maxSize,
		files:               map[string]int64{},
		pins:                map[string]int{},
		mounts:              map[string]func() error{},
		eviction:            eviction.NewLRUSet[string](),
	}
	// Like the native worker cache, discard state from previous worker runs.
	// Never clear anything outside the two owned subdirectories.
	for _, directory := range []string{cache.rootPath, cache.stagePath} {
		if err := os.RemoveAll(directory); err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to clear actiondfs cache directory %#v", directory)
		}
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create actiondfs cache directory %#v", directory)
		}
	}
	for i := range cache.downloads {
		cache.downloads[i] = semaphore.NewWeighted(1)
	}
	return cache, nil
}

func (c *ActiondfsFileCache) getInstancePath(instance digest.InstanceName) (string, error) {
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(instance.String())))
	directory := filepath.Join(c.rootPath, key)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return "", util.StatusWrapfWithCode(err, codes.Internal, "Failed to create actiondfs instance directory %#v", directory)
	}
	return directory, nil
}

func (c *ActiondfsFileCache) fetch(ctx context.Context, blobDigest digest.Digest, pinned map[string]struct{}) error {
	ctx, cancel := context.WithTimeout(ctx, c.inputFetchTimeout)
	defer cancel()
	if blobDigest.GetSizeBytes() > c.maxSize {
		return status.Error(codes.ResourceExhausted, "Input blob exceeds actiondfs cache size")
	}
	lock := c.downloads[blobDigest.GetHashBytes()[0]]
	if err := util.AcquireSemaphore(ctx, lock, 1); err != nil {
		return err
	}
	defer lock.Release(1)
	instancePath, err := c.getInstancePath(blobDigest.GetInstanceName())
	if err != nil {
		return err
	}
	hash := blobDigest.GetHashString()
	destination := filepath.Join(instancePath, hash[:2], hash)
	c.lock.Lock()
	if size, ok := c.files[destination]; ok {
		if size != blobDigest.GetSizeBytes() {
			c.lock.Unlock()
			return status.Error(codes.InvalidArgument, "Inconsistent input blob size")
		}
		if _, ok := pinned[destination]; !ok {
			pinned[destination] = struct{}{}
			c.pins[destination]++
		}
		c.eviction.Touch(destination)
		c.lock.Unlock()
		return nil
	}
	c.lock.Unlock()
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create actiondfs cache directory %#v", filepath.Dir(destination))
	}
	if err := util.AcquireSemaphore(ctx, c.downloadConcurrency, 1); err != nil {
		return err
	}
	defer c.downloadConcurrency.Release(1)
	file, err := os.CreateTemp(filepath.Dir(destination), ".fetch-")
	if err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to create temporary actiondfs cache file in %#v", filepath.Dir(destination))
	}
	defer os.Remove(file.Name())
	// IntoWriter verifies the digest before the file becomes visible to the kernel.
	if err := c.storage.Get(ctx, blobDigest).IntoWriter(file); err != nil {
		file.Close()
		return err
	}
	if err := file.Chmod(0o444); err != nil {
		file.Close()
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to set actiondfs cache file permissions for %#v", file.Name())
	}
	if err := file.Close(); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to close actiondfs cache file %#v", file.Name())
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	for len(c.files) >= c.maxFiles || blobDigest.GetSizeBytes() > c.maxSize-c.size {
		victim := c.eviction.Peek()
		attempts := 0
		for c.pins[victim] > 0 {
			if attempts == len(c.files) {
				return status.Error(codes.ResourceExhausted, "actiondfs cache is full of active inputs")
			}
			c.eviction.Touch(victim)
			victim = c.eviction.Peek()
			attempts++
		}
		if err := os.Remove(victim); err != nil && !os.IsNotExist(err) {
			return util.StatusWrapfWithCode(err, codes.Internal, "Failed to remove cached actiondfs file %#v", victim)
		}
		c.size -= c.files[victim]
		delete(c.files, victim)
		c.eviction.Remove()
	}
	if err := os.Rename(file.Name(), destination); err != nil {
		return util.StatusWrapfWithCode(err, codes.Internal, "Failed to add cached actiondfs file %#v", destination)
	}
	c.files[destination] = blobDigest.GetSizeBytes()
	c.size += blobDigest.GetSizeBytes()
	c.eviction.Insert(destination)
	// Keep newly fetched files until their mount is fully destroyed, so eviction
	// cannot race the kernel's open immediately after the fetch reply.
	pinned[destination] = struct{}{}
	c.pins[destination]++
	return nil
}

func (c *ActiondfsFileCache) release(pinned map[string]struct{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for name := range pinned {
		c.pins[name]--
		if c.pins[name] == 0 {
			delete(c.pins, name)
		}
	}
}

// CheckHealthy prevents further actions and directory cleaning after failed
// cleanup. Backing files must remain available until the mount is recovered.
func (c *ActiondfsFileCache) CheckHealthy() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cleanupError
}

func (c *ActiondfsFileCache) failCleanup(err error) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.cleanupError == nil {
		c.cleanupError = err
	}
	return err
}

func (c *ActiondfsFileCache) registerMount(stage string, cleanup func() error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.mounts[stage] = cleanup
}

func (c *ActiondfsFileCache) unregisterMount(stage string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.mounts, stage)
}

// checkDrained is called after worker execution threads stop, before reporting
// successful shutdown. Never erase retained state to make this check pass.
func (c *ActiondfsFileCache) checkDrained() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.mounts) != 0 || len(c.pins) != 0 {
		return status.Errorf(codes.FailedPrecondition, "actiondfs shutdown retains %d mount owners and %d pinned blobs", len(c.mounts), len(c.pins))
	}
	return c.cleanupError
}

// DrainAfterExecutions retries retained mount cleanup after all worker executions
// have returned, then checks that shutdown released all mounts and pinned blobs.
func (c *ActiondfsFileCache) DrainAfterExecutions() error {
	c.lock.Lock()
	callbacks := make([]func() error, 0, len(c.mounts))
	for _, cleanup := range c.mounts {
		callbacks = append(callbacks, cleanup)
	}
	c.lock.Unlock()
	var group errgroup.Group
	for _, cleanup := range callbacks {
		group.Go(cleanup)
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return c.checkDrained()
}
