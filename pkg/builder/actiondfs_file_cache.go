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
		return nil, util.StatusWrap(err, "Failed to inspect actiondfs backing directory")
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
		eviction:            eviction.NewLRUSet[string](),
	}
	// Like the native worker cache, discard state from previous worker runs.
	// Never clear anything outside the two owned subdirectories.
	for _, directory := range []string{cache.rootPath, cache.stagePath} {
		if err := os.RemoveAll(directory); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, err
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
	return directory, os.MkdirAll(directory, 0o755)
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
		return err
	}
	if err := util.AcquireSemaphore(ctx, c.downloadConcurrency, 1); err != nil {
		return err
	}
	defer c.downloadConcurrency.Release(1)
	file, err := os.CreateTemp(filepath.Dir(destination), ".fetch-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	// IntoWriter verifies the digest before the file becomes visible to the kernel.
	if err := c.storage.Get(ctx, blobDigest).IntoWriter(file); err != nil {
		file.Close()
		return err
	}
	if err := file.Chmod(0o444); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
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
			return err
		}
		c.size -= c.files[victim]
		delete(c.files, victim)
		c.eviction.Remove()
	}
	if err := os.Rename(file.Name(), destination); err != nil {
		return err
	}
	c.files[destination] = blobDigest.GetSizeBytes()
	c.size += blobDigest.GetSizeBytes()
	c.eviction.Insert(destination)
	// Keep newly fetched files until their mount is detached, so eviction
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

// CheckHealthy prevents further actions and directory cleaning after a failed
// detach. Backing files must remain available until the mount is recovered.
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
