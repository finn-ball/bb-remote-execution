//go:build !linux

package builder

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// NewActiondfsBuildDirectory is only supported on Linux.
func NewActiondfsBuildDirectory(directory filesystem.DirectoryCloser, directoryPath string, cache *ActiondfsFileCache, storage blobstore.BlobAccess) BuildDirectory {
	panic("actiondfs requires Linux")
}
