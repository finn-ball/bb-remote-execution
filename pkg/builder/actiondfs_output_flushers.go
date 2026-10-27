package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/util"
)

// NewActiondfsOutputFlushers preserves an early upload error for the existing
// outer StorageFlushingBuildExecutor. That executor must still see the error
// so it can prune output digests whose blobs may not have reached CAS. State is
// private to one slot, called sequentially, and reset by the outer completion
// flush for the next action.
func NewActiondfsOutputFlushers(flush StorageFlusher) (StorageFlusher, StorageFlusher) {
	var earlyError error
	beforeClose := func(ctx context.Context) error {
		earlyError = flush(ctx)
		return earlyError
	}
	afterExecute := func(ctx context.Context) error {
		err := flush(ctx)
		saved := earlyError
		earlyError = nil
		if saved == nil {
			return err
		}
		if err != nil {
			return util.StatusFromMultiple([]error{saved, err})
		}
		return saved
	}
	return beforeClose, afterExecute
}
