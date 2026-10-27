package builder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/program"
)

// LaunchActiondfsDrain must be a sibling of the execution threads, not a
// dependency. Otherwise RPC/CAS dependencies may close concurrently with mount
// recovery and prevent legitimate retained-namespace fetches from completing.
// launched closes once the caller can no longer append to the worker list,
// including partial startup failures. Each done closes after its executor returns.
func LaunchActiondfsDrain(group program.Group, launched <-chan struct{}, workers func() []<-chan struct{}, cache *ActiondfsFileCache) {
	group.Go(func(ctx context.Context, _, _ program.Group) error {
		<-ctx.Done()
		<-launched
		for _, done := range workers() {
			<-done
		}
		return cache.DrainAfterExecutions()
	})
}
