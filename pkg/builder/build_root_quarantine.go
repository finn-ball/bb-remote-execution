package builder

import (
	"log"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BuildRootQuarantine is shared by all executors using one configured build
// root. A failed completion barrier permanently stops new admissions and holds
// directory references, preventing both per-action and idle root cleanup.
// Quarantine is memory-only. Recovery must confirm all prior actions are gone
// before restarting the worker, whose startup cleanup removes these directories.
type BuildRootQuarantine struct {
	lock     sync.Mutex
	cause    error
	retained []BuildDirectory
}

func (q *BuildRootQuarantine) check() error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.cause != nil {
		return status.Errorf(codes.Unavailable, "Build root quarantined: %v", q.cause)
	}
	return nil
}

func (q *BuildRootQuarantine) retain(path string, cause error, directories ...BuildDirectory) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.cause == nil {
		q.cause = cause
	}
	q.retained = append(q.retained, directories...)
	log.Printf("Build root quarantined; retaining action directory %q until worker shutdown: %v", path, cause)
}
