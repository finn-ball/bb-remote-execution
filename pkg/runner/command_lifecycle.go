package runner

import (
	"context"
	"fmt"
	"os/exec"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CommandLifecycle owns containment for one command, including its descendants.
type CommandLifecycle interface {
	// Configure runs after the action environment and streams are final.
	Configure(*exec.Cmd) error
	// Started observes every Start attempt, releasing child-only descriptors.
	Started(*exec.Cmd, error)
	// Wait owns waiting for a successfully started command and returns the
	// principal action result, which may differ from the supervisor's result.
	Wait(context.Context, *exec.Cmd) (*runner_pb.RunResponse, error)
	// Finish also runs if Configure was not reached or Start failed. It must
	// bound its work independently of execution cancellation and may succeed
	// only once all contained processes have exited.
	Finish(context.Context) error
}

// CommandLifecycleFactory allocates a fresh containment domain for each attempt.
type CommandLifecycleFactory func() (CommandLifecycle, error)

// CompletionUnknownError means that a build directory cannot safely be reused.
// Callers must retain its references and stop admitting work to its build root.
type CompletionUnknownError struct{ Cause error }

func (e *CompletionUnknownError) Error() string {
	return fmt.Sprintf("Execution completion could not be confirmed: %v", e.Cause)
}

// Unwrap returns the error that prevented completion from being confirmed.
func (e *CompletionUnknownError) Unwrap() error { return e.Cause }

// GRPCStatus reports uncertain completion as unavailable.
func (e *CompletionUnknownError) GRPCStatus() *status.Status {
	return status.New(codes.Unavailable, e.Error())
}

// completionState is shared through decorators, including decorators that replace
// errors. A successful handler return alone is never a completion acknowledgement.
type (
	completionState    struct{ confirmed bool }
	completionStateKey struct{}
)

func setCompletionConfirmed(ctx context.Context, confirmed bool) {
	if state, ok := ctx.Value(completionStateKey{}).(*completionState); ok {
		state.confirmed = confirmed
	}
}
