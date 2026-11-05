package runner

import (
	"context"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type completionRequiredRunner struct {
	runner_pb.RunnerServer
}

// NewCompletionRequiredRunner preserves readiness checks while requiring
// ExecutionControl.Run for action execution.
func NewCompletionRequiredRunner(base runner_pb.RunnerServer) runner_pb.RunnerServer {
	return &completionRequiredRunner{RunnerServer: base}
}

func (*completionRequiredRunner) Run(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "Action execution requires ExecutionControl.Run")
}
