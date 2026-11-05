package runner

import (
	"context"
	"errors"
	"io"
	"time"

	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type executionControlServer struct{ base runner_pb.RunnerServer }

// NewExecutionControlServer wraps the fully decorated, contained runner. Only
// register this service when the local runner has a per-command lifecycle.
func NewExecutionControlServer(base runner_pb.RunnerServer) runner_pb.ExecutionControlServer {
	return &executionControlServer{base: base}
}

func (s *executionControlServer) Run(stream runner_pb.ExecutionControl_RunServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	request, ok := first.Request.(*runner_pb.ExecutionControlRequest_Run)
	if !ok || request.Run == nil {
		return status.Error(codes.InvalidArgument, "First control message must contain a run request")
	}
	// The worker owns execution timeouts (including suspendable I/O time). A
	// disappearing stream still cancels execution, but cannot cancel its cleanup.
	state := &completionState{confirmed: true} // A decorator may reject before Start.
	ctx, cancel := context.WithCancel(context.WithValue(context.WithoutCancel(stream.Context()), completionStateKey{}, state))
	defer cancel()
	protocolErrors := make(chan error, 1)
	go func() {
		canceled := false
		for {
			message, err := stream.Recv()
			if err != nil {
				cancel()
				if err != io.EOF {
					protocolErrors <- err
				}
				return
			}
			if _, ok := message.Request.(*runner_pb.ExecutionControlRequest_Cancel); !ok || canceled {
				protocolErrors <- status.Error(codes.InvalidArgument, "Expected a single cancel message")
				cancel()
				return
			}
			canceled = true
			cancel()
		}
	}()
	// This handler always waits for the underlying runner, even when the stream
	// has disappeared. In particular it never leaves an unowned cleanup goroutine.
	response, runErr := s.base.Run(ctx, request.Run)
	var unknown *CompletionUnknownError
	if errors.As(runErr, &unknown) {
		state.confirmed = false
	}
	select {
	case err := <-protocolErrors:
		if runErr == nil {
			runErr = err
		}
	default:
	}
	executionStatus := status.New(codes.OK, "")
	if runErr != nil {
		var ok bool
		executionStatus, ok = status.FromError(runErr)
		if !ok {
			executionStatus = status.FromContextError(runErr)
		}
	}
	return stream.Send(&runner_pb.ExecutionControlResponse{
		Result:              response,
		ExecutionStatus:     executionStatus.Proto(),
		CompletionConfirmed: state.confirmed,
	})
}

type completionRunnerClient struct {
	clock                  clock.Clock
	base                   runner_pb.RunnerClient
	control                runner_pb.ExecutionControlClient
	acknowledgementTimeout time.Duration
}

// NewCompletionRunnerClient requires explicit completion acknowledgements. It
// never falls back to the unary Run RPC, including against older runner servers.
func NewCompletionRunnerClient(base runner_pb.RunnerClient, control runner_pb.ExecutionControlClient, acknowledgementTimeout time.Duration, clock clock.Clock) runner_pb.RunnerClient {
	return &completionRunnerClient{
		clock:                  clock,
		base:                   base,
		control:                control,
		acknowledgementTimeout: acknowledgementTimeout,
	}
}

func (c *completionRunnerClient) CheckReadiness(ctx context.Context, request *runner_pb.CheckReadinessRequest, options ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.base.CheckReadiness(ctx, request, options...)
}

func (c *completionRunnerClient) Run(ctx context.Context, request *runner_pb.RunRequest, options ...grpc.CallOption) (*runner_pb.RunResponse, error) {
	if ctx.Err() != nil {
		return nil, util.StatusFromContext(ctx)
	}
	// Preserve tracing/auth values, but detach the transport deadline. Cancel is
	// an application message; the stream remains alive for the completion barrier.
	transportCtx, stopTransport := context.WithCancel(context.WithoutCancel(ctx))
	ready := make(chan runner_pb.ExecutionControl_RunClient, 1)
	done := make(chan struct{})
	stopped := make(chan struct{})
	defer func() {
		close(done)
		stopTransport()
		<-stopped
	}()
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
		case <-done:
			return
		}
		acknowledgementCtx, cancelAcknowledgement := c.clock.NewContextWithTimeout(transportCtx, c.acknowledgementTimeout)
		defer cancelAcknowledgement()
		stop := context.AfterFunc(acknowledgementCtx, stopTransport)
		defer stop()
		select {
		case stream := <-ready:
			// Send may block on a lost transport. The independent timer still ends it.
			_ = stream.Send(&runner_pb.ExecutionControlRequest{Request: &runner_pb.ExecutionControlRequest_Cancel{Cancel: &emptypb.Empty{}}})
		case <-done:
			return
		case <-transportCtx.Done():
			return
		}
		select {
		case <-done:
		case <-transportCtx.Done():
		}
	}()
	stream, err := c.control.Run(transportCtx, options...)
	if err != nil {
		return nil, &CompletionUnknownError{Cause: err}
	}
	// Accessing Context commits the gRPC attempt and disables retries. Do this
	// before sending Run: a configured retry policy must never replay an action
	// whose first attempt may still own processes, even if the replay completes.
	_ = stream.Context()
	if err := stream.Send(&runner_pb.ExecutionControlRequest{Request: &runner_pb.ExecutionControlRequest_Run{Run: request}}); err != nil {
		return nil, &CompletionUnknownError{Cause: err}
	}
	ready <- stream
	terminal, err := stream.Recv()
	if err != nil {
		return nil, &CompletionUnknownError{Cause: err}
	}
	if !terminal.CompletionConfirmed {
		cause := status.ErrorProto(terminal.ExecutionStatus)
		if cause == nil {
			cause = status.Error(codes.Unavailable, "Runner did not confirm process exit")
		}
		return nil, &CompletionUnknownError{Cause: cause}
	}
	if terminal.ExecutionStatus == nil {
		return nil, &CompletionUnknownError{Cause: status.Error(codes.Unavailable, "Missing terminal execution status")}
	}
	if ctx.Err() != nil {
		return nil, util.StatusFromContext(ctx)
	}
	if err := status.ErrorProto(terminal.ExecutionStatus); err != nil {
		return nil, err
	}
	if terminal.Result == nil {
		return nil, &CompletionUnknownError{Cause: status.Error(codes.Unavailable, "Missing terminal result")}
	}
	return terminal.Result, nil
}
