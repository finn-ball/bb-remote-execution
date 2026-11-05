package runner_test

import (
	"context"
	"errors"
	"net"
	"os/exec"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/buildbarn/bb-remote-execution/internal/mock"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

type testLifecycle struct {
	configure func(*exec.Cmd) error
	finish    func(context.Context) error
}

func (l testLifecycle) Configure(cmd *exec.Cmd) error    { return l.configure(cmd) }
func (l testLifecycle) Finish(ctx context.Context) error { return l.finish(ctx) }
func (l testLifecycle) Started(*exec.Cmd, error)           {}
func (testLifecycle) Wait(ctx context.Context, cmd *exec.Cmd) (*runner_pb.RunResponse, error) {
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return nil, err
		}
	}
	return &runner_pb.RunResponse{ExitCode: int64(cmd.ProcessState.ExitCode())}, nil
}

func localLifecycleRunner(t *testing.T, command string, lifecycle runner.CommandLifecycle) runner_pb.RunnerServer {
	t.Helper()
	directoryPath := t.TempDir()
	directory, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(directoryPath))
	require.NoError(t, err)
	t.Cleanup(func() { directory.Close() })
	builder, walker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.LocalFormat.NewParser(directoryPath), walker))
	creator := func(ctx context.Context, args []string, root *path.Builder, working path.Parser, env string) (*exec.Cmd, error) {
		return exec.CommandContext(ctx, command), nil
	}
	return runner.NewLocalRunnerWithLifecycle(directory, builder, creator, false, func() (runner.CommandLifecycle, error) { return lifecycle, nil })
}

func TestCommandLifecycleAllExitPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses Unix executables")
	}
	for _, tc := range []struct {
		name                string
		failedStart         bool
		failedLog           bool
		failedConfiguration bool
		canceled            bool
		deadline            bool
		cleanupFailure      bool
		expectedError       error
	}{
		{name: "Success"},
		{
			name:        "FailedStart",
			failedStart: true,
		},
		{
			name:      "FailedLog",
			failedLog: true,
		},
		{
			name:                "FailedConfiguration",
			failedConfiguration: true,
			expectedError:       status.Error(codes.InvalidArgument, "Configuration failed"),
		},
		{
			name:          "Canceled",
			canceled:      true,
			expectedError: status.Error(codes.Canceled, "Failed to start process: context canceled"),
		},
		{
			name:           "CleanupFailure",
			cleanupFailure: true,
		},
		{
			name:           "CleanupFailureFailedStart",
			cleanupFailure: true,
			failedStart:    true,
		},
		{
			name:          "Deadline",
			deadline:      true,
			expectedError: status.Error(codes.DeadlineExceeded, "Failed to start process: context deadline exceeded"),
		},
		{
			name:           "CleanupFailureCanceled",
			cleanupFailure: true,
			canceled:       true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configured, finished := 0, 0
			lifecycle := testLifecycle{
				configure: func(cmd *exec.Cmd) error {
					configured++
					require.Contains(t, cmd.Env, "EXACT=one\ntwo")
					require.NotNil(t, cmd.Stdout)
					require.NotNil(t, cmd.Stderr)
					if tc.failedConfiguration {
						return status.Error(codes.InvalidArgument, "Configuration failed")
					}
					return nil
				},
				finish: func(ctx context.Context) error {
					finished++
					require.NoError(t, ctx.Err(), "cleanup context must be independent of action cancellation")
					if tc.cleanupFailure {
						return syscall.EBUSY
					}
					return nil
				},
			}
			command := "/bin/true"
			if tc.failedStart {
				command = "/nonexistent"
			}
			r := localLifecycleRunner(t, command, lifecycle)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.deadline {
				var expire context.CancelFunc
				ctx, expire = context.WithDeadline(ctx, time.Unix(0, 0))
				defer expire()
			}
			if tc.canceled {
				cancel()
			}
			request := &runner_pb.RunRequest{
				Arguments:            []string{"command"},
				EnvironmentVariables: map[string]string{"EXACT": "one\ntwo"},
				StdoutPath:           "stdout",
				StderrPath:           "stderr",
			}
			if tc.failedLog {
				request.StdoutPath = "missing/stdout"
			}
			_, err := r.Run(ctx, request)
			if tc.failedLog {
				require.Equal(t, 0, configured, "configuration follows final environment and stream setup")
			} else {
				require.Equal(t, 1, configured)
			}
			require.Equal(t, 1, finished)
			if tc.expectedError != nil {
				testutil.RequireEqualStatus(t, tc.expectedError, err)
			} else if tc.failedStart || tc.failedLog || tc.cleanupFailure {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.cleanupFailure {
				var unknown *runner.CompletionUnknownError
				require.ErrorAs(t, err, &unknown)
				require.Equal(t, codes.Unavailable, status.Code(err))
			}
		})
	}
}

type functionRunner struct {
	runner_pb.RunnerServer
	run func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error)
}

func (r functionRunner) Run(ctx context.Context, req *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	return r.run(ctx, req)
}

func controlConnection(t *testing.T, base runner_pb.RunnerServer) (*grpc.ClientConn, *grpc.Server) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	runner_pb.RegisterExecutionControlServer(server, runner.NewExecutionControlServer(base))
	go server.Serve(listener)
	connection, err := grpc.NewClient("passthrough:///runner", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { connection.Close(); server.Stop(); listener.Close() })
	return connection, server
}

func TestCompletionClientWaitsAfterCancellation(t *testing.T) {
	canceled, release := make(chan struct{}), make(chan struct{})
	started := make(chan struct{})
	connection, _ := controlConnection(t, functionRunner{run: func(ctx context.Context, _ *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
		_, deadline := ctx.Deadline()
		require.False(t, deadline, "the worker owns the execution timeout")
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return &runner_pb.RunResponse{}, nil
	}})
	client := runner.NewCompletionRunnerClient(nil, runner_pb.NewExecutionControlClient(connection), time.Second, clock.SystemClock)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.Run(ctx, &runner_pb.RunRequest{}); done <- err }()
	<-started
	cancel()
	<-canceled
	select {
	case err := <-done:
		t.Fatalf("returned before cleanup acknowledgement: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	require.Equal(t, codes.Canceled, status.Code(<-done))
}

func TestCompletionUnknownOnLostTransportOrAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		acknowledgementTimeout bool
		transportLoss          bool
		unconfirmed            bool
	}{
		{
			name:                   "AcknowledgementTimeout",
			acknowledgementTimeout: true,
		},
		{
			name:          "TransportLoss",
			transportLoss: true,
		},
		{
			name:        "Unconfirmed",
			unconfirmed: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			connection, server := controlConnection(t, functionRunner{run: func(ctx context.Context, _ *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
				close(started)
				<-release
				if tc.unconfirmed {
					return nil, &runner.CompletionUnknownError{Cause: syscall.EBUSY}
				}
				return &runner_pb.RunResponse{}, nil
			}})
			ctrl := gomock.NewController(t)
			mockClock := mock.NewMockClock(ctrl)
			expireAcknowledgement := make(chan context.CancelFunc, 1)
			if tc.acknowledgementTimeout {
				mockClock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Minute).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
					ctx, cancel := context.WithCancel(parent)
					expireAcknowledgement <- cancel
					return ctx, cancel
				})
			}
			client := runner.NewCompletionRunnerClient(nil, runner_pb.NewExecutionControlClient(connection), time.Minute, mockClock)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := client.Run(ctx, &runner_pb.RunRequest{}); done <- err }()
			<-started
			switch {
			case tc.acknowledgementTimeout:
				cancel()
				(<-expireAcknowledgement)()
			case tc.transportLoss:
				server.Stop()
			case tc.unconfirmed:
				close(release)
			}
			err := <-done
			var unknown *runner.CompletionUnknownError
			require.ErrorAs(t, err, &unknown)
			if tc.unconfirmed {
				require.Contains(t, err.Error(), syscall.EBUSY.Error())
			}
			if !tc.unconfirmed {
				close(release)
			}
		})
	}
}

type blockedCancelClient struct {
	runner_pb.ExecutionControlClient
	cancelSendEntered chan struct{}
}

func (c blockedCancelClient) Run(ctx context.Context, options ...grpc.CallOption) (runner_pb.ExecutionControl_RunClient, error) {
	stream, err := c.ExecutionControlClient.Run(ctx, options...)
	if err != nil {
		return nil, err
	}
	return blockedCancelStream{
		ExecutionControl_RunClient: stream,
		cancelSendEntered:          c.cancelSendEntered,
	}, nil
}

type blockedCancelStream struct {
	runner_pb.ExecutionControl_RunClient
	cancelSendEntered chan struct{}
}

func (s blockedCancelStream) Send(request *runner_pb.ExecutionControlRequest) error {
	if _, ok := request.Request.(*runner_pb.ExecutionControlRequest_Cancel); ok {
		close(s.cancelSendEntered)
		<-s.Context().Done()
		return s.Context().Err()
	}
	return s.ExecutionControl_RunClient.Send(request)
}

func TestCompletionAcknowledgementTimeoutDuringBlockedCancelSend(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClock := mock.NewMockClock(ctrl)
	expireAcknowledgement := make(chan context.CancelFunc, 1)
	mockClock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Minute).DoAndReturn(func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		expireAcknowledgement <- cancel
		return ctx, cancel
	})
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	connection, _ := controlConnection(t, functionRunner{run: func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
		close(started)
		<-release
		return &runner_pb.RunResponse{}, nil
	}})
	cancelSendEntered := make(chan struct{})
	client := runner.NewCompletionRunnerClient(nil, blockedCancelClient{runner_pb.NewExecutionControlClient(connection), cancelSendEntered}, time.Minute, mockClock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Run(ctx, &runner_pb.RunRequest{}); done <- err }()
	<-started
	cancel()
	<-cancelSendEntered
	(<-expireAcknowledgement)()
	var unknown *runner.CompletionUnknownError
	require.ErrorAs(t, <-done, &unknown)
}

func TestControlEOFAndMalformedCancelStillWaitForCleanup(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "EOF", true: "second_run"}[malformed], func(t *testing.T) {
			canceled, release := make(chan struct{}), make(chan struct{})
			connection, _ := controlConnection(t, functionRunner{run: func(ctx context.Context, _ *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
				<-ctx.Done()
				close(canceled)
				<-release
				return &runner_pb.RunResponse{}, nil
			}})
			stream, err := runner_pb.NewExecutionControlClient(connection).Run(context.Background())
			require.NoError(t, err)
			request := &runner_pb.ExecutionControlRequest{Request: &runner_pb.ExecutionControlRequest_Run{Run: &runner_pb.RunRequest{}}}
			require.NoError(t, stream.Send(request))
			if malformed {
				require.NoError(t, stream.Send(request))
			} else {
				require.NoError(t, stream.CloseSend())
			}
			<-canceled
			done := make(chan *runner_pb.ExecutionControlResponse, 1)
			go func() { response, _ := stream.Recv(); done <- response }()
			select {
			case <-done:
				t.Fatal("terminal response preceded cleanup")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			terminal := <-done
			require.True(t, terminal.CompletionConfirmed)
			if malformed {
				require.Equal(t, int32(codes.InvalidArgument), terminal.ExecutionStatus.Code)
			}
		})
	}
}

func TestCompletionStateSurvivesErrorReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses Unix executable")
	}
	local := localLifecycleRunner(t, "/bin/true", testLifecycle{configure: func(*exec.Cmd) error { return nil }, finish: func(context.Context) error { return syscall.EBUSY }})
	decorator := functionRunner{run: func(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
		_, _ = local.Run(ctx, request)
		return nil, status.Error(codes.Internal, "decorator replaced the cleanup error")
	}}
	connection, _ := controlConnection(t, decorator)
	client := runner.NewCompletionRunnerClient(nil, runner_pb.NewExecutionControlClient(connection), time.Second, clock.SystemClock)
	_, err := client.Run(context.Background(), &runner_pb.RunRequest{Arguments: []string{"command"}, StdoutPath: "stdout", StderrPath: "stderr"})
	var unknown *runner.CompletionUnknownError
	require.True(t, errors.As(err, &unknown))
}

func TestCompletionSuccessAndExecutionError(t *testing.T) {
	for _, executionError := range []error{nil, status.Error(codes.InvalidArgument, "bad command")} {
		connection, _ := controlConnection(t, functionRunner{run: func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
			return &runner_pb.RunResponse{ExitCode: 42}, executionError
		}})
		client := runner.NewCompletionRunnerClient(nil, runner_pb.NewExecutionControlClient(connection), time.Second, clock.SystemClock)
		response, err := client.Run(context.Background(), &runner_pb.RunRequest{})
		require.Equal(t, status.Code(executionError), status.Code(err))
		if err == nil {
			require.Equal(t, int64(42), response.ExitCode)
		}
	}
}

func TestCompletionAlreadyCanceledNeverStarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := runner.NewCompletionRunnerClient(nil, nil, time.Second, clock.SystemClock)
	_, err := client.Run(ctx, &runner_pb.RunRequest{})
	require.Equal(t, codes.Canceled, status.Code(err))
}

func TestCompletionRequiredRunner(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	base := mock.NewMockRunnerServer(ctrl)
	r := runner.NewCompletionRequiredRunner(base)

	request := &runner_pb.CheckReadinessRequest{Path: "ready"}
	base.EXPECT().CheckReadiness(ctx, request).Return(&emptypb.Empty{}, nil)
	_, err := r.CheckReadiness(ctx, request)
	require.NoError(t, err)

	_, err = r.Run(ctx, &runner_pb.RunRequest{Arguments: []string{"/bin/true"}})
	testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Action execution requires ExecutionControl.Run"), err)
}

func TestCompletionRequiredDoesNotFallBackToUnary(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	// Any unary Run would invoke this function and return success. It must never
	// run when the required completion service is absent.
	unaryCalled := make(chan struct{}, 1)
	runner_pb.RegisterRunnerServer(server, functionRunner{run: func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
		unaryCalled <- struct{}{}
		return &runner_pb.RunResponse{}, nil
	}})
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///old-runner", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer connection.Close()
	client := runner.NewCompletionRunnerClient(runner_pb.NewRunnerClient(connection), runner_pb.NewExecutionControlClient(connection), time.Second, clock.SystemClock)
	_, err = client.Run(context.Background(), &runner_pb.RunRequest{})
	var unknown *runner.CompletionUnknownError
	require.ErrorAs(t, err, &unknown)
	require.Equal(t, codes.Unimplemented, status.Code(unknown.Cause))
	select {
	case <-unaryCalled:
		t.Fatal("fell back to unacknowledged unary execution")
	default:
	}
}

type retryingControlServer struct {
	runner_pb.UnimplementedExecutionControlServer
	attempts atomic.Int32
}

func (s *retryingControlServer) Run(stream runner_pb.ExecutionControl_RunServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if s.attempts.Add(1) == 1 {
		// A transport or proxy can fail after delivery, without proving that
		// the execution and cleanup associated with this attempt have stopped.
		return status.Error(codes.Unavailable, "lost first attempt")
	}
	return stream.Send(&runner_pb.ExecutionControlResponse{
		Result:              &runner_pb.RunResponse{},
		ExecutionStatus:     status.New(codes.OK, "").Proto(),
		CompletionConfirmed: true,
	})
}

func TestCompletionDoesNotReplayDeliveredRun(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	control := &retryingControlServer{}
	runner_pb.RegisterExecutionControlServer(server, control)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	connection, err := grpc.NewClient("passthrough:///retrying-runner",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithDefaultServiceConfig(`{"methodConfig":[{"name":[{"service":"buildbarn.runner.ExecutionControl"}],"retryPolicy":{"maxAttempts":3,"initialBackoff":"0.001s","maxBackoff":"0.001s","backoffMultiplier":1,"retryableStatusCodes":["UNAVAILABLE"]}}]}`))
	require.NoError(t, err)
	t.Cleanup(func() { connection.Close() })
	client := runner.NewCompletionRunnerClient(nil, runner_pb.NewExecutionControlClient(connection), time.Second, clock.SystemClock)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.Run(ctx, &runner_pb.RunRequest{})
	var unknown *runner.CompletionUnknownError
	require.ErrorAs(t, err, &unknown, "a replay must not conceal an unconfirmed first attempt")
	require.Equal(t, int32(1), control.attempts.Load())
}

func TestExecutionControlTerminalStatus(t *testing.T) {
	detailed, err := status.New(codes.InvalidArgument, "Invalid command").WithDetails(&runner_pb.RunResponse{ExitCode: 42})
	require.NoError(t, err)
	unknown := &runner.CompletionUnknownError{Cause: context.Canceled}
	for name, tc := range map[string]struct {
		err       error
		status    *status.Status
		confirmed bool
	}{
		"Canceled":          {context.Canceled, status.New(codes.Canceled, "context canceled"), true},
		"DeadlineExceeded":  {context.DeadlineExceeded, status.New(codes.DeadlineExceeded, "context deadline exceeded"), true},
		"Classified":        {detailed.Err(), detailed, true},
		"UnknownCompletion": {unknown, unknown.GRPCStatus(), false},
	} {
		t.Run(name, func(t *testing.T) {
			connection, _ := controlConnection(t, functionRunner{run: func(context.Context, *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
				return nil, tc.err
			}})
			stream, err := runner_pb.NewExecutionControlClient(connection).Run(context.Background())
			require.NoError(t, err)
			require.NoError(t, stream.Send(&runner_pb.ExecutionControlRequest{Request: &runner_pb.ExecutionControlRequest_Run{Run: &runner_pb.RunRequest{}}}))
			terminal, err := stream.Recv()
			require.NoError(t, err)
			testutil.RequireEqualStatus(t, tc.status.Err(), status.ErrorProto(terminal.ExecutionStatus))
			require.Equal(t, tc.confirmed, terminal.CompletionConfirmed)
		})
	}
}
