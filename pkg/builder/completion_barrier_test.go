package builder_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-remote-execution/pkg/runner"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.uber.org/mock/gomock"
)

func TestCompletionUncertaintyRetainsDirectoriesAndQuarantinesSharedRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	directory := mock.NewMockBuildDirectory(ctrl)
	input := mock.NewMockBuildDirectory(ctrl)
	creator := mock.NewMockBuildDirectoryCreator(ctrl)
	commandReader := mock.NewMockReader[*remoteexecution.Command](ctrl)
	client := mock.NewMockRunnerClient(ctrl)
	clock := mock.NewMockClock(ctrl)
	filePool := mock.NewMockFilePool(ctrl)
	monitor := mock.NewMockUnreadDirectoryMonitor(ctrl)
	quarantine := &builder.BuildRootQuarantine{}
	newExecutor := func() builder.BuildExecutor {
		return builder.NewLocalBuildExecutorWithQuarantine(commandReader, nil, creator, client, clock, 0, nil, nil, false, quarantine)
	}
	executor, sibling := newExecutor(), newExecutor()
	creator.EXPECT().GetBuildDirectory(gomock.Any(), gomock.Any()).Return(directory, nil, nil)
	directory.EXPECT().InstallHooks(filePool, gomock.Any())
	directory.EXPECT().Mkdir(path.MustNewComponent("root"), os.FileMode(0o777))
	directory.EXPECT().EnterBuildDirectory(path.MustNewComponent("root")).Return(input, nil)
	input.EXPECT().MergeDirectoryContents(gomock.Any(), gomock.Any(), gomock.Any(), monitor)
	commandReader.EXPECT().Read(gomock.Any(), gomock.Any()).Return(&remoteexecution.Command{Arguments: []string{"command"}}, nil)
	directory.EXPECT().Mkdir(path.MustNewComponent("tmp"), os.FileMode(0o777))
	directory.EXPECT().Mkdir(path.MustNewComponent("server_logs"), os.FileMode(0o777))
	clock.EXPECT().NewContextWithTimeout(gomock.Any(), time.Second).DoAndReturn(func(ctx context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
		return context.WithCancel(ctx)
	})
	started, terminal := make(chan struct{}), make(chan struct{})
	client.EXPECT().Run(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ *runner_pb.RunRequest, _ ...grpc.CallOption) (*runner_pb.RunResponse, error) {
		close(started)
		<-terminal
		return nil, &runner.CompletionUnknownError{Cause: status.Error(codes.Unavailable, "lost acknowledgement")}
	})
	// There are deliberately no Close, UploadFile, or output traversal expectations.
	// Any such operation, before or after the unknown terminal, fails the test.
	request := &remoteworker.DesiredState_Executing{
		ActionDigest: &remoteexecution.Digest{Hash: strings.Repeat("1", 64), SizeBytes: 1},
		Action: &remoteexecution.Action{
			CommandDigest:   &remoteexecution.Digest{Hash: strings.Repeat("2", 64), SizeBytes: 1},
			InputRootDigest: &remoteexecution.Digest{Hash: strings.Repeat("3", 64), SizeBytes: 1},
			Timeout:         durationpb.New(time.Second),
		},
	}
	function := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
	done := make(chan *remoteexecution.ExecuteResponse, 1)
	updates := make(chan *remoteworker.CurrentState_Executing, 10)
	go func() { done <- executor.Execute(context.Background(), filePool, monitor, function, request, updates) }()
	select {
	case <-started:
	case response := <-done:
		t.Fatalf("Execution returned before starting the runner: %v", response.Status)
	}
	select {
	case <-done:
		t.Fatal("execution returned before terminal acknowledgement")
	case <-time.After(20 * time.Millisecond):
	}
	close(terminal)
	response := <-done
	require.Equal(t, int32(codes.Unavailable), response.Status.Code)
	require.Contains(t, response.Status.Message, "completion could not be confirmed")
	require.Equal(t, codes.Unavailable, status.Code(sibling.CheckReadiness(context.Background())))
	rejected := sibling.Execute(context.Background(), filePool, monitor, function, request, updates)
	require.Equal(t, int32(codes.Unavailable), rejected.Status.Code)
	require.Contains(t, rejected.Status.Message, "Build root quarantined")
}
