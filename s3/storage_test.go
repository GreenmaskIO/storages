package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/greenmaskio/storages"
)

// ---------------------------------------------------------------------------
// Test doubles (testify/mock)
//
// mockS3 doubles the s3API seam and mockUploader the uploaderAPI seam. Both
// embed mock.Mock: expectations are set with .On(...).Return(...) and verified
// with AssertExpectations. Captured-argument assertions go through the helper
// methods below (getKeys/headKeys/deleteBatches/listMarkers/lastPut) rather than
// reaching into mock.Calls from the test bodies.
// ---------------------------------------------------------------------------

type mockS3 struct {
	mock.Mock

	// markers captures the Marker of each ListObjects (v1) call at call time.
	// ListDir reuses and mutates a single input pointer across pages, so the
	// value must be snapshotted here rather than read back from mock.Calls.
	markers []string
}

func (m *mockS3) ListObjects(
	ctx context.Context, in *s3.ListObjectsInput, _ ...func(*s3.Options),
) (*s3.ListObjectsOutput, error) {
	m.markers = append(m.markers, aws.ToString(in.Marker))
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.ListObjectsOutput)
	return out, args.Error(1)
}

func (m *mockS3) ListObjectsV2(
	ctx context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.ListObjectsV2Output)
	return out, args.Error(1)
}

func (m *mockS3) GetObject(
	ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.GetObjectOutput)
	return out, args.Error(1)
}

func (m *mockS3) HeadObject(
	ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.HeadObjectOutput)
	return out, args.Error(1)
}

func (m *mockS3) HeadBucket(
	ctx context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options),
) (*s3.HeadBucketOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.HeadBucketOutput)
	return out, args.Error(1)
}

func (m *mockS3) DeleteObjects(
	ctx context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options),
) (*s3.DeleteObjectsOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.DeleteObjectsOutput)
	return out, args.Error(1)
}

// getKeys returns the Key of every GetObject call, in order.
func (m *mockS3) getKeys() []string {
	return m.capturedKeys("GetObject", func(a mock.Arguments) string {
		return aws.ToString(a.Get(1).(*s3.GetObjectInput).Key)
	})
}

// headKeys returns the Key of every HeadObject call, in order.
func (m *mockS3) headKeys() []string {
	return m.capturedKeys("HeadObject", func(a mock.Arguments) string {
		return aws.ToString(a.Get(1).(*s3.HeadObjectInput).Key)
	})
}

// listMarkers returns the Marker of every ListObjects (v1) call, in order.
func (m *mockS3) listMarkers() []string {
	return m.markers
}

func (m *mockS3) capturedKeys(method string, extract func(mock.Arguments) string) []string {
	var keys []string
	for _, c := range m.Calls {
		if c.Method == method {
			keys = append(keys, extract(c.Arguments))
		}
	}
	return keys
}

// deleteBatches returns the keys of each DeleteObjects call, one slice per call.
func (m *mockS3) deleteBatches() [][]string {
	var batches [][]string
	for _, c := range m.Calls {
		if c.Method != "DeleteObjects" {
			continue
		}
		in := c.Arguments.Get(1).(*s3.DeleteObjectsInput)
		keys := make([]string, 0, len(in.Delete.Objects))
		for _, o := range in.Delete.Objects {
			keys = append(keys, aws.ToString(o.Key))
		}
		batches = append(batches, keys)
	}
	return batches
}

type mockUploader struct {
	mock.Mock
}

func (m *mockUploader) Upload(
	ctx context.Context, in *s3.PutObjectInput, _ ...func(*manager.Uploader),
) (*manager.UploadOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*manager.UploadOutput)
	return out, args.Error(1)
}

// lastPut returns the input of the most recent Upload call, or nil.
func (m *mockUploader) lastPut() *s3.PutObjectInput {
	for i := len(m.Calls) - 1; i >= 0; i-- {
		if m.Calls[i].Method == "Upload" {
			return m.Calls[i].Arguments.Get(1).(*s3.PutObjectInput)
		}
	}
	return nil
}

// newStorage builds a Storage wired to the supplied doubles, bypassing
// NewStorage (which needs AWS config/network). Either double may be nil for
// tests that never reach it.
func newStorage(t *testing.T, prefix string, svc s3API, up uploaderAPI) *Storage {
	t.Helper()
	return &Storage{
		config:   Config{Bucket: "test-bucket", StorageClass: defaultStorageClass},
		service:  svc,
		uploader: up,
		prefix:   prefix,
		logger:   slog.New(slog.DiscardHandler),
	}
}

// makeFilePaths returns n distinct file names, for exercising Delete batching.
func makeFilePaths(n int) []string {
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("file_%d.dat", i)
	}
	return paths
}

// apiErr is a smithy APIError with the given code, standing in for an
// S3-compatible endpoint that reports errors by code rather than by concrete
// typed error.
func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: "boom"}
}

// ===========================================================================
// Pure logic (no doubles required)
// ===========================================================================

func Test_fixPrefix(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty stays empty", in: "", want: ""},
		{name: "adds trailing slash", in: "foo", want: "foo/"},
		{name: "idempotent when already suffixed", in: "foo/", want: "foo/"},
		{name: "nested path", in: "a/b", want: "a/b/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			got := fixPrefix(tt.in)

			// Assert
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_isNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "typed NoSuchKey", err: &types.NoSuchKey{}, want: true},
		{name: "typed NotFound", err: &types.NotFound{}, want: true},
		{name: "wrapped typed NotFound", err: fmt.Errorf("wrap: %w", &types.NotFound{}), want: true},
		{name: "apierr NotFound code", err: apiErr(NotFountAwsErrorCode), want: true},
		{name: "apierr NoSuchKey code", err: apiErr(NoSuchKeyAwsErrorCode), want: true},
		{name: "apierr unrelated code", err: apiErr("AccessDenied"), want: false},
		{name: "plain error", err: errors.New("nope"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			got := isNotFound(tt.err)

			// Assert
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStorage_GetCwd(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "root", prefix: "", want: ""},
		{name: "single dir", prefix: "dumps/", want: "dumps/"},
		{name: "nested", prefix: "a/b/", want: "a/b/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			st := newStorage(t, tt.prefix, nil, nil)

			// Act & Assert
			assert.Equal(t, tt.want, st.GetCwd())
		})
	}
}

func TestStorage_Dirname(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "empty prefix yields dot", prefix: "", want: "."},
		{name: "single dir strips slash", prefix: "dumps/", want: "dumps"},
		{name: "nested returns base", prefix: "a/b/", want: "b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			st := newStorage(t, tt.prefix, nil, nil)

			// Act & Assert
			assert.Equal(t, tt.want, st.Dirname())
		})
	}
}

func TestStorage_SubStorage(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		subPath  string
		relative bool
		wantCwd  string
	}{
		{name: "relative joins and normalizes", prefix: "dumps/", subPath: "sub", relative: true, wantCwd: "dumps/sub/"},
		{name: "relative from root", prefix: "", subPath: "sub", relative: true, wantCwd: "sub/"},
		{name: "relative nested", prefix: "a/", subPath: "b/c", relative: true, wantCwd: "a/b/c/"},
		{name: "absolute used verbatim", prefix: "dumps/", subPath: "other/", relative: false, wantCwd: "other/"},
		{name: "absolute not normalized", prefix: "dumps/", subPath: "other", relative: false, wantCwd: "other"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			up := &mockUploader{}
			st := newStorage(t, tt.prefix, svc, up)

			// Act
			sub := st.SubStorage(tt.subPath, tt.relative)

			// Assert
			assert.Equal(t, tt.wantCwd, sub.GetCwd())
			subImpl, ok := sub.(*Storage)
			require.True(t, ok)
			assert.Same(t, st.service, subImpl.service, "sub-storage must share the parent client")
			assert.Same(t, st.config, subImpl.config, "sub-storage must share the parent config")
		})
	}
}

// ===========================================================================
// Object I/O logic (mocked seam)
// ===========================================================================

func TestStorage_PutObject(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		filePath  string
		uploadErr error
		wantKey   string
		wantErr   string
	}{
		{name: "joins prefix", prefix: "dumps/", filePath: "a.txt", wantKey: "dumps/a.txt"},
		{name: "empty prefix", prefix: "", filePath: "a.txt", wantKey: "a.txt"},
		{name: "nested file path", prefix: "dumps/", filePath: "x/y.txt", wantKey: "dumps/x/y.txt"},
		{name: "upload error wrapped", prefix: "dumps/", filePath: "a.txt", uploadErr: errors.New("boom"), wantKey: "dumps/a.txt", wantErr: "s3 object uploading error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			up := &mockUploader{}
			up.On("Upload", mock.Anything, mock.Anything).Return(&manager.UploadOutput{}, tt.uploadErr)
			st := newStorage(t, tt.prefix, nil, up)

			// Act
			err := st.PutObject(context.Background(), tt.filePath, bytes.NewReader([]byte("data")))

			// Assert
			put := up.lastPut()
			require.NotNil(t, put)
			assert.Equal(t, tt.wantKey, aws.ToString(put.Key))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "test-bucket", aws.ToString(put.Bucket))
			assert.Equal(t, types.StorageClass(defaultStorageClass), put.StorageClass)
			up.AssertExpectations(t)
		})
	}
}

func TestStorage_GetObject(t *testing.T) {
	tests := []struct {
		name        string
		prefix      string
		filePath    string
		getErr      error
		body        string
		wantKey     string
		wantBody    string
		wantErrIs   error
		wantErrText string
	}{
		{name: "success returns body", prefix: "dumps/", filePath: "a.txt", body: "hello", wantKey: "dumps/a.txt", wantBody: "hello"},
		{name: "empty prefix", prefix: "", filePath: "a.txt", body: "hi", wantKey: "a.txt", wantBody: "hi"},
		{name: "NoSuchKey maps to ErrFileNotFound", prefix: "dumps/", filePath: "a.txt", getErr: &types.NoSuchKey{}, wantKey: "dumps/a.txt", wantErrIs: storages.ErrFileNotFound},
		{name: "NotFound maps to ErrFileNotFound", prefix: "dumps/", filePath: "a.txt", getErr: &types.NotFound{}, wantKey: "dumps/a.txt", wantErrIs: storages.ErrFileNotFound},
		{name: "apierr code maps to ErrFileNotFound", prefix: "dumps/", filePath: "a.txt", getErr: apiErr(NoSuchKeyAwsErrorCode), wantKey: "dumps/a.txt", wantErrIs: storages.ErrFileNotFound},
		{name: "other error wrapped", prefix: "dumps/", filePath: "a.txt", getErr: errors.New("network down"), wantKey: "dumps/a.txt", wantErrText: "error getting object"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			if tt.getErr != nil {
				svc.On("GetObject", mock.Anything, mock.Anything).Return(nil, tt.getErr)
			} else {
				svc.On("GetObject", mock.Anything, mock.Anything).
					Return(&s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(tt.body))}, nil)
			}
			st := newStorage(t, tt.prefix, svc, nil)

			// Act
			reader, err := st.GetObject(context.Background(), tt.filePath)

			// Assert
			require.Equal(t, []string{tt.wantKey}, svc.getKeys())
			switch {
			case tt.wantErrIs != nil:
				assert.ErrorIs(t, err, tt.wantErrIs)
				assert.Nil(t, reader)
			case tt.wantErrText != "":
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
				assert.Nil(t, reader)
			default:
				require.NoError(t, err)
				defer reader.Close()
				got, err := io.ReadAll(reader)
				require.NoError(t, err)
				assert.Equal(t, tt.wantBody, string(got))
			}
			svc.AssertExpectations(t)
		})
	}
}

func TestStorage_Exists(t *testing.T) {
	tests := []struct {
		name        string
		prefix      string
		fileName    string
		headErr     error
		wantKey     string
		wantExists  bool
		wantErrText string
	}{
		{name: "present", prefix: "dumps/", fileName: "a.txt", wantKey: "dumps/a.txt", wantExists: true},
		{name: "NoSuchKey means absent", prefix: "dumps/", fileName: "a.txt", headErr: &types.NoSuchKey{}, wantKey: "dumps/a.txt", wantExists: false},
		{name: "NotFound means absent", prefix: "dumps/", fileName: "a.txt", headErr: &types.NotFound{}, wantKey: "dumps/a.txt", wantExists: false},
		{name: "apierr code means absent", prefix: "dumps/", fileName: "a.txt", headErr: apiErr(NotFountAwsErrorCode), wantKey: "dumps/a.txt", wantExists: false},
		{name: "other error wrapped", prefix: "dumps/", fileName: "a.txt", headErr: errors.New("boom"), wantKey: "dumps/a.txt", wantErrText: "error getting object info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			if tt.headErr != nil {
				svc.On("HeadObject", mock.Anything, mock.Anything).Return(nil, tt.headErr)
			} else {
				svc.On("HeadObject", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{}, nil)
			}
			st := newStorage(t, tt.prefix, svc, nil)

			// Act
			exists, err := st.Exists(context.Background(), tt.fileName)

			// Assert
			require.Equal(t, []string{tt.wantKey}, svc.headKeys())
			if tt.wantErrText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
				assert.False(t, exists)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantExists, exists)
			svc.AssertExpectations(t)
		})
	}
}

func TestStorage_Stat(t *testing.T) {
	modTime := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		prefix      string
		fileName    string
		headErr     error
		wantName    string
		wantErrText string
	}{
		{name: "present builds full path name", prefix: "dumps/", fileName: "a.txt", wantName: "dumps/a.txt"},
		{name: "name keeps prefix and is not stripped", prefix: "a/b/", fileName: "c.txt", wantName: "a/b/c.txt"},
		{name: "not found is an error not Exist=false", prefix: "dumps/", fileName: "a.txt", headErr: &types.NotFound{}, wantName: "dumps/a.txt", wantErrText: "error getting object info"},
		{name: "other error wrapped", prefix: "dumps/", fileName: "a.txt", headErr: errors.New("boom"), wantName: "dumps/a.txt", wantErrText: "error getting object info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			if tt.headErr != nil {
				svc.On("HeadObject", mock.Anything, mock.Anything).Return(nil, tt.headErr)
			} else {
				svc.On("HeadObject", mock.Anything, mock.Anything).
					Return(&s3.HeadObjectOutput{LastModified: aws.Time(modTime)}, nil)
			}
			st := newStorage(t, tt.prefix, svc, nil)

			// Act
			stat, err := st.Stat(tt.fileName)

			// Assert
			require.Equal(t, []string{tt.wantName}, svc.headKeys())
			if tt.wantErrText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
				assert.Nil(t, stat)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, stat)
			assert.Equal(t, tt.wantName, stat.Name)
			assert.True(t, stat.Exist)
			assert.True(t, stat.LastModified.Equal(modTime), "LastModified = %v", stat.LastModified)
			svc.AssertExpectations(t)
		})
	}
}

func TestStorage_Delete(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		paths  []string
		verify func(t *testing.T, batches [][]string)
	}{
		{
			name:   "empty list makes no calls",
			prefix: "dumps/",
			paths:  nil,
			verify: func(t *testing.T, batches [][]string) {
				assert.Empty(t, batches)
			},
		},
		{
			name:   "single sub-batch is one call",
			prefix: "dumps/",
			paths:  makeFilePaths(500),
			verify: func(t *testing.T, batches [][]string) {
				require.Len(t, batches, 1)
				assert.Len(t, batches[0], 500)
			},
		},
		{
			name:   "exactly one full batch",
			prefix: "dumps/",
			paths:  makeFilePaths(deleteObjectsBatchSize),
			verify: func(t *testing.T, batches [][]string) {
				require.Len(t, batches, 1)
				assert.Len(t, batches[0], deleteObjectsBatchSize)
			},
		},
		{
			name:   "splits into batches of 1000",
			prefix: "dumps/",
			paths:  makeFilePaths(2500),
			verify: func(t *testing.T, batches [][]string) {
				require.Len(t, batches, 3)
				assert.Len(t, batches[0], 1000)
				assert.Len(t, batches[1], 1000)
				assert.Len(t, batches[2], 500)
			},
		},
		{
			name:   "prefix applied to every key",
			prefix: "dumps/",
			paths:  []string{"a.txt", "b.txt"},
			verify: func(t *testing.T, batches [][]string) {
				require.Len(t, batches, 1)
				assert.Equal(t, []string{"dumps/a.txt", "dumps/b.txt"}, batches[0])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			svc.On("DeleteObjects", mock.Anything, mock.Anything).Return(&s3.DeleteObjectsOutput{}, nil).Maybe()
			st := newStorage(t, tt.prefix, svc, nil)

			// Act
			err := st.Delete(context.Background(), tt.paths...)

			// Assert
			require.NoError(t, err)
			tt.verify(t, svc.deleteBatches())
		})
	}
}

func TestStorage_Delete_ErrorStopsBatching(t *testing.T) {
	// Arrange: first batch succeeds, the second fails, so batching must stop.
	svc := &mockS3{}
	svc.On("DeleteObjects", mock.Anything, mock.Anything).Return(&s3.DeleteObjectsOutput{}, nil).Once()
	svc.On("DeleteObjects", mock.Anything, mock.Anything).Return(nil, errors.New("s3 failure")).Once()
	st := newStorage(t, "dumps/", svc, nil)

	// Act
	err := st.Delete(context.Background(), makeFilePaths(2500)...)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error deleting objects")
	assert.Contains(t, err.Error(), "s3 failure")
	assert.Len(t, svc.deleteBatches(), 2, "batching should stop after the failing call")
	svc.AssertExpectations(t)
}

func TestStorage_Ping(t *testing.T) {
	tests := []struct {
		name    string
		headErr error
		wantErr string
	}{
		{name: "reachable", headErr: nil},
		{name: "error wrapped", headErr: errors.New("no route"), wantErr: "error pinging s3 bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			if tt.headErr != nil {
				svc.On("HeadBucket", mock.Anything, mock.Anything).Return(nil, tt.headErr)
			} else {
				svc.On("HeadBucket", mock.Anything, mock.Anything).Return(&s3.HeadBucketOutput{}, nil)
			}
			st := newStorage(t, "dumps/", svc, nil)

			// Act
			err := st.Ping(context.Background())

			// Assert
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			assert.NoError(t, err)
			svc.AssertExpectations(t)
		})
	}
}

func TestStorage_ListDir(t *testing.T) {
	noToken := mock.MatchedBy(func(in *s3.ListObjectsV2Input) bool { return in.ContinuationToken == nil })
	withToken := mock.MatchedBy(func(in *s3.ListObjectsV2Input) bool { return in.ContinuationToken != nil })
	noMarker := mock.MatchedBy(func(in *s3.ListObjectsInput) bool { return in.Marker == nil })
	withMarker := mock.MatchedBy(func(in *s3.ListObjectsInput) bool { return in.Marker != nil })

	t.Run("v2 paginates, splits dirs from files, strips prefix", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjectsV2", mock.Anything, noToken).Return(&s3.ListObjectsV2Output{
			CommonPrefixes:        []types.CommonPrefix{{Prefix: aws.String("p/dirA/")}},
			Contents:              []types.Object{{Key: aws.String("p/file1.txt")}},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("token-1"),
		}, nil)
		svc.On("ListObjectsV2", mock.Anything, withToken).Return(&s3.ListObjectsV2Output{
			CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("p/dirB/")}},
			Contents:       []types.Object{{Key: aws.String("p/file2.txt")}},
			IsTruncated:    aws.Bool(false),
		}, nil)
		st := newStorage(t, "p/", svc, nil)

		// Act
		files, dirs, err := st.ListDir(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, []string{"file1.txt", "file2.txt"}, files)
		require.Len(t, dirs, 2)
		assert.Equal(t, "p/dirA/", dirs[0].GetCwd())
		assert.Equal(t, "p/dirB/", dirs[1].GetCwd())
	})

	t.Run("v1 marker falls back to last key when NextMarker empty", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjects", mock.Anything, noMarker).Return(&s3.ListObjectsOutput{
			Contents:    []types.Object{{Key: aws.String("p/a")}, {Key: aws.String("p/b")}},
			IsTruncated: aws.Bool(true),
			// NextMarker deliberately empty -> fall back to last key "p/b".
		}, nil)
		svc.On("ListObjects", mock.Anything, withMarker).Return(&s3.ListObjectsOutput{
			Contents:    []types.Object{{Key: aws.String("p/c")}},
			IsTruncated: aws.Bool(false),
		}, nil)
		st := newStorage(t, "p/", svc, nil)
		st.config.UseListObjectsV1 = true

		// Act
		files, _, err := st.ListDir(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, files)
		assert.Equal(t, []string{"", "p/b"}, svc.listMarkers(), "second page must page from the last key")
	})

	t.Run("v1 uses NextMarker when present", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjects", mock.Anything, noMarker).Return(&s3.ListObjectsOutput{
			Contents:    []types.Object{{Key: aws.String("p/a")}},
			IsTruncated: aws.Bool(true),
			NextMarker:  aws.String("explicit-marker"),
		}, nil)
		svc.On("ListObjects", mock.Anything, withMarker).Return(&s3.ListObjectsOutput{
			Contents:    []types.Object{{Key: aws.String("p/b")}},
			IsTruncated: aws.Bool(false),
		}, nil)
		st := newStorage(t, "p/", svc, nil)
		st.config.UseListObjectsV1 = true

		// Act
		files, _, err := st.ListDir(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, files)
		assert.Equal(t, []string{"", "explicit-marker"}, svc.listMarkers())
	})

	t.Run("v1 breaks when truncated page has no marker and no contents", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjects", mock.Anything, mock.Anything).Return(&s3.ListObjectsOutput{
			IsTruncated: aws.Bool(true),
		}, nil)
		st := newStorage(t, "p/", svc, nil)
		st.config.UseListObjectsV1 = true

		// Act
		files, dirs, err := st.ListDir(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Empty(t, files)
		assert.Empty(t, dirs)
		assert.Len(t, svc.listMarkers(), 1, "must not loop forever when no marker can be derived")
	})

	t.Run("v2 error is wrapped", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjectsV2", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))
		st := newStorage(t, "p/", svc, nil)

		// Act
		_, _, err := st.ListDir(context.Background())

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error listing s3 objects v2")
	})
}

func TestStorage_DeleteAll(t *testing.T) {
	// Arrange: a two-level tree under "sub/". ListObjectsV2 is matched per-prefix
	// so Walk recurses; DeleteObjects records the keys it is asked to remove.
	atPrefix := func(prefix string) any {
		return mock.MatchedBy(func(in *s3.ListObjectsV2Input) bool { return aws.ToString(in.Prefix) == prefix })
	}
	svc := &mockS3{}
	svc.On("ListObjectsV2", mock.Anything, atPrefix("sub/")).Return(&s3.ListObjectsV2Output{
		Contents:       []types.Object{{Key: aws.String("sub/f1.txt")}, {Key: aws.String("sub/f2.txt")}},
		CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("sub/nested/")}},
	}, nil)
	svc.On("ListObjectsV2", mock.Anything, atPrefix("sub/nested/")).Return(&s3.ListObjectsV2Output{
		Contents: []types.Object{{Key: aws.String("sub/nested/g.txt")}},
	}, nil)
	svc.On("DeleteObjects", mock.Anything, mock.Anything).Return(&s3.DeleteObjectsOutput{}, nil)
	st := newStorage(t, "", svc, nil)

	// Act
	err := st.DeleteAll(context.Background(), "sub")

	// Assert
	require.NoError(t, err)
	batches := svc.deleteBatches()
	require.Len(t, batches, 1)
	assert.Equal(t,
		[]string{"sub/f1.txt", "sub/f2.txt", "sub/nested/g.txt"},
		batches[0],
		"every walked key must be deleted with the sub-storage prefix re-applied",
	)
}

// ===========================================================================
// Integration (real MinIO container, behind -short)
// ===========================================================================

var (
	minioOnce      sync.Once
	minioStorage   *Storage
	minioContainer *minio.MinioContainer
	minioErr       error
)

// TestMain terminates the shared MinIO container (if one was started) after the
// whole package's tests have run.
func TestMain(m *testing.M) {
	code := m.Run()
	if minioContainer != nil {
		_ = minioContainer.Terminate(context.Background())
	}
	os.Exit(code)
}

// requireMinio lazily starts a single MinIO container shared by all integration
// tests and returns a Storage rooted at its bucket. Container tests are skipped
// under -short.
func requireMinio(t *testing.T) *Storage {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping MinIO container test in short mode")
	}
	minioOnce.Do(func() {
		minioStorage, minioContainer, minioErr = startMinio(context.Background())
	})
	require.NoError(t, minioErr)
	return minioStorage
}

func startMinio(ctx context.Context) (*Storage, *minio.MinioContainer, error) {
	container, err := minio.Run(ctx, "minio/minio:latest")
	if err != nil {
		return nil, nil, fmt.Errorf("starting minio: %w", err)
	}

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, container, fmt.Errorf("minio endpoint: %w", err)
	}

	const bucket = "test-bucket"
	cfg := Config{
		Bucket:          bucket,
		Region:          "us-east-1",
		Endpoint:        "http://" + endpoint,
		AccessKeyId:     container.Username,
		SecretAccessKey: container.Password,
		ForcePathStyle:  true,
		NoVerifySsl:     true,
		StorageClass:    defaultStorageClass,
		MaxPartSize:     defaultMaxPartSize,
		MaxRetries:      defaultMaxRetries,
	}
	st, err := NewStorage(ctx, cfg, WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		return nil, container, fmt.Errorf("new storage: %w", err)
	}

	// The bucket has to exist before any object operations. The s3API seam does
	// not expose CreateBucket, so reach the concrete client behind it.
	client, ok := st.service.(*s3.Client)
	if !ok {
		return nil, container, fmt.Errorf("expected *s3.Client, got %T", st.service)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return nil, container, fmt.Errorf("create bucket: %w", err)
	}
	return st, container, nil
}

func TestStorage_Integration(t *testing.T) {
	ctx := context.Background()
	root := requireMinio(t)

	t.Run("PutObject and GetObject round-trip", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-roundtrip", true)
		content := []byte("hello world")

		// Act
		require.NoError(t, st.PutObject(ctx, "file.txt", bytes.NewReader(content)))
		reader, err := st.GetObject(ctx, "file.txt")
		require.NoError(t, err)
		defer reader.Close()
		got, err := io.ReadAll(reader)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, content, got)
	})

	t.Run("GetObject on missing key returns ErrFileNotFound", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-missing", true)

		// Act
		_, err := st.GetObject(ctx, "nope.txt")

		// Assert
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})

	t.Run("Exists and Stat reflect real objects", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-stat", true)
		require.NoError(t, st.PutObject(ctx, "stat.txt", bytes.NewReader([]byte("body"))))

		// Act
		exists, err := st.Exists(ctx, "stat.txt")
		require.NoError(t, err)
		stat, statErr := st.Stat("stat.txt")
		missing, missErr := st.Exists(ctx, "absent.txt")

		// Assert
		assert.True(t, exists)
		require.NoError(t, statErr)
		assert.True(t, stat.Exist)
		assert.Contains(t, stat.Name, "stat.txt")
		require.NoError(t, missErr)
		assert.False(t, missing)
	})

	t.Run("ListDir separates files and sub-directories", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-list", true)
		require.NoError(t, st.PutObject(ctx, "root1.txt", bytes.NewReader([]byte("1"))))
		require.NoError(t, st.PutObject(ctx, "root2.txt", bytes.NewReader([]byte("2"))))
		require.NoError(t, st.PutObject(ctx, "d1/inner1.txt", bytes.NewReader([]byte("3"))))
		require.NoError(t, st.PutObject(ctx, "d1/inner2.txt", bytes.NewReader([]byte("4"))))

		// Act
		files, dirs, err := st.ListDir(ctx)

		// Assert
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"root1.txt", "root2.txt"}, files)
		require.Len(t, dirs, 1)

		subFiles, subDirs, err := dirs[0].ListDir(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"inner1.txt", "inner2.txt"}, subFiles)
		assert.Empty(t, subDirs)
	})

	t.Run("Delete removes a single object", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-delete", true)
		require.NoError(t, st.PutObject(ctx, "bye.txt", bytes.NewReader([]byte("bye"))))

		// Act
		require.NoError(t, st.Delete(ctx, "bye.txt"))

		// Assert
		exists, err := st.Exists(ctx, "bye.txt")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("DeleteAll recursively clears a sub-tree", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-deleteall", true)
		require.NoError(t, st.PutObject(ctx, "victims/a.txt", bytes.NewReader([]byte("a"))))
		require.NoError(t, st.PutObject(ctx, "victims/nested/b.txt", bytes.NewReader([]byte("b"))))

		// Act
		require.NoError(t, st.DeleteAll(ctx, "victims"))

		// Assert
		files, err := storages.Walk(ctx, st.SubStorage("victims", true), "")
		require.NoError(t, err)
		assert.Empty(t, files)
	})

	t.Run("Ping reaches the bucket", func(t *testing.T) {
		// Act & Assert
		assert.NoError(t, root.Ping(ctx))
	})
}
