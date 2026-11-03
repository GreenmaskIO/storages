// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The end-to-end tests against a real MinIO server live in the tests/integration
// module, which keeps testcontainers out of this module's dependency graph.
// What stays here drives the s3API/uploaderAPI seams with test doubles.

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

	// partCopyHook, when set, runs inside every UploadPartCopy call.
	partCopyHook func()

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

func (m *mockS3) GetObjectTagging(
	ctx context.Context, in *s3.GetObjectTaggingInput, _ ...func(*s3.Options),
) (*s3.GetObjectTaggingOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.GetObjectTaggingOutput)
	return out, args.Error(1)
}

func (m *mockS3) CopyObject(
	ctx context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options),
) (*s3.CopyObjectOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.CopyObjectOutput)
	return out, args.Error(1)
}

func (m *mockS3) CreateMultipartUpload(
	ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options),
) (*s3.CreateMultipartUploadOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.CreateMultipartUploadOutput)
	return out, args.Error(1)
}

// UploadPartCopy derives a part's ETag from its number when no output is canned,
// so part order can be checked.
func (m *mockS3) UploadPartCopy(
	ctx context.Context, in *s3.UploadPartCopyInput, _ ...func(*s3.Options),
) (*s3.UploadPartCopyOutput, error) {
	if m.partCopyHook != nil {
		m.partCopyHook()
	}
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.UploadPartCopyOutput)
	if out == nil && args.Error(1) == nil {
		out = &s3.UploadPartCopyOutput{CopyPartResult: &types.CopyPartResult{
			ETag: aws.String(fmt.Sprintf("etag-%d", aws.ToInt32(in.PartNumber))),
		}}
	}
	return out, args.Error(1)
}

func (m *mockS3) CompleteMultipartUpload(
	ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options),
) (*s3.CompleteMultipartUploadOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.CompleteMultipartUploadOutput)
	return out, args.Error(1)
}

func (m *mockS3) AbortMultipartUpload(
	ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options),
) (*s3.AbortMultipartUploadOutput, error) {
	args := m.Called(ctx, in)
	out, _ := args.Get(0).(*s3.AbortMultipartUploadOutput)
	return out, args.Error(1)
}

// partRanges returns the CopySourceRange of every UploadPartCopy call, keyed by
// part number.
func (m *mockS3) partRanges() map[int32]string {
	ranges := map[int32]string{}
	for _, c := range m.Calls {
		if c.Method == "UploadPartCopy" {
			in := c.Arguments.Get(1).(*s3.UploadPartCopyInput)
			ranges[aws.ToInt32(in.PartNumber)] = aws.ToString(in.CopySourceRange)
		}
	}
	return ranges
}

// lastCall returns the input argument of the most recent call to method, or nil.
func (m *mockS3) lastCall(method string) any {
	for i := len(m.Calls) - 1; i >= 0; i-- {
		if m.Calls[i].Method == method {
			return m.Calls[i].Arguments.Get(1)
		}
	}
	return nil
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
// New (which needs AWS config/network). Either double may be nil for
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

// statusErr builds the transport-level error the SDK surfaces for a response
// whose body carried no recognizable API error code — the shape an
// S3-compatible endpoint can produce for a 416.
func statusErr(status int) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      errors.New("boom"),
		},
	}
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

func Test_isInvalidRange(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "apierr InvalidRange code", err: apiErr(InvalidRangeAwsErrorCode), want: true},
		{name: "wrapped apierr InvalidRange code", err: fmt.Errorf("wrap: %w", apiErr(InvalidRangeAwsErrorCode)), want: true},
		{name: "apierr unrelated code", err: apiErr("AccessDenied"), want: false},
		{name: "416 without a recognizable code", err: statusErr(http.StatusRequestedRangeNotSatisfiable), want: true},
		{name: "other status", err: statusErr(http.StatusForbidden), want: false},
		{name: "plain error", err: errors.New("nope"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			got := isInvalidRange(tt.err)

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
			sub, err := st.SubStorage(tt.subPath, tt.relative)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, tt.wantCwd, sub.GetCwd())
			subImpl, ok := sub.(*Storage)
			require.True(t, ok)
			assert.Same(t, st.service, subImpl.service, "sub-storage must share the parent client")
			// Config is a value on Storage, so the sub-storage carries a copy —
			// compare by equality, not identity.
			assert.Equal(t, st.config, subImpl.config, "sub-storage must inherit the parent config")
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

func TestStorage_PutObject_Encryption(t *testing.T) {
	tests := []struct {
		name          string
		sse           string
		kmsKeyARN     string
		bucketKey     bool
		wantSSE       types.ServerSideEncryption
		wantKMSKeyID  *string
		wantBucketKey *bool
	}{
		{name: "none by default"},
		{name: "sse-s3", sse: "AES256", wantSSE: types.ServerSideEncryptionAes256},
		{name: "sse-kms with key", sse: "aws:kms", kmsKeyARN: testKMSKeyARN,
			wantSSE: types.ServerSideEncryptionAwsKms, wantKMSKeyID: aws.String(testKMSKeyARN)},
		{name: "dsse-kms with key", sse: "aws:kms:dsse", kmsKeyARN: testKMSKeyARN,
			wantSSE: types.ServerSideEncryptionAwsKmsDsse, wantKMSKeyID: aws.String(testKMSKeyARN)},
		{name: "sse-kms without key uses the aws managed key", sse: "aws:kms",
			wantSSE: types.ServerSideEncryptionAwsKms},
		{name: "sse-kms with bucket key", sse: "aws:kms", bucketKey: true,
			wantSSE: types.ServerSideEncryptionAwsKms, wantBucketKey: aws.Bool(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			up := &mockUploader{}
			up.On("Upload", mock.Anything, mock.Anything).Return(&manager.UploadOutput{}, nil)
			st := newStorage(t, "dumps/", nil, up)
			st.config.SSE = tt.sse
			st.config.KMSKeyARN = tt.kmsKeyARN
			st.config.BucketKeyEnabled = tt.bucketKey

			// Act
			err := st.PutObject(context.Background(), "a.txt", bytes.NewReader([]byte("data")))

			// Assert
			require.NoError(t, err)
			put := up.lastPut()
			require.NotNil(t, put)
			assert.Equal(t, tt.wantSSE, put.ServerSideEncryption)
			assert.Equal(t, tt.wantKMSKeyID, put.SSEKMSKeyId)
			assert.Equal(t, tt.wantBucketKey, put.BucketKeyEnabled)
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

func TestStorage_GetObjectRange(t *testing.T) {
	// ranges returns the Range header of every GetObject call, in order.
	ranges := func(m *mockS3) []string {
		return m.capturedKeys("GetObject", func(a mock.Arguments) string {
			return aws.ToString(a.Get(1).(*s3.GetObjectInput).Range)
		})
	}

	tests := []struct {
		name        string
		offset      int64
		length      int64
		getErr      error
		wantRange   string
		wantErrIs   error
		wantErrText string
	}{
		// HTTP ranges are inclusive on both ends, so a 4-byte range from 5 ends at 8.
		{name: "bounded range is inclusive", offset: 5, length: 4, wantRange: "bytes=5-8"},
		{name: "single byte", offset: 0, length: 1, wantRange: "bytes=0-0"},
		{name: "negative length is open ended", offset: 15, length: -1, wantRange: "bytes=15-"},
		// An end past MaxInt64 would wrap; no object can reach it, so ask for the rest.
		{name: "overflowing end degrades to open ended", offset: 10, length: math.MaxInt64, wantRange: "bytes=10-"},
		{name: "InvalidRange code maps to ErrInvalidRange", offset: 0, length: 4, getErr: apiErr(InvalidRangeAwsErrorCode), wantRange: "bytes=0-3", wantErrIs: storages.ErrInvalidRange},
		{name: "416 status maps to ErrInvalidRange", offset: 0, length: 4, getErr: statusErr(http.StatusRequestedRangeNotSatisfiable), wantRange: "bytes=0-3", wantErrIs: storages.ErrInvalidRange},
		{name: "NoSuchKey maps to ErrFileNotFound", offset: 0, length: 4, getErr: &types.NoSuchKey{}, wantRange: "bytes=0-3", wantErrIs: storages.ErrFileNotFound},
		{name: "other error wrapped", offset: 0, length: 4, getErr: errors.New("network down"), wantRange: "bytes=0-3", wantErrText: "error getting object range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			if tt.getErr != nil {
				svc.On("GetObject", mock.Anything, mock.Anything).Return(nil, tt.getErr)
			} else {
				svc.On("GetObject", mock.Anything, mock.Anything).
					Return(&s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("chunk"))}, nil)
			}
			st := newStorage(t, "dumps/", svc, nil)

			// Act
			reader, err := st.GetObjectRange(context.Background(), "a.txt", tt.offset, tt.length)

			// Assert
			require.Equal(t, []string{"dumps/a.txt"}, svc.getKeys())
			require.Equal(t, []string{tt.wantRange}, ranges(svc), "the server must be asked for exactly the requested bytes")
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
				assert.Equal(t, "chunk", string(got))
			}
			svc.AssertExpectations(t)
		})
	}
}

// A range that cannot yield anything is a caller bug the backend rejects on the
// spot; no request is worth sending.
func TestStorage_GetObjectRange_RejectsImpossibleRangeWithoutCallingS3(t *testing.T) {
	tests := []struct {
		name           string
		offset, length int64
	}{
		{name: "zero length", offset: 0, length: 0},
		{name: "negative offset", offset: -1, length: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			st := newStorage(t, "dumps/", svc, nil)

			// Act
			reader, err := st.GetObjectRange(context.Background(), "a.txt", tt.offset, tt.length)

			// Assert
			assert.ErrorIs(t, err, storages.ErrInvalidRange)
			assert.Nil(t, reader)
			assert.Empty(t, svc.Calls, "an impossible range must not reach S3")
		})
	}
}

func TestStorage_List(t *testing.T) {
	modTime := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	noToken := mock.MatchedBy(func(in *s3.ListObjectsV2Input) bool { return in.ContinuationToken == nil })
	withToken := mock.MatchedBy(func(in *s3.ListObjectsV2Input) bool { return in.ContinuationToken != nil })
	// A flat listing must not pass a delimiter: the request covers the whole
	// sub-tree, not one directory level.
	flatAtPrefix := func(prefix string) any {
		return mock.MatchedBy(func(in *s3.ListObjectsV2Input) bool {
			return aws.ToString(in.Prefix) == prefix && in.Delimiter == nil
		})
	}

	t.Run("v2 paginates, strips prefix, carries sizes", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjectsV2", mock.Anything, noToken).Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("p/a.txt"), Size: aws.Int64(1), LastModified: aws.Time(modTime), ETag: aws.String(`"e1"`)},
			},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("token-1"),
		}, nil)
		svc.On("ListObjectsV2", mock.Anything, withToken).Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("p/dir/b.txt"), Size: aws.Int64(22), LastModified: aws.Time(modTime)},
			},
			IsTruncated: aws.Bool(false),
		}, nil)
		st := newStorage(t, "p/", svc, nil)

		// Act
		objects, err := st.List(context.Background(), "")

		// Assert
		require.NoError(t, err)
		require.Len(t, objects, 2)
		assert.Equal(t, "a.txt", objects[0].Name)
		assert.Equal(t, int64(1), objects[0].Size)
		assert.True(t, objects[0].Exist)
		assert.True(t, objects[0].LastModified.Equal(modTime))
		assert.Equal(t, `"e1"`, objects[0].ETag)
		assert.Equal(t, "dir/b.txt", objects[1].Name)
		assert.Equal(t, int64(22), objects[1].Size)
	})

	// The trailing slash is what stops "data" from also matching "database".
	t.Run("v2 prefix is directory-like", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjectsV2", mock.Anything, flatAtPrefix("p/data/")).Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{{Key: aws.String("p/data/one.txt"), Size: aws.Int64(1)}},
		}, nil)
		st := newStorage(t, "p/", svc, nil)

		// Act
		objects, err := st.List(context.Background(), "data")

		// Assert
		require.NoError(t, err)
		require.Len(t, objects, 1)
		assert.Equal(t, "one.txt", objects[0].Name)
		svc.AssertExpectations(t)
	})

	t.Run("empty prefix on a root storage lists the whole bucket", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjectsV2", mock.Anything, flatAtPrefix("")).Return(&s3.ListObjectsV2Output{}, nil)
		st := newStorage(t, "", svc, nil)

		// Act
		objects, err := st.List(context.Background(), "")

		// Assert
		require.NoError(t, err)
		assert.Empty(t, objects)
		svc.AssertExpectations(t)
	})

	t.Run("v1 pages by marker", func(t *testing.T) {
		// Arrange
		noMarker := mock.MatchedBy(func(in *s3.ListObjectsInput) bool { return in.Marker == nil })
		withMarker := mock.MatchedBy(func(in *s3.ListObjectsInput) bool { return in.Marker != nil })
		svc := &mockS3{}
		svc.On("ListObjects", mock.Anything, noMarker).Return(&s3.ListObjectsOutput{
			Contents:    []types.Object{{Key: aws.String("p/a.txt"), Size: aws.Int64(1)}},
			IsTruncated: aws.Bool(true),
			// Without a delimiter S3 sets no NextMarker -> page from the last key.
		}, nil)
		svc.On("ListObjects", mock.Anything, withMarker).Return(&s3.ListObjectsOutput{
			Contents:    []types.Object{{Key: aws.String("p/b.txt"), Size: aws.Int64(2)}},
			IsTruncated: aws.Bool(false),
		}, nil)
		st := newStorage(t, "p/", svc, nil)
		st.config.UseListObjectsV1 = true

		// Act
		objects, err := st.List(context.Background(), "")

		// Assert
		require.NoError(t, err)
		require.Len(t, objects, 2)
		assert.Equal(t, []string{"a.txt", "b.txt"}, []string{objects[0].Name, objects[1].Name})
		assert.Equal(t, []string{"", "p/a.txt"}, svc.listMarkers(), "second page must start after the last key")
	})

	t.Run("a prefix holding nothing is an empty listing, not an error", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjectsV2", mock.Anything, mock.Anything).Return(&s3.ListObjectsV2Output{}, nil)
		st := newStorage(t, "p/", svc, nil)

		// Act
		objects, err := st.List(context.Background(), "never_existed")

		// Assert
		require.NoError(t, err)
		assert.Empty(t, objects)
	})

	t.Run("v2 error is wrapped", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("ListObjectsV2", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))
		st := newStorage(t, "p/", svc, nil)

		// Act
		_, err := st.List(context.Background(), "")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error listing s3 objects v2")
	})
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
		wantAbsent  bool
		wantErrText string
	}{
		{name: "present builds full path name", prefix: "dumps/", fileName: "a.txt", wantName: "dumps/a.txt"},
		{name: "name keeps prefix and is not stripped", prefix: "a/b/", fileName: "c.txt", wantName: "a/b/c.txt"},
		{name: "typed NotFound reports Exist=false, not an error", prefix: "dumps/", fileName: "a.txt", headErr: &types.NotFound{}, wantName: "dumps/a.txt", wantAbsent: true},
		{name: "typed NoSuchKey reports Exist=false", prefix: "dumps/", fileName: "a.txt", headErr: &types.NoSuchKey{}, wantName: "dumps/a.txt", wantAbsent: true},
		{name: "apierr NotFound code reports Exist=false", prefix: "dumps/", fileName: "a.txt", headErr: apiErr(NotFountAwsErrorCode), wantName: "dumps/a.txt", wantAbsent: true},
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
					Return(&s3.HeadObjectOutput{LastModified: aws.Time(modTime), ContentLength: aws.Int64(42), ETag: aws.String(`"e1"`)}, nil)
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
			if tt.wantAbsent {
				assert.False(t, stat.Exist)
				assert.Equal(t, tt.wantName, stat.Name)
				svc.AssertExpectations(t)
				return
			}
			assert.Equal(t, tt.wantName, stat.Name)
			assert.Equal(t, `"e1"`, stat.ETag)
			assert.True(t, stat.Exist)
			assert.True(t, stat.LastModified.Equal(modTime), "LastModified = %v", stat.LastModified)
			assert.Equal(t, int64(42), stat.Size, "Size must come from the HeadObject Content-Length")
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
			// Arrange: every key verifies as present, so batching is what the
			// assertions below actually observe.
			svc := &mockS3{}
			svc.On("HeadObject", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{}, nil).Maybe()
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
	svc.On("HeadObject", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{}, nil).Maybe()
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

func TestStorage_Delete_VerifiesBeforeDeleting(t *testing.T) {
	t.Run("a missing key deletes nothing and is reported", func(t *testing.T) {
		// Arrange: "a.txt" is present, "ghost.txt" is not.
		svc := &mockS3{}
		svc.On("HeadObject", mock.Anything, mock.MatchedBy(func(in *s3.HeadObjectInput) bool {
			return aws.ToString(in.Key) == "dumps/a.txt"
		})).Return(&s3.HeadObjectOutput{}, nil)
		svc.On("HeadObject", mock.Anything, mock.MatchedBy(func(in *s3.HeadObjectInput) bool {
			return aws.ToString(in.Key) == "dumps/ghost.txt"
		})).Return(nil, &types.NotFound{})
		st := newStorage(t, "dumps/", svc, nil)

		// Act
		err := st.Delete(context.Background(), "a.txt", "ghost.txt")

		// Assert
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
		var missing *storages.MissingObjectsError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, []string{"ghost.txt"}, missing.Paths)
		assert.Empty(t, svc.deleteBatches(), "a failed verification must delete nothing")
	})

	t.Run("a verification error other than not-found is wrapped", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("HeadObject", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))
		st := newStorage(t, "dumps/", svc, nil)

		// Act
		err := st.Delete(context.Background(), "a.txt")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error checking object")
		assert.NotErrorIs(t, err, storages.ErrFileNotFound)
		assert.Empty(t, svc.deleteBatches())
	})
}

func TestStorage_DeleteAll_EmptyPrefix(t *testing.T) {
	// Arrange: nothing carries the prefix, which is the only sense in which a
	// prefix can be missing on an object store.
	svc := &mockS3{}
	svc.On("ListObjectsV2", mock.Anything, mock.Anything).Return(&s3.ListObjectsV2Output{}, nil)
	st := newStorage(t, "", svc, nil)

	// Act
	err := st.DeleteAll(context.Background(), "gone")

	// Assert
	assert.ErrorIs(t, err, storages.ErrFileNotFound)
	assert.Empty(t, svc.deleteBatches())
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
	assert.Empty(t, svc.headKeys(),
		"keys came from the walk, so DeleteAll must not re-check them with HeadObject")
	batches := svc.deleteBatches()
	require.Len(t, batches, 1)
	assert.Equal(t,
		[]string{"sub/f1.txt", "sub/f2.txt", "sub/nested/g.txt"},
		batches[0],
		"every walked key must be deleted with the sub-storage prefix re-applied",
	)
}

func TestStorage_Copy(t *testing.T) {
	tests := []struct {
		name      string
		src, dst  string
		headErr   error
		copyErr   error
		wantCopy  bool
		wantErrIs error
		wantErr   string
	}{
		{name: "copies with a single CopyObject", src: "a.txt", dst: "b/c.txt", wantCopy: true},
		{name: "missing source is ErrFileNotFound and copies nothing", src: "a.txt", dst: "b.txt",
			headErr: &types.NotFound{}, wantErrIs: storages.ErrFileNotFound},
		{name: "head failure is wrapped", src: "a.txt", dst: "b.txt",
			headErr: errors.New("boom"), wantErr: "error getting object info"},
		{name: "a copy onto itself sends no copy", src: "a.txt", dst: "a.txt"},
		{name: "a source vanishing before the copy is ErrFileNotFound", src: "a.txt", dst: "b.txt",
			copyErr: &types.NoSuchKey{}, wantCopy: true, wantErrIs: storages.ErrFileNotFound},
		{name: "copy failure is wrapped", src: "a.txt", dst: "b.txt",
			copyErr: errors.New("boom"), wantCopy: true, wantErr: "s3 object copying error"},
		{name: "a source overwritten after the HEAD is ErrSourceChanged", src: "a.txt", dst: "b.txt",
			copyErr: apiErr(PreconditionFailedAwsErrorCode), wantCopy: true, wantErrIs: storages.ErrSourceChanged},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			svc.On("HeadObject", mock.Anything, mock.Anything).
				Return(&s3.HeadObjectOutput{ContentLength: aws.Int64(4), ETag: aws.String(`"v1"`)}, tt.headErr)
			if tt.wantCopy {
				svc.On("CopyObject", mock.Anything, mock.Anything).Return(&s3.CopyObjectOutput{}, tt.copyErr)
			}
			st := newStorage(t, "dumps/", svc, nil)

			// Act
			stat, err := st.Copy(context.Background(), tt.src, tt.dst)

			// Assert
			switch {
			case tt.wantErrIs != nil:
				require.ErrorIs(t, err, tt.wantErrIs)
				assert.Zero(t, stat, "a failed copy reports nothing")
			case tt.wantErr != "":
				require.ErrorContains(t, err, tt.wantErr)
				assert.Zero(t, stat, "a failed copy reports nothing")
			case tt.src == tt.dst:
				require.NoError(t, err)
				assert.Zero(t, stat, "a copy onto itself writes nothing")
			default:
				require.NoError(t, err)
				assert.Equal(t, storages.CopyStat{Bytes: 4}, stat, "the size comes from the HEAD; nothing was streamed")
			}
			svc.AssertExpectations(t)
			assert.Equal(t, []string{"dumps/" + tt.src}, svc.headKeys())
			if !tt.wantCopy {
				svc.AssertNotCalled(t, "CopyObject", mock.Anything, mock.Anything)
				return
			}
			in := svc.lastCall("CopyObject").(*s3.CopyObjectInput)
			assert.Equal(t, "test-bucket", aws.ToString(in.Bucket))
			assert.Equal(t, "test-bucket/dumps/"+tt.src, aws.ToString(in.CopySource))
			assert.Equal(t, "dumps/"+tt.dst, aws.ToString(in.Key))
			assert.Equal(t, types.StorageClass(defaultStorageClass), in.StorageClass)
			assert.Equal(t, `"v1"`, aws.ToString(in.CopySourceIfMatch), "the copy is pinned to the version the HEAD saw")
		})
	}
}

// TestCopyAll_MoreThanOneListPage checks that CopyAll copies every key of a
// multi-page listing exactly once.
func TestCopyAll_MoreThanOneListPage(t *testing.T) {
	const total = 2500
	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("dumps/src/obj-%04d", i)
	}
	pages := [][]string{keys[:1000], keys[1000:2000], keys[2000:]}
	objects := func(page []string) []types.Object {
		res := make([]types.Object, len(page))
		for i, k := range page {
			res[i] = types.Object{Key: aws.String(k), Size: aws.Int64(3), ETag: aws.String(`"` + k + `"`)}
		}
		return res
	}

	for _, tc := range []struct {
		name string
		v1   bool
	}{{name: "ListObjectsV2"}, {name: "ListObjects v1", v1: true}} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			if tc.v1 {
				// NextMarker is left empty, as S3 leaves it without a delimiter.
				for i, page := range pages {
					marker := ""
					if i > 0 {
						marker = pages[i-1][len(pages[i-1])-1]
					}
					svc.On("ListObjects", mock.Anything, mock.MatchedBy(func(in *s3.ListObjectsInput) bool {
						return aws.ToString(in.Prefix) == "dumps/src/" && aws.ToString(in.Marker) == marker
					})).Return(&s3.ListObjectsOutput{
						Contents:    objects(page),
						IsTruncated: aws.Bool(i < len(pages)-1),
					}, nil).Once()
				}
			} else {
				for i, page := range pages {
					token := ""
					if i > 0 {
						token = fmt.Sprintf("token-%d", i)
					}
					out := &s3.ListObjectsV2Output{Contents: objects(page), IsTruncated: aws.Bool(i < len(pages)-1)}
					if i < len(pages)-1 {
						out.NextContinuationToken = aws.String(fmt.Sprintf("token-%d", i+1))
					}
					svc.On("ListObjectsV2", mock.Anything, mock.MatchedBy(func(in *s3.ListObjectsV2Input) bool {
						return aws.ToString(in.Prefix) == "dumps/src/" && aws.ToString(in.ContinuationToken) == token
					})).Return(out, nil).Once()
				}
			}
			svc.On("CopyObject", mock.Anything, mock.Anything).Return(&s3.CopyObjectOutput{}, nil)
			st := newStorage(t, "dumps/", svc, nil)
			st.config.UseListObjectsV1 = tc.v1

			// Act
			stat, err := st.CopyAll(context.Background(), "src", "dst",
				storages.WithCopyAllObjectConcurrency(16))

			// Assert
			require.NoError(t, err)
			svc.AssertExpectations(t)
			assert.Equal(t, storages.CopyStat{Bytes: 3 * total, Objects: total}, stat)

			var gotSrc, gotDst []string
			for _, c := range svc.Calls {
				if c.Method != "CopyObject" {
					continue
				}
				in := c.Arguments.Get(1).(*s3.CopyObjectInput)
				src := strings.TrimPrefix(aws.ToString(in.CopySource), "test-bucket/")
				gotSrc = append(gotSrc, src)
				gotDst = append(gotDst, aws.ToString(in.Key))
				assert.Equal(t, `"`+src+`"`, aws.ToString(in.CopySourceIfMatch), "each copy is pinned to its listed ETag")
			}
			// Copies run concurrently, so compare as sets: every key exactly once.
			wantDst := make([]string, total)
			for i, k := range keys {
				wantDst[i] = strings.Replace(k, "dumps/src/", "dumps/dst/", 1)
			}
			assert.ElementsMatch(t, keys, gotSrc, "every listed key is copied exactly once")
			assert.ElementsMatch(t, wantDst, gotDst)
			assert.Empty(t, svc.headKeys(), "the listing already gave size and ETag, so no HEAD is sent")
		})
	}
}

// TestStorage_copyListed checks that a listed object is copied without a HEAD
// unless its listing is stale or not enough to copy it.
func TestStorage_copyListed(t *testing.T) {
	const listedETag, headETag = `"listed"`, `"current"`
	listed := func(size int64) storages.ObjectStat {
		return storages.ObjectStat{Name: "a.txt", Size: size, ETag: listedETag, Exist: true}
	}
	ifMatch := func(etag string) any {
		return mock.MatchedBy(func(in *s3.CopyObjectInput) bool { return aws.ToString(in.CopySourceIfMatch) == etag })
	}

	t.Run("skips the HEAD and pins the copy to the listed ETag", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("CopyObject", mock.Anything, ifMatch(listedETag)).Return(&s3.CopyObjectOutput{}, nil)
		st := newStorage(t, "dumps/", svc, nil)

		// Act
		stat, err := st.copyListed(context.Background(), listed(7), "a.txt", "b.txt")

		// Assert
		require.NoError(t, err)
		assert.Equal(t, storages.CopyStat{Bytes: 7}, stat, "the size comes from the listing")
		assert.Empty(t, svc.headKeys())
		svc.AssertExpectations(t)
	})

	t.Run("a stale listing falls back to the HEAD and copies the current version", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("CopyObject", mock.Anything, ifMatch(listedETag)).
			Return(nil, apiErr(PreconditionFailedAwsErrorCode)).Once()
		svc.On("HeadObject", mock.Anything, mock.Anything).
			Return(&s3.HeadObjectOutput{ContentLength: aws.Int64(9), ETag: aws.String(headETag)}, nil)
		svc.On("CopyObject", mock.Anything, ifMatch(headETag)).Return(&s3.CopyObjectOutput{}, nil).Once()
		st := newStorage(t, "dumps/", svc, nil)

		// Act
		stat, err := st.copyListed(context.Background(), listed(7), "a.txt", "b.txt")

		// Assert
		require.NoError(t, err)
		assert.Equal(t, storages.CopyStat{Bytes: 9}, stat, "the size of the version actually copied")
		assert.Equal(t, []string{"dumps/a.txt"}, svc.headKeys())
		svc.AssertExpectations(t)
	})

	t.Run("a source deleted since the listing is ErrFileNotFound", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("CopyObject", mock.Anything, mock.Anything).Return(nil, &types.NoSuchKey{})
		st := newStorage(t, "dumps/", svc, nil)

		// Act
		_, err := st.copyListed(context.Background(), listed(7), "a.txt", "b.txt")

		// Assert
		require.ErrorIs(t, err, storages.ErrFileNotFound)
		assert.Empty(t, svc.headKeys())
	})

	for _, tc := range []struct {
		name     string
		obj      storages.ObjectStat
		src, dst string
	}{
		{name: "a listing without an ETag takes the HEAD", obj: storages.ObjectStat{Name: "a.txt", Size: 7}, src: "a.txt", dst: "b.txt"},
		{name: "a copy onto itself still checks the source exists", obj: listed(7), src: "a.txt", dst: "a.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			svc := &mockS3{}
			svc.On("HeadObject", mock.Anything, mock.Anything).
				Return(&s3.HeadObjectOutput{ContentLength: aws.Int64(9), ETag: aws.String(headETag)}, nil)
			svc.On("CopyObject", mock.Anything, ifMatch(headETag)).Return(&s3.CopyObjectOutput{}, nil).Maybe()
			st := newStorage(t, "dumps/", svc, nil)

			// Act
			_, err := st.copyListed(context.Background(), tc.obj, tc.src, tc.dst)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, []string{"dumps/a.txt"}, svc.headKeys())
			svc.AssertNotCalled(t, "CopyObject", mock.Anything, ifMatch(listedETag))
		})
	}

	t.Run("an object too large for CopyObject takes the HEAD for its headers", func(t *testing.T) {
		// Arrange
		svc := &mockS3{}
		svc.On("HeadObject", mock.Anything, mock.Anything).Return(nil, errors.New("stop here"))
		st := newStorage(t, "dumps/", svc, nil)

		// Act
		_, err := st.copyListed(context.Background(), listed(maxCopyObjectSize+1), "a.txt", "b.txt")

		// Assert
		require.ErrorContains(t, err, "stop here")
		assert.Equal(t, []string{"dumps/a.txt"}, svc.headKeys(), "the multipart copy needs the HEAD's headers")
		svc.AssertNotCalled(t, "CopyObject", mock.Anything, mock.Anything)
	})
}

// TestStorage_Copy_Encryption checks that a copy carries the same SSE headers
// PutObject does.
func TestStorage_Copy_Encryption(t *testing.T) {
	svc := &mockS3{}
	svc.On("HeadObject", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{ContentLength: aws.Int64(4)}, nil)
	svc.On("CopyObject", mock.Anything, mock.Anything).Return(&s3.CopyObjectOutput{}, nil)
	st := newStorage(t, "", svc, nil)
	st.config.SSE = "aws:kms"
	st.config.KMSKeyARN = testKMSKeyARN
	st.config.BucketKeyEnabled = true

	_, err := st.Copy(context.Background(), "a.txt", "b.txt")
	require.NoError(t, err)

	in := svc.lastCall("CopyObject").(*s3.CopyObjectInput)
	assert.Equal(t, types.ServerSideEncryptionAwsKms, in.ServerSideEncryption)
	assert.Equal(t, aws.String(testKMSKeyARN), in.SSEKMSKeyId)
	assert.Equal(t, aws.Bool(true), in.BucketKeyEnabled)
}

// TestStorage_Copy_LargeObjectUsesMultipartCopy checks that Copy switches to a
// multipart copy above 5 GiB, carrying content type and metadata.
func TestStorage_Copy_LargeObjectUsesMultipartCopy(t *testing.T) {
	const size = maxCopyObjectSize + 1
	svc := &mockS3{}
	svc.On("HeadObject", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{
		ContentLength: aws.Int64(size),
		ETag:          aws.String(`"src-etag"`),
		ContentType:   aws.String("application/x-tar"),
		Metadata:      map[string]string{"k": "v"},
	}, nil)
	svc.On("CreateMultipartUpload", mock.Anything, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-1")}, nil)
	svc.On("UploadPartCopy", mock.Anything, mock.Anything).Return(nil, nil)
	svc.On("CompleteMultipartUpload", mock.Anything, mock.Anything).Return(&s3.CompleteMultipartUploadOutput{}, nil)
	st := newStorage(t, "dumps/", svc, nil)
	st.config.SSE = "AES256"

	stat, err := st.Copy(context.Background(), "big.tar", "copy.tar")
	require.NoError(t, err)

	assert.Equal(t, storages.CopyStat{Bytes: size}, stat)
	svc.AssertNotCalled(t, "CopyObject", mock.Anything, mock.Anything)
	create := svc.lastCall("CreateMultipartUpload").(*s3.CreateMultipartUploadInput)
	assert.Equal(t, "dumps/copy.tar", aws.ToString(create.Key))
	assert.Equal(t, "application/x-tar", aws.ToString(create.ContentType))
	assert.Equal(t, map[string]string{"k": "v"}, create.Metadata)
	assert.Equal(t, types.ServerSideEncryptionAes256, create.ServerSideEncryption)

	// 5 GiB + 1 byte in 512 MiB parts is ten full parts and a one-byte eleventh.
	wantParts := int32((size + copyPartSize - 1) / copyPartSize)
	ranges := svc.partRanges()
	require.Len(t, ranges, int(wantParts))
	assert.Equal(t, fmt.Sprintf("bytes=0-%d", copyPartSize-1), ranges[1])
	assert.Equal(t, fmt.Sprintf("bytes=%d-%d", size-1, size-1), ranges[wantParts])

	part := svc.lastCall("UploadPartCopy").(*s3.UploadPartCopyInput)
	assert.Equal(t, "upload-1", aws.ToString(part.UploadId))
	assert.Equal(t, "test-bucket/dumps/big.tar", aws.ToString(part.CopySource))
	assert.Equal(t, `"src-etag"`, aws.ToString(part.CopySourceIfMatch), "every part is pinned to the source version")

	complete := svc.lastCall("CompleteMultipartUpload").(*s3.CompleteMultipartUploadInput)
	require.Len(t, complete.MultipartUpload.Parts, int(wantParts))
	for i, p := range complete.MultipartUpload.Parts {
		assert.Equal(t, int32(i+1), aws.ToInt32(p.PartNumber), "parts must be completed in order")
		assert.Equal(t, fmt.Sprintf("etag-%d", i+1), aws.ToString(p.ETag))
	}
	svc.AssertNotCalled(t, "AbortMultipartUpload", mock.Anything, mock.Anything)
}

func TestStorage_multipartCopy_Ranges(t *testing.T) {
	tests := []struct {
		name       string
		size       int64
		partSize   int64
		wantRanges map[int32]string
		wantParts  int
	}{
		{name: "last part is short", size: 10, partSize: 4,
			wantRanges: map[int32]string{1: "bytes=0-3", 2: "bytes=4-7", 3: "bytes=8-9"}},
		{name: "exact multiple has no empty tail", size: 8, partSize: 4,
			wantRanges: map[int32]string{1: "bytes=0-3", 2: "bytes=4-7"}},
		{name: "part size grows to stay within 10000 parts", size: 30000, partSize: 1, wantParts: 10000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &mockS3{}
			svc.On("CreateMultipartUpload", mock.Anything, mock.Anything).
				Return(&s3.CreateMultipartUploadOutput{UploadId: aws.String("u")}, nil)
			svc.On("UploadPartCopy", mock.Anything, mock.Anything).Return(nil, nil)
			svc.On("CompleteMultipartUpload", mock.Anything, mock.Anything).Return(&s3.CompleteMultipartUploadOutput{}, nil)
			st := newStorage(t, "", svc, nil)

			head := &s3.HeadObjectOutput{ContentLength: aws.Int64(tt.size)}
			require.NoError(t, st.multipartCopy(context.Background(), "src", "dst", head, tt.partSize, 5))

			ranges := svc.partRanges()
			if tt.wantRanges != nil {
				assert.Equal(t, tt.wantRanges, ranges)
			} else {
				assert.Len(t, ranges, tt.wantParts)
				assert.Equal(t, "bytes=0-2", ranges[1])
			}
		})
	}
}

// TestStorage_multipartCopy_FailureAborts checks that a failed part aborts the
// upload.
func TestStorage_multipartCopy_FailureAborts(t *testing.T) {
	tests := []struct {
		name        string
		partErr     error
		completeErr error
		wantErrIs   error
		wantErr     string
	}{
		{name: "part failure", partErr: errors.New("boom"), wantErr: "error copying part"},
		{name: "source deleted mid-copy", partErr: &types.NoSuchKey{}, wantErrIs: storages.ErrFileNotFound},
		{name: "complete failure", completeErr: errors.New("boom"), wantErr: "error completing multipart copy"},
		{name: "source overwritten mid-copy", partErr: apiErr(PreconditionFailedAwsErrorCode), wantErrIs: storages.ErrSourceChanged},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &mockS3{}
			svc.On("CreateMultipartUpload", mock.Anything, mock.Anything).
				Return(&s3.CreateMultipartUploadOutput{UploadId: aws.String("u")}, nil)
			svc.On("UploadPartCopy", mock.Anything, mock.Anything).Return(nil, tt.partErr)
			if tt.partErr == nil {
				svc.On("CompleteMultipartUpload", mock.Anything, mock.Anything).
					Return(&s3.CompleteMultipartUploadOutput{}, tt.completeErr)
			}
			svc.On("AbortMultipartUpload", mock.Anything, mock.Anything).Return(&s3.AbortMultipartUploadOutput{}, nil)
			st := newStorage(t, "", svc, nil)

			head := &s3.HeadObjectOutput{ContentLength: aws.Int64(100)}
			err := st.multipartCopy(context.Background(), "src", "dst", head, 10, 5)

			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			abort := svc.lastCall("AbortMultipartUpload").(*s3.AbortMultipartUploadInput)
			assert.Equal(t, "u", aws.ToString(abort.UploadId))
			assert.Equal(t, "dst", aws.ToString(abort.Key))
			if tt.partErr != nil {
				svc.AssertNotCalled(t, "CompleteMultipartUpload", mock.Anything, mock.Anything)
			}
		})
	}
}

// TestStorage_multipartCopy_CarriesHeadersAndTags checks that a multipart copy
// carries over the headers and tags CopyObject would.
func TestStorage_multipartCopy_CarriesHeadersAndTags(t *testing.T) {
	expires := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	svc := &mockS3{}
	svc.On("GetObjectTagging", mock.Anything, mock.Anything).Return(&s3.GetObjectTaggingOutput{
		TagSet: []types.Tag{{Key: aws.String("team"), Value: aws.String("data eng")}, {Key: aws.String("tier"), Value: aws.String("cold")}},
	}, nil)
	svc.On("CreateMultipartUpload", mock.Anything, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{UploadId: aws.String("u")}, nil)
	svc.On("UploadPartCopy", mock.Anything, mock.Anything).Return(nil, nil)
	svc.On("CompleteMultipartUpload", mock.Anything, mock.Anything).Return(&s3.CompleteMultipartUploadOutput{}, nil)
	st := newStorage(t, "", svc, nil)

	head := &s3.HeadObjectOutput{
		ContentLength:      aws.Int64(10),
		CacheControl:       aws.String("no-cache"),
		ContentDisposition: aws.String("attachment"),
		ContentEncoding:    aws.String("gzip"),
		ContentLanguage:    aws.String("en"),
		ContentType:        aws.String("application/gzip"),
		Expires:            &expires,
		Metadata:           map[string]string{"k": "v"},
		TagCount:           aws.Int32(2),
		VersionId:          aws.String("v1"),
	}
	require.NoError(t, st.multipartCopy(context.Background(), "src", "dst", head, 4, 5))

	tagging := svc.lastCall("GetObjectTagging").(*s3.GetObjectTaggingInput)
	assert.Equal(t, "v1", aws.ToString(tagging.VersionId), "tags are read from the version being copied")
	create := svc.lastCall("CreateMultipartUpload").(*s3.CreateMultipartUploadInput)
	assert.Equal(t, "no-cache", aws.ToString(create.CacheControl))
	assert.Equal(t, "attachment", aws.ToString(create.ContentDisposition))
	assert.Equal(t, "gzip", aws.ToString(create.ContentEncoding))
	assert.Equal(t, "en", aws.ToString(create.ContentLanguage))
	assert.Equal(t, "application/gzip", aws.ToString(create.ContentType))
	assert.Equal(t, &expires, create.Expires)
	assert.Equal(t, map[string]string{"k": "v"}, create.Metadata)
	assert.Equal(t, "team=data%20eng&tier=cold", aws.ToString(create.Tagging))
}

// TestStorage_multipartCopy_UntaggedSkipsTagging checks that an untagged source
// costs no GetObjectTagging request.
func TestStorage_multipartCopy_UntaggedSkipsTagging(t *testing.T) {
	svc := &mockS3{}
	svc.On("CreateMultipartUpload", mock.Anything, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{UploadId: aws.String("u")}, nil)
	svc.On("UploadPartCopy", mock.Anything, mock.Anything).Return(nil, nil)
	svc.On("CompleteMultipartUpload", mock.Anything, mock.Anything).Return(&s3.CompleteMultipartUploadOutput{}, nil)
	st := newStorage(t, "", svc, nil)

	head := &s3.HeadObjectOutput{ContentLength: aws.Int64(10)}
	require.NoError(t, st.multipartCopy(context.Background(), "src", "dst", head, 4, 5))

	svc.AssertNotCalled(t, "GetObjectTagging", mock.Anything, mock.Anything)
	assert.Nil(t, svc.lastCall("CreateMultipartUpload").(*s3.CreateMultipartUploadInput).Tagging)
}

func TestStorage_copyConcurrency(t *testing.T) {
	tests := []struct {
		name   string
		config int
		opts   []storages.CopyOption
		want   int
	}{
		{name: "sdk default when nothing is set", want: manager.DefaultUploadConcurrency},
		{name: "config below the default is honored", config: 1, want: 1},
		{name: "config above the default is honored", config: 16, want: 16},
		{name: "the call option overrides the config", config: 16, opts: []storages.CopyOption{storages.WithCopyConcurrency(2)}, want: 2},
		{name: "a non-positive option falls back to the config", config: 3, opts: []storages.CopyOption{storages.WithCopyConcurrency(0)}, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newStorage(t, "", nil, nil)
			st.config.Concurrency = tt.config
			assert.Equal(t, tt.want, st.copyConcurrency(storages.NewCopyOptions(tt.opts...)))
		})
	}
}

func Test_resolveCopyPartSize(t *testing.T) {
	tests := []struct {
		name string
		opts []storages.CopyOption
		want int64
	}{
		{name: "default", want: copyPartSize},
		{name: "option within limits", opts: []storages.CopyOption{storages.WithCopyPartSize(64 << 20)}, want: 64 << 20},
		{name: "below the S3 minimum is raised to it", opts: []storages.CopyOption{storages.WithCopyPartSize(1)}, want: manager.MinUploadPartSize},
		{name: "above the S3 maximum is lowered to it", opts: []storages.CopyOption{storages.WithCopyPartSize(10 << 30)}, want: maxCopyPartSize},
		{name: "non-positive keeps the default", opts: []storages.CopyOption{storages.WithCopyPartSize(-1)}, want: copyPartSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveCopyPartSize(storages.NewCopyOptions(tt.opts...)))
		})
	}
}

// TestStorage_Copy_RespectsConcurrency checks that the resolved concurrency is a
// hard cap on overlapping part copies.
func TestStorage_Copy_RespectsConcurrency(t *testing.T) {
	const size = maxCopyObjectSize + 1
	var inFlight, peak atomic.Int32
	svc := &mockS3{partCopyHook: func() {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
	}}
	svc.On("HeadObject", mock.Anything, mock.Anything).Return(&s3.HeadObjectOutput{ContentLength: aws.Int64(size)}, nil)
	svc.On("CreateMultipartUpload", mock.Anything, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{UploadId: aws.String("u")}, nil)
	svc.On("UploadPartCopy", mock.Anything, mock.Anything).Return(nil, nil)
	svc.On("CompleteMultipartUpload", mock.Anything, mock.Anything).Return(&s3.CompleteMultipartUploadOutput{}, nil)
	st := newStorage(t, "", svc, nil)
	st.config.Concurrency = 8

	_, err := st.Copy(context.Background(), "src", "dst",
		storages.WithCopyConcurrency(2), storages.WithCopyPartSize(256<<20))

	require.NoError(t, err)
	assert.Len(t, svc.partRanges(), int((size+(256<<20)-1)/(256<<20)), "the part size option must apply")
	assert.LessOrEqual(t, peak.Load(), int32(2), "no more part copies may overlap than the option allows")
	assert.Equal(t, int32(2), peak.Load(), "the allowed concurrency should actually be used")
}

func Test_copySource(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{key: "dumps/a.txt", want: "bucket/dumps/a.txt"},
		{key: "with space/a b.txt", want: "bucket/with%20space/a%20b.txt"},
		{key: "plus+sign.txt", want: "bucket/plus%2Bsign.txt"},
		{key: "q?x#y%z.txt", want: "bucket/q%3Fx%23y%25z.txt"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.want, copySource("bucket", tt.key))
		})
	}
}

// The guard itself is tested once, in the root package, over a backend-agnostic
// decorator. What this backend has to pin is only that its constructor applies
// it — that a caller who does nothing special gets a storage no key can climb
// out of. No request is made: the key is refused before the SDK is reached.
func TestNewStorage_GuardsKeysByDefault(t *testing.T) {
	ctx := context.Background()

	cfg := DefaultConfig()
	cfg.Bucket = "test-bucket"
	cfg.Region = "us-east-1"
	cfg.Prefix = "dumps"
	cfg.Endpoint = "http://127.0.0.1:1" // never dialled
	cfg.AccessKeyId = "id"
	cfg.SecretAccessKey = "secret"

	st, err := New(ctx, cfg)
	require.NoError(t, err)

	_, err = st.GetObject(ctx, "../../escape.txt")
	assert.ErrorIs(t, err, storages.ErrUnsafeKey)
	assert.ErrorIs(t, st.DeleteAll(ctx, ""), storages.ErrUnsafeKey)

	// WithUnsafe hands back the bare backend instead.
	unsafe, err := New(ctx, cfg, WithUnsafe())
	require.NoError(t, err)
	_, ok := unsafe.(*Storage)
	assert.True(t, ok, "WithUnsafe must yield the bare backend")
}
