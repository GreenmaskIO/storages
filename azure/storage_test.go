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

package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/azurite"

	"github.com/greenmaskio/storages"
)

// ===========================================================================
// Pure logic (no client required)
// ===========================================================================

func TestConfig_Validate_RequiresContainerAndAccount(t *testing.T) {
	t.Run("missing container", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.StorageAccount = "test-account"
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing storage account", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Container = "test-container"
		assert.Error(t, cfg.Validate())
	})

	t.Run("valid input", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Container = "test-container"
		cfg.StorageAccount = "test-account"
		assert.NoError(t, cfg.Validate())
	})
}

func TestResolveAuth_AccessKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AccessKey = "foo"
	at, sasToken, accessKey := resolveAuth(cfg)
	assert.Equal(t, authTypeAccessKey, at)
	assert.Empty(t, sasToken)
	assert.Equal(t, "foo", accessKey)
}

func TestResolveAuth_SASToken(t *testing.T) {
	t.Run("without leading question mark", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.SASToken = "foo"
		at, sasToken, accessKey := resolveAuth(cfg)
		assert.Equal(t, authTypeSASToken, at)
		assert.Equal(t, "?foo", sasToken)
		assert.Empty(t, accessKey)
	})

	t.Run("with leading question mark", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.SASToken = "?foo"
		at, sasToken, accessKey := resolveAuth(cfg)
		assert.Equal(t, authTypeSASToken, at)
		assert.Equal(t, "?foo", sasToken)
		assert.Empty(t, accessKey)
	})

	t.Run("access key takes precedence", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.AccessKey = "key"
		cfg.SASToken = "token"
		at, _, accessKey := resolveAuth(cfg)
		assert.Equal(t, authTypeAccessKey, at)
		assert.Equal(t, "key", accessKey)
	})
}

func TestResolveAuth_Default(t *testing.T) {
	cfg := DefaultConfig()
	at, sasToken, accessKey := resolveAuth(cfg)
	assert.Equal(t, authTypeNotSpecified, at)
	assert.Empty(t, sasToken)
	assert.Empty(t, accessKey)
}

func TestEndpointSuffix(t *testing.T) {
	tests := map[string]string{
		"AzureUSGovernmentCloud": "core.usgovcloudapi.net",
		"AzureChinaCloud":        "core.chinacloudapi.cn",
		"AzureGermanCloud":       "core.cloudapi.de",
		"AzurePublicCloud":       "core.windows.net",
		"":                       "core.windows.net",
		"SomethingElse":          "core.windows.net",
	}
	for env, want := range tests {
		assert.Equalf(t, want, getStorageEndpointSuffix(env), "environment %q", env)
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, defaultBufferSize, cfg.BufferSize)
	assert.Equal(t, defaultBuffers, cfg.MaxBuffers)
	assert.Equal(t, defaultTryTimeout, cfg.TryTimeout)
	assert.Equal(t, defaultEnvName, cfg.EnvironmentName)

	// A zero MaxBuffers is treated as unset and gets the default, not the
	// minimum; Validate only clamps explicit below-minimum values.
	cfg.Container = "test-container"
	cfg.StorageAccount = "test-account"
	cfg.BufferSize = 1
	cfg.MaxBuffers = -5
	require.NoError(t, cfg.Validate())
	assert.Equal(t, minBufferSize, cfg.BufferSize)
	assert.Equal(t, minBuffers, cfg.MaxBuffers)
}

func TestContainerBaseURL(t *testing.T) {
	t.Run("endpoint override is path-style", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Endpoint = "http://127.0.0.1:10000/devstoreaccount1"
		cfg.Container = "greenmask-test"
		assert.Equal(t, "http://127.0.0.1:10000/devstoreaccount1/greenmask-test", containerBaseURL(cfg))
	})

	t.Run("subdomain form from environment name", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.StorageAccount = "acct"
		cfg.Container = "cont"
		assert.Equal(t, "https://acct.blob.core.windows.net/cont", containerBaseURL(cfg))
	})

	t.Run("explicit endpoint suffix wins over environment name", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.StorageAccount = "acct"
		cfg.Container = "cont"
		cfg.EnvironmentName = "AzureChinaCloud"
		cfg.EndpointSuffix = "core.windows.net"
		assert.Equal(t, "https://acct.blob.core.windows.net/cont", containerBaseURL(cfg))
	})
}

func TestBuildClientOptions(t *testing.T) {
	t.Run("try timeout maps to retry options", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TryTimeout = 7
		opts := buildClientOptions(cfg)
		assert.Equal(t, 7*time.Minute, opts.Retry.TryTimeout)
		assert.Empty(t, opts.PerCallPolicies, "no api-version policy without an override")
	})

	t.Run("api version policy appended only when configured", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.BlobStoreAPIVersion = "2021-08-06"
		opts := buildClientOptions(cfg)
		require.Len(t, opts.PerCallPolicies, 1)
		p, ok := opts.PerCallPolicies[0].(*apiVersionPolicy)
		require.True(t, ok)
		assert.Equal(t, "2021-08-06", p.apiVersion)
	})
}

// recordingTransport captures the outgoing request and returns a canned 200.
type recordingTransport struct {
	captured *http.Request
}

func (rt *recordingTransport) Do(req *http.Request) (*http.Response, error) {
	rt.captured = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
		Header:     http.Header{},
		Request:    req,
	}, nil
}

func TestApiVersionPolicy(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion string
		wantHeader string
	}{
		{"overrides x-ms-version when configured", "2021-08-06", "2021-08-06"},
		{"is a no-op when empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &recordingTransport{}
			pipeline := runtime.NewPipeline(
				"test", "v1.0.0",
				runtime.PipelineOptions{PerCall: []policy.Policy{&apiVersionPolicy{apiVersion: tt.apiVersion}}},
				&policy.ClientOptions{Transport: rt},
			)
			req, err := runtime.NewRequest(context.Background(), http.MethodGet, "https://example.com")
			require.NoError(t, err)

			_, err = pipeline.Do(req)
			require.NoError(t, err)
			// The policy sets the header under the literal (non-canonical) key
			// so that it overrides the value the SDK sets under that same literal
			// key; read the raw map rather than Header.Get, which canonicalizes.
			got := rt.captured.Header["x-ms-version"] //nolint:staticcheck // SA1008: non-canonical key is intentional — must match the SDK's literal header key
			if tt.wantHeader == "" {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, []string{tt.wantHeader}, got)
			}
		})
	}
}

// TestNewStorage_AuthDispatch verifies that every auth method builds a usable
// container client (no network calls are made by client construction).
func TestNewStorage_AuthDispatch(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
	}{
		{"access key", func(c *Config) { c.AccessKey = azurite.AccountKey }},
		{"sas token", func(c *Config) { c.SASToken = "sig=abc&se=2030-01-01" }},
		{"default credential chain", func(c *Config) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.StorageAccount = azurite.AccountName
			cfg.Container = "cont"
			cfg.Endpoint = "http://127.0.0.1:10000/devstoreaccount1"
			tt.configure(&cfg)

			st, err := NewStorage(context.Background(), cfg)
			require.NoError(t, err)
			require.NotNil(t, st)
			assert.NotNil(t, st.containerClient)
		})
	}
}

func Test_fixPrefix(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty stays empty", in: "", want: ""},
		{name: "adds trailing slash", in: "foo", want: "foo/"},
		{name: "idempotent when already suffixed", in: "foo/", want: "foo/"},
		{name: "trims leading slash", in: "/foo", want: "foo/"},
		{name: "trims leading and adds trailing", in: "/foo/bar", want: "foo/bar/"},
		{name: "nested", in: "a/b", want: "a/b/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, fixPrefix(tt.in))
		})
	}
}

func Test_blobName(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		in     string
		want   string
	}{
		{name: "joins prefix", prefix: "p/", in: "x.txt", want: "p/x.txt"},
		{name: "trims leading slash on name", prefix: "p/", in: "/x.txt", want: "p/x.txt"},
		{name: "empty prefix", prefix: "", in: "x.txt", want: "x.txt"},
		{name: "empty prefix trims leading slash", prefix: "", in: "/x.txt", want: "x.txt"},
		{name: "nested", prefix: "a/b/", in: "c/d.txt", want: "a/b/c/d.txt"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &Storage{prefix: tt.prefix}
			assert.Equal(t, tt.want, st.blobName(tt.in))
		})
	}
}

func TestStorage_GetCwd(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"", ""},
		{"foo", "foo/"},
		{"foo/bar", "foo/bar/"},
		{"/foo", "foo/"},
		{"foo/", "foo/"},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			st := &Storage{prefix: fixPrefix(tt.prefix)}
			assert.Equal(t, tt.want, st.GetCwd())
		})
	}
}

func TestStorage_Dirname(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"", "."},
		{"foo", "foo"},
		{"foo/bar", "bar"},
		{"/foo/bar", "bar"},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			st := &Storage{prefix: fixPrefix(tt.prefix)}
			assert.Equal(t, tt.want, st.Dirname())
		})
	}
}

func TestStorage_SubStorage(t *testing.T) {
	tests := []struct {
		name     string
		base     string
		sub      string
		relative bool
		wantCwd  string
	}{
		{"relative from root", "", "child", true, "child/"},
		{"relative nested", "parent/", "child", true, "parent/child/"},
		{"relative trims leading slash", "", "/child", true, "child/"},
		{"absolute replaces prefix", "parent/", "other", false, "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &Storage{prefix: tt.base}
			sub := st.SubStorage(tt.sub, tt.relative)
			assert.Equal(t, tt.wantCwd, sub.GetCwd())
		})
	}
}

// ===========================================================================
// Test doubles (testify/mock) for the containerAPI / blockBlobAPI seam
//
// mockContainer doubles the container seam and mockBlockBlob the block-blob
// seam; both embed mock.Mock. Expectations are set with .On(...).Return(...) and
// verified with AssertExpectations. This lets the object methods be exercised
// without a real Azure backend, so OUR logic (blobName construction, the
// BlobNotFound translation, listing prefix-stripping, Delete's skip-missing,
// DeleteAll's Walk→Delete wiring) is verified deterministically. The blob names
// Storage builds are captured on mockContainer.blobNames at call time.
// ===========================================================================

type mockBlockBlob struct {
	mock.Mock
}

func (m *mockBlockBlob) DownloadStream(
	ctx context.Context, o *blob.DownloadStreamOptions,
) (blob.DownloadStreamResponse, error) {
	args := m.Called(ctx, o)
	out, _ := args.Get(0).(blob.DownloadStreamResponse)
	return out, args.Error(1)
}

func (m *mockBlockBlob) UploadStream(
	ctx context.Context, body io.Reader, o *blockblob.UploadStreamOptions,
) (blockblob.UploadStreamResponse, error) {
	args := m.Called(ctx, body, o)
	out, _ := args.Get(0).(blockblob.UploadStreamResponse)
	return out, args.Error(1)
}

func (m *mockBlockBlob) Delete(
	ctx context.Context, o *blob.DeleteOptions,
) (blob.DeleteResponse, error) {
	args := m.Called(ctx, o)
	out, _ := args.Get(0).(blob.DeleteResponse)
	return out, args.Error(1)
}

func (m *mockBlockBlob) GetProperties(
	ctx context.Context, o *blob.GetPropertiesOptions,
) (blob.GetPropertiesResponse, error) {
	args := m.Called(ctx, o)
	out, _ := args.Get(0).(blob.GetPropertiesResponse)
	return out, args.Error(1)
}

// deleteBlob returns a block-blob double whose Delete yields the given error
// (nil for success, blobNotFound() to exercise the skip-missing path).
func deleteBlob(err error) *mockBlockBlob {
	b := &mockBlockBlob{}
	b.On("Delete", mock.Anything, mock.Anything).Return(blob.DeleteResponse{}, err)
	return b
}

type mockContainer struct {
	mock.Mock

	// blobNames captures the names passed to NewBlockBlobClient at call time.
	blobNames []string
}

func (m *mockContainer) NewBlockBlobClient(blobName string) blockBlobAPI {
	m.blobNames = append(m.blobNames, blobName)
	args := m.Called(blobName)
	return args.Get(0).(blockBlobAPI)
}

func (m *mockContainer) NewListBlobsHierarchyPager(
	delimiter string, o *container.ListBlobsHierarchyOptions,
) *runtime.Pager[container.ListBlobsHierarchyResponse] {
	args := m.Called(delimiter, o)
	return args.Get(0).(*runtime.Pager[container.ListBlobsHierarchyResponse])
}

func (m *mockContainer) GetProperties(
	ctx context.Context, o *container.GetPropertiesOptions,
) (container.GetPropertiesResponse, error) {
	args := m.Called(ctx, o)
	out, _ := args.Get(0).(container.GetPropertiesResponse)
	return out, args.Error(1)
}

func (m *mockContainer) Create(
	_ context.Context, _ *container.CreateOptions,
) (container.CreateResponse, error) {
	panic("Create is not used in mock tests")
}

func (m *mockContainer) NewListBlobsFlatPager(
	_ *container.ListBlobsFlatOptions,
) *runtime.Pager[container.ListBlobsFlatResponse] {
	panic("NewListBlobsFlatPager is not used in mock tests")
}

// atPrefix matches a hierarchy-pager options argument by its Prefix.
func atPrefix(prefix string) any {
	return mock.MatchedBy(func(o *container.ListBlobsHierarchyOptions) bool {
		return o != nil && o.Prefix != nil && *o.Prefix == prefix
	})
}

// hierarchyPage builds one page of a hierarchy listing from blob keys and
// sub-directory prefixes.
func hierarchyPage(blobs, prefixes []string) container.ListBlobsHierarchyResponse {
	seg := &container.BlobHierarchyListSegment{}
	for _, b := range blobs {
		seg.BlobItems = append(seg.BlobItems, &container.BlobItem{Name: to.Ptr(b)})
	}
	for _, p := range prefixes {
		seg.BlobPrefixes = append(seg.BlobPrefixes, &container.BlobPrefix{Name: to.Ptr(p)})
	}
	return container.ListBlobsHierarchyResponse{
		ListBlobsHierarchySegmentResponse: container.ListBlobsHierarchySegmentResponse{Segment: seg},
	}
}

// newFakeHierarchyPager returns a pager that yields the given pages in order.
func newFakeHierarchyPager(pages ...container.ListBlobsHierarchyResponse) *runtime.Pager[container.ListBlobsHierarchyResponse] {
	idx := 0
	return runtime.NewPager(runtime.PagingHandler[container.ListBlobsHierarchyResponse]{
		More: func(container.ListBlobsHierarchyResponse) bool { return idx < len(pages) },
		Fetcher: func(context.Context, *container.ListBlobsHierarchyResponse) (container.ListBlobsHierarchyResponse, error) {
			p := pages[idx]
			idx++
			return p, nil
		},
	})
}

// newErrorHierarchyPager returns a pager whose first fetch fails.
func newErrorHierarchyPager(err error) *runtime.Pager[container.ListBlobsHierarchyResponse] {
	return runtime.NewPager(runtime.PagingHandler[container.ListBlobsHierarchyResponse]{
		More: func(container.ListBlobsHierarchyResponse) bool { return true },
		Fetcher: func(context.Context, *container.ListBlobsHierarchyResponse) (container.ListBlobsHierarchyResponse, error) {
			return container.ListBlobsHierarchyResponse{}, err
		},
	})
}

// blobNotFound builds an error that bloberror.HasCode recognizes as BlobNotFound.
func blobNotFound() error {
	return &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}
}

func newMockStorage(t *testing.T, prefix string, c containerAPI) *Storage {
	t.Helper()
	return &Storage{
		config:          DefaultConfig(),
		containerClient: c,
		prefix:          prefix,
		logger:          slog.New(slog.DiscardHandler),
	}
}

// ===========================================================================
// Object I/O logic (mocked seam)
// ===========================================================================

func TestStorage_PutObject(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		filePath     string
		uploadErr    error
		wantBlobName string
		wantErr      string
	}{
		{name: "joins prefix", prefix: "dumps/", filePath: "a.txt", wantBlobName: "dumps/a.txt"},
		{name: "trims leading slash", prefix: "dumps/", filePath: "/a.txt", wantBlobName: "dumps/a.txt"},
		{name: "empty prefix", prefix: "", filePath: "a.txt", wantBlobName: "a.txt"},
		{name: "nested path", prefix: "dumps/", filePath: "x/y.txt", wantBlobName: "dumps/x/y.txt"},
		{name: "upload error wrapped", prefix: "dumps/", filePath: "a.txt", uploadErr: errors.New("boom"), wantBlobName: "dumps/a.txt", wantErr: "azure object uploading error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			bb := &mockBlockBlob{}
			bb.On("UploadStream", mock.Anything, mock.Anything, mock.Anything).
				Return(blockblob.UploadStreamResponse{}, tt.uploadErr)
			c := &mockContainer{}
			c.On("NewBlockBlobClient", mock.Anything).Return(bb)
			st := newMockStorage(t, tt.prefix, c)

			// Act
			err := st.PutObject(context.Background(), tt.filePath, bytes.NewReader([]byte("data")))

			// Assert
			assert.Equal(t, []string{tt.wantBlobName}, c.blobNames)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			assert.NoError(t, err)
			bb.AssertExpectations(t)
		})
	}
}

func TestStorage_GetObject(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		filePath     string
		downloadErr  error
		body         string
		wantBlobName string
		wantBody     string
		wantErrIs    error
		wantErrText  string
	}{
		{name: "success returns body", prefix: "dumps/", filePath: "a.txt", body: "hello", wantBlobName: "dumps/a.txt", wantBody: "hello"},
		{name: "trims leading slash", prefix: "dumps/", filePath: "/a.txt", body: "hi", wantBlobName: "dumps/a.txt", wantBody: "hi"},
		{name: "BlobNotFound maps to ErrFileNotFound", prefix: "dumps/", filePath: "a.txt", downloadErr: blobNotFound(), wantBlobName: "dumps/a.txt", wantErrIs: storages.ErrFileNotFound},
		{name: "other error wrapped", prefix: "dumps/", filePath: "a.txt", downloadErr: errors.New("network"), wantBlobName: "dumps/a.txt", wantErrText: "error getting object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			bb := &mockBlockBlob{}
			if tt.downloadErr != nil {
				bb.On("DownloadStream", mock.Anything, mock.Anything).
					Return(blob.DownloadStreamResponse{}, tt.downloadErr)
			} else {
				bb.On("DownloadStream", mock.Anything, mock.Anything).Return(blob.DownloadStreamResponse{
					DownloadResponse: blob.DownloadResponse{Body: io.NopCloser(strings.NewReader(tt.body))},
				}, nil)
			}
			c := &mockContainer{}
			c.On("NewBlockBlobClient", mock.Anything).Return(bb)
			st := newMockStorage(t, tt.prefix, c)

			// Act
			reader, err := st.GetObject(context.Background(), tt.filePath)

			// Assert
			assert.Equal(t, []string{tt.wantBlobName}, c.blobNames)
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
		})
	}
}

func TestStorage_Exists(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		fileName     string
		getErr       error
		wantBlobName string
		wantExists   bool
		wantErrText  string
	}{
		{name: "present", prefix: "dumps/", fileName: "a.txt", wantBlobName: "dumps/a.txt", wantExists: true},
		{name: "BlobNotFound means absent", prefix: "dumps/", fileName: "a.txt", getErr: blobNotFound(), wantBlobName: "dumps/a.txt", wantExists: false},
		{name: "other error wrapped", prefix: "dumps/", fileName: "a.txt", getErr: errors.New("boom"), wantBlobName: "dumps/a.txt", wantErrText: "error getting object info"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			bb := &mockBlockBlob{}
			bb.On("GetProperties", mock.Anything, mock.Anything).
				Return(blob.GetPropertiesResponse{}, tt.getErr)
			c := &mockContainer{}
			c.On("NewBlockBlobClient", mock.Anything).Return(bb)
			st := newMockStorage(t, tt.prefix, c)

			// Act
			exists, err := st.Exists(context.Background(), tt.fileName)

			// Assert
			assert.Equal(t, []string{tt.wantBlobName}, c.blobNames)
			if tt.wantErrText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
				assert.False(t, exists)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantExists, exists)
		})
	}
}

func TestStorage_Stat(t *testing.T) {
	modTime := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		prefix       string
		fileName     string
		getErr       error
		wantBlobName string
		wantErrText  string
	}{
		{name: "present builds full path name", prefix: "dumps/", fileName: "a.txt", wantBlobName: "dumps/a.txt"},
		{name: "name keeps prefix and is not stripped", prefix: "a/b/", fileName: "c.txt", wantBlobName: "a/b/c.txt"},
		{name: "not found is an error not Exist=false", prefix: "dumps/", fileName: "a.txt", getErr: blobNotFound(), wantBlobName: "dumps/a.txt", wantErrText: "error getting object info"},
		{name: "other error wrapped", prefix: "dumps/", fileName: "a.txt", getErr: errors.New("boom"), wantBlobName: "dumps/a.txt", wantErrText: "error getting object info"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			bb := &mockBlockBlob{}
			if tt.getErr != nil {
				bb.On("GetProperties", mock.Anything, mock.Anything).
					Return(blob.GetPropertiesResponse{}, tt.getErr)
			} else {
				bb.On("GetProperties", mock.Anything, mock.Anything).
					Return(blob.GetPropertiesResponse{LastModified: to.Ptr(modTime)}, nil)
			}
			c := &mockContainer{}
			c.On("NewBlockBlobClient", mock.Anything).Return(bb)
			st := newMockStorage(t, tt.prefix, c)

			// Act
			stat, err := st.Stat(tt.fileName)

			// Assert
			assert.Equal(t, []string{tt.wantBlobName}, c.blobNames)
			if tt.wantErrText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
				assert.Nil(t, stat)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, stat)
			assert.Equal(t, tt.wantBlobName, stat.Name)
			assert.True(t, stat.Exist)
			assert.True(t, stat.LastModified.Equal(modTime), "LastModified = %v", stat.LastModified)
		})
	}
}

func TestStorage_Delete(t *testing.T) {
	tests := []struct {
		name          string
		prefix        string
		paths         []string
		errByBlobName map[string]error
		wantBlobNames []string
		wantErrText   string
	}{
		{
			name:          "deletes each path with prefix",
			prefix:        "dumps/",
			paths:         []string{"a.txt", "b.txt"},
			wantBlobNames: []string{"dumps/a.txt", "dumps/b.txt"},
		},
		{
			name:          "trims leading slash",
			prefix:        "dumps/",
			paths:         []string{"/a.txt"},
			wantBlobNames: []string{"dumps/a.txt"},
		},
		{
			name:          "not found is skipped, not an error",
			prefix:        "dumps/",
			paths:         []string{"a.txt", "ghost.txt"},
			errByBlobName: map[string]error{"dumps/ghost.txt": blobNotFound()},
			wantBlobNames: []string{"dumps/a.txt", "dumps/ghost.txt"},
		},
		{
			name:          "other error wraps and stops",
			prefix:        "dumps/",
			paths:         []string{"a.txt", "b.txt", "c.txt"},
			errByBlobName: map[string]error{"dumps/b.txt": errors.New("boom")},
			wantBlobNames: []string{"dumps/a.txt", "dumps/b.txt"},
			wantErrText:   "error deleting object",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: one block-blob double per requested name, returning that
			// name's injected error (nil / BlobNotFound / other).
			c := &mockContainer{}
			for _, name := range tt.wantBlobNames {
				c.On("NewBlockBlobClient", name).Return(deleteBlob(tt.errByBlobName[name]))
			}
			st := newMockStorage(t, tt.prefix, c)

			// Act
			err := st.Delete(context.Background(), tt.paths...)

			// Assert
			assert.Equal(t, tt.wantBlobNames, c.blobNames)
			if tt.wantErrText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestStorage_Ping(t *testing.T) {
	tests := []struct {
		name    string
		getErr  error
		wantErr string
	}{
		{name: "reachable", getErr: nil},
		{name: "error wrapped", getErr: errors.New("no route"), wantErr: "error pinging azure container"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			c := &mockContainer{}
			c.On("GetProperties", mock.Anything, mock.Anything).
				Return(container.GetPropertiesResponse{}, tt.getErr)
			st := newMockStorage(t, "dumps/", c)

			// Act
			err := st.Ping(context.Background())

			// Assert
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestStorage_ListDir(t *testing.T) {
	t.Run("paginates, splits dirs from files, strips prefix", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewListBlobsHierarchyPager", mock.Anything, mock.Anything).Return(newFakeHierarchyPager(
			hierarchyPage([]string{"p/file1.txt"}, []string{"p/dirA/"}),
			hierarchyPage([]string{"p/file2.txt"}, []string{"p/dirB/"}),
		))
		st := newMockStorage(t, "p/", c)

		// Act
		files, dirs, err := st.ListDir(context.Background())

		// Assert
		require.NoError(t, err)
		assert.Equal(t, []string{"file1.txt", "file2.txt"}, files)
		require.Len(t, dirs, 2)
		assert.Equal(t, "p/dirA/", dirs[0].GetCwd())
		assert.Equal(t, "p/dirB/", dirs[1].GetCwd())
	})

	t.Run("error is wrapped", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewListBlobsHierarchyPager", mock.Anything, mock.Anything).
			Return(newErrorHierarchyPager(errors.New("boom")))
		st := newMockStorage(t, "p/", c)

		// Act
		_, _, err := st.ListDir(context.Background())

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error listing azure blobs")
	})
}

func TestStorage_DeleteAll(t *testing.T) {
	// Arrange: a two-level tree under "sub/". The hierarchy pager is matched
	// per-prefix so Walk recurses; every Delete goes through NewBlockBlobClient,
	// whose names are captured in order on c.blobNames.
	c := &mockContainer{}
	c.On("NewListBlobsHierarchyPager", mock.Anything, atPrefix("sub/")).Return(newFakeHierarchyPager(
		hierarchyPage([]string{"sub/f1.txt", "sub/f2.txt"}, []string{"sub/nested/"}),
	))
	c.On("NewListBlobsHierarchyPager", mock.Anything, atPrefix("sub/nested/")).Return(newFakeHierarchyPager(
		hierarchyPage([]string{"sub/nested/g.txt"}, nil),
	))
	c.On("NewBlockBlobClient", mock.Anything).Return(deleteBlob(nil))
	st := newMockStorage(t, "", c)

	// Act
	err := st.DeleteAll(context.Background(), "sub")

	// Assert
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"sub/f1.txt", "sub/f2.txt", "sub/nested/g.txt"},
		c.blobNames,
		"every walked key must be deleted with the sub-storage prefix re-applied",
	)
}

// ===========================================================================
// Integration (real Azurite emulator, behind -short)
//
// A single Azurite container is shared across all integration tests (started
// lazily on first use, terminated in TestMain). Each subtest gets its own
// freshly created blob container via newTestStorage, so cases stay isolated.
// ===========================================================================

const azuriteImage = "mcr.microsoft.com/azure-storage/azurite:latest"

var (
	azuriteOnce    sync.Once
	azuriteCtr     *azurite.Container
	azuriteBlobURL string
	azuriteErr     error

	containerCounter atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if azuriteCtr != nil {
		_ = azuriteCtr.Terminate(context.Background())
	}
	os.Exit(code)
}

// azuriteEndpoint lazily starts the shared Azurite container and returns the
// blob service URL. Integration tests are skipped under -short.
// --skipApiVersionCheck lets the emulator accept the API version sent by the
// current Azure SDK.
func azuriteEndpoint(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Azurite container test in short mode")
	}
	azuriteOnce.Do(func() {
		ctx := context.Background()
		azuriteCtr, azuriteErr = azurite.Run(
			ctx, azuriteImage,
			azurite.WithEnabledServices(azurite.BlobService),
			testcontainers.WithCmdArgs("--skipApiVersionCheck"),
		)
		if azuriteErr != nil {
			return
		}
		azuriteBlobURL, azuriteErr = azuriteCtr.BlobServiceURL(ctx)
	})
	require.NoError(t, azuriteErr)
	return azuriteBlobURL
}

// newTestStorage returns a Storage backed by a unique, freshly created blob
// container in the shared Azurite emulator.
func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	ctx := context.Background()
	endpoint := azuriteEndpoint(t)

	cfg := DefaultConfig()
	// Path-style endpoint includes the account name (Azurite well-known account).
	cfg.Endpoint = fmt.Sprintf("%s/%s", endpoint, azurite.AccountName)
	cfg.StorageAccount = azurite.AccountName
	cfg.AccessKey = azurite.AccountKey
	cfg.Container = fmt.Sprintf("test-%d", containerCounter.Add(1))

	st, err := NewStorage(ctx, cfg)
	require.NoError(t, err)
	_, err = st.containerClient.Create(ctx, nil)
	require.NoError(t, err)
	return st
}

// putObject is a small helper that writes content under key.
func putObject(t *testing.T, st *Storage, key string, content []byte) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), key, bytes.NewReader(content)))
}

// mustGet reads the object at key and returns its bytes.
func mustGet(t *testing.T, st *Storage, key string) []byte {
	t.Helper()
	r, err := st.GetObject(context.Background(), key)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

// rawListBlobs lists every blob in the container directly through the SDK,
// independent of the Storager implementation. It is used to cross-check that
// the storage actually holds (or no longer holds) the expected objects.
func rawListBlobs(t *testing.T, st *Storage) []string {
	t.Helper()
	ctx := context.Background()
	var names []string
	pager := st.containerClient.NewListBlobsFlatPager(nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, b := range page.Segment.BlobItems {
			names = append(names, *b.Name)
		}
	}
	return names
}

func dirNames(dirs []storages.Storager) []string {
	names := make([]string, 0, len(dirs))
	for _, d := range dirs {
		names = append(names, d.Dirname())
	}
	return names
}

func mapKeys(m map[string][]byte) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// TestStorage_Integration exercises every method against the real emulator.
func TestStorage_Integration(t *testing.T) {
	t.Run("PutObject", func(t *testing.T) {
		tests := []struct {
			name    string
			key     string
			content []byte
		}{
			{"root file", "file.txt", []byte("hello")},
			{"nested file", "dir/file.txt", []byte("nested")},
			{"deeply nested", "a/b/c/d.txt", []byte("deep")},
			{"leading slash key is trimmed", "/slash.txt", []byte("slash")},
			{"empty content", "empty.txt", []byte{}},
			{"binary content", "bin.dat", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := newTestStorage(t)
				putObject(t, st, tt.key, tt.content)
				assert.Equal(t, tt.content, mustGet(t, st, tt.key))
			})
		}

		t.Run("overwrite creates new version", func(t *testing.T) {
			st := newTestStorage(t)
			putObject(t, st, "v.txt", []byte("version-1"))
			putObject(t, st, "v.txt", []byte("version-2"))
			assert.Equal(t, []byte("version-2"), mustGet(t, st, "v.txt"))
			// the overwrite must not leave a duplicate blob behind
			assert.Equal(t, []string{"v.txt"}, rawListBlobs(t, st))
		})
	})

	t.Run("GetObject", func(t *testing.T) {
		tests := []struct {
			name        string
			putKey      string
			getKey      string
			wantContent []byte
			wantErr     error
		}{
			{"existing root", "f.txt", "f.txt", []byte("data"), nil},
			{"existing nested", "d/f.txt", "d/f.txt", []byte("nested"), nil},
			{"missing key", "", "missing.txt", nil, storages.ErrFileNotFound},
			{"leading slash matches put without slash", "f.txt", "/f.txt", []byte("data"), nil},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := newTestStorage(t)
				if tt.putKey != "" {
					putObject(t, st, tt.putKey, tt.wantContent)
				}
				reader, err := st.GetObject(context.Background(), tt.getKey)
				if tt.wantErr != nil {
					assert.ErrorIs(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
				defer func() { _ = reader.Close() }()
				data, err := io.ReadAll(reader)
				require.NoError(t, err)
				assert.Equal(t, tt.wantContent, data)
			})
		}
	})

	t.Run("Exists", func(t *testing.T) {
		tests := []struct {
			name     string
			put      []string
			checkKey string
			want     bool
		}{
			{"present root", []string{"a.txt"}, "a.txt", true},
			{"present nested", []string{"d/a.txt"}, "d/a.txt", true},
			{"absent", []string{"a.txt"}, "b.txt", false},
			{"absent in empty container", nil, "a.txt", false},
			{"prefix of existing key is not a blob", []string{"dir/a.txt"}, "dir", false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := newTestStorage(t)
				for _, k := range tt.put {
					putObject(t, st, k, []byte("x"))
				}
				got, err := st.Exists(context.Background(), tt.checkKey)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("Stat", func(t *testing.T) {
		tests := []struct {
			name    string
			putKey  string
			statKey string
			wantErr bool
		}{
			{"existing root", "f.txt", "f.txt", false},
			{"existing nested", "d/f.txt", "d/f.txt", false},
			{"missing", "", "missing.txt", true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := newTestStorage(t)
				if tt.putKey != "" {
					putObject(t, st, tt.putKey, []byte("data"))
				}
				stat, err := st.Stat(tt.statKey)
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.True(t, stat.Exist)
				assert.Equal(t, tt.statKey, stat.Name)
				assert.False(t, stat.LastModified.IsZero())
			})
		}
	})

	t.Run("ListDir", func(t *testing.T) {
		tests := []struct {
			name       string
			put        []string
			listPrefix string // "" lists the root storage, otherwise a relative SubStorage
			wantFiles  []string
			wantDirs   []string
		}{
			{
				name:      "mixed files and dirs at root",
				put:       []string{"a.txt", "b.txt", "d1/c.txt", "d2/e.txt"},
				wantFiles: []string{"a.txt", "b.txt"},
				wantDirs:  []string{"d1", "d2"},
			},
			{
				name:      "only files",
				put:       []string{"a.txt", "b.txt"},
				wantFiles: []string{"a.txt", "b.txt"},
				wantDirs:  nil,
			},
			{
				name:     "only dirs",
				put:      []string{"d1/a.txt", "d2/b.txt"},
				wantDirs: []string{"d1", "d2"},
			},
			{
				name: "empty container",
			},
			{
				name:       "nested listing via sub storage",
				put:        []string{"sub/x.txt", "sub/y.txt", "sub/deep/z.txt"},
				listPrefix: "sub",
				wantFiles:  []string{"x.txt", "y.txt"},
				wantDirs:   []string{"deep"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := newTestStorage(t)
				for _, k := range tt.put {
					putObject(t, st, k, []byte("x"))
				}

				var target storages.Storager = st
				if tt.listPrefix != "" {
					target = st.SubStorage(tt.listPrefix, true)
				}

				files, dirs, err := target.ListDir(context.Background())
				require.NoError(t, err)
				assert.ElementsMatch(t, tt.wantFiles, files)
				assert.ElementsMatch(t, tt.wantDirs, dirNames(dirs))
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tests := []struct {
			name     string
			put      []string
			del      []string
			wantGone []string
			wantKept []string
		}{
			{"single", []string{"a.txt", "b.txt"}, []string{"a.txt"}, []string{"a.txt"}, []string{"b.txt"}},
			{"multiple", []string{"a.txt", "b.txt", "c.txt"}, []string{"a.txt", "c.txt"}, []string{"a.txt", "c.txt"}, []string{"b.txt"}},
			{"non-existent is ignored", []string{"a.txt"}, []string{"ghost.txt"}, []string{"ghost.txt"}, []string{"a.txt"}},
			{"nested", []string{"d/a.txt", "d/b.txt"}, []string{"d/a.txt"}, []string{"d/a.txt"}, []string{"d/b.txt"}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := context.Background()
				st := newTestStorage(t)
				for _, k := range tt.put {
					putObject(t, st, k, []byte("x"))
				}

				require.NoError(t, st.Delete(ctx, tt.del...))

				for _, k := range tt.wantGone {
					exists, err := st.Exists(ctx, k)
					require.NoError(t, err)
					assert.Falsef(t, exists, "expected %q to be gone", k)
				}
				for _, k := range tt.wantKept {
					exists, err := st.Exists(ctx, k)
					require.NoError(t, err)
					assert.Truef(t, exists, "expected %q to be kept", k)
				}
			})
		}
	})

	t.Run("DeleteAll", func(t *testing.T) {
		tests := []struct {
			name          string
			put           []string
			deletePrefix  string
			wantRemaining []string
		}{
			{
				name:          "prefix isolation leaves other prefixes intact",
				put:           []string{"books/a.txt", "books/b.txt", "users/u.txt"},
				deletePrefix:  "books",
				wantRemaining: []string{"users/u.txt"},
			},
			{
				name:          "nested prefix only",
				put:           []string{"books/sci/a.txt", "books/sci/b.txt", "books/hist/c.txt"},
				deletePrefix:  "books/sci",
				wantRemaining: []string{"books/hist/c.txt"},
			},
			{
				name:          "similarly named prefix is not affected",
				put:           []string{"data/a.txt", "data2/b.txt"},
				deletePrefix:  "data",
				wantRemaining: []string{"data2/b.txt"},
			},
			{
				name:          "delete everything from root",
				put:           []string{"a.txt", "d/b.txt", "d/e/c.txt"},
				deletePrefix:  "/",
				wantRemaining: nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := newTestStorage(t)
				for _, k := range tt.put {
					putObject(t, st, k, []byte("x"))
				}

				require.NoError(t, st.DeleteAll(context.Background(), tt.deletePrefix))

				// Cross-check directly against the storage, not via ListDir.
				assert.ElementsMatch(t, tt.wantRemaining, rawListBlobs(t, st))
			})
		}
	})

	t.Run("SubStorage round-trip", func(t *testing.T) {
		st := newTestStorage(t)
		sub := st.SubStorage("Sub1", true)
		content := []byte("sub-storage-payload")
		require.NoError(t, sub.PutObject(context.Background(), "test.txt", bytes.NewReader(content)))

		// readable through the sub storage
		reader, err := sub.GetObject(context.Background(), "test.txt")
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, data)

		// and at the full path through the root storage
		assert.Equal(t, content, mustGet(t, st, "Sub1/test.txt"))
	})

	t.Run("Ping", func(t *testing.T) {
		st := newTestStorage(t)
		assert.NoError(t, st.Ping(context.Background()))
	})

	// Full lifecycle: create objects across independent prefixes, read them
	// back, overwrite (new version), partially delete, then DeleteAll one prefix
	// and verify the other prefix is untouched — cross-checked against storage.
	t.Run("lifecycle", func(t *testing.T) {
		ctx := context.Background()
		st := newTestStorage(t)

		objects := map[string][]byte{
			"books/fiction/dune.txt":        []byte("dune v1"),
			"books/fiction/neuromancer.txt": []byte("neuromancer"),
			"books/history/rome.txt":        []byte("rome"),
			"users/alice/profile.txt":       []byte("alice"),
			"users/bob/profile.txt":         []byte("bob"),
		}
		for k, v := range objects {
			putObject(t, st, k, v)
		}
		assert.ElementsMatch(t, mapKeys(objects), rawListBlobs(t, st))

		for k, v := range objects {
			exists, err := st.Exists(ctx, k)
			require.NoError(t, err)
			assert.Truef(t, exists, "expected %q to exist", k)
			assert.Equal(t, v, mustGet(t, st, k))
		}

		// new version: overwriting an existing object replaces its content in place
		putObject(t, st, "books/fiction/dune.txt", []byte("dune v2"))
		assert.Equal(t, []byte("dune v2"), mustGet(t, st, "books/fiction/dune.txt"))
		assert.ElementsMatch(t, mapKeys(objects), rawListBlobs(t, st), "overwrite must not add a blob")

		// partial delete: a single object goes, its sibling stays
		require.NoError(t, st.Delete(ctx, "books/fiction/neuromancer.txt"))
		exists, err := st.Exists(ctx, "books/fiction/neuromancer.txt")
		require.NoError(t, err)
		assert.False(t, exists)
		exists, err = st.Exists(ctx, "books/fiction/dune.txt")
		require.NoError(t, err)
		assert.True(t, exists)

		// DeleteAll on the books prefix must not touch the users prefix
		require.NoError(t, st.DeleteAll(ctx, "books"))
		assert.ElementsMatch(t, []string{
			"users/alice/profile.txt",
			"users/bob/profile.txt",
		}, rawListBlobs(t, st))
		assert.Equal(t, []byte("alice"), mustGet(t, st, "users/alice/profile.txt"))
		assert.Equal(t, []byte("bob"), mustGet(t, st, "users/bob/profile.txt"))

		// DeleteAll everything from the root empties the storage
		require.NoError(t, st.DeleteAll(ctx, "/"))
		assert.Empty(t, rawListBlobs(t, st))
		files, dirs, err := st.ListDir(ctx)
		require.NoError(t, err)
		assert.Empty(t, files)
		assert.Empty(t, dirs)
	})
}
