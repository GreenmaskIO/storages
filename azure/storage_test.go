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

// The end-to-end tests against the Azurite emulator live in the tests/integration
// module, which keeps testcontainers out of this module's dependency graph. What
// stays here drives the containerAPI/blockBlobAPI seams with test doubles.
package azure

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
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

// devAccountName / devAccountKey are the well-known Azure Storage development
// account credentials. Nothing here talks to a server; they are only used to
// build clients.
const (
	devAccountName = "devstoreaccount1"
	devAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// TestNewStorage_AuthDispatch verifies that every auth method builds a usable
// container client (no network calls are made by client construction).
func TestNewStorage_AuthDispatch(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
	}{
		{"access key", func(c *Config) { c.AccessKey = devAccountKey }},
		{"sas token", func(c *Config) { c.SASToken = "sig=abc&se=2030-01-01" }},
		{"default credential chain", func(c *Config) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.StorageAccount = devAccountName
			cfg.Container = "cont"
			cfg.Endpoint = "http://127.0.0.1:10000/devstoreaccount1"
			tt.configure(&cfg)

			// WithUnsafe yields the bare backend, whose container client is what
			// this test is about; the guard wrapper does not expose it.
			st, err := New(context.Background(), cfg, WithUnsafe())
			require.NoError(t, err)
			require.NotNil(t, st)
			raw, ok := st.(*Storage)
			require.True(t, ok, "WithUnsafe must yield the bare backend")
			assert.NotNil(t, raw.containerClient)
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
	cfg.StorageAccount = devAccountName
	cfg.Container = "cont"
	cfg.AccessKey = devAccountKey
	cfg.Endpoint = "http://127.0.0.1:1" // never dialled
	cfg.Prefix = "dumps"

	st, err := New(ctx, cfg)
	require.NoError(t, err)

	_, err = st.GetObject(ctx, "../../escape.txt")
	assert.ErrorIs(t, err, storages.ErrUnsafeKey)
	assert.ErrorIs(t, st.DeleteAll(ctx, ""), storages.ErrUnsafeKey)
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
			sub, err := st.SubStorage(tt.sub, tt.relative)
			require.NoError(t, err)
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

// deleteBlob returns a block-blob double that exists (GetProperties succeeds)
// and whose Delete yields the given error.
func deleteBlob(err error) *mockBlockBlob {
	b := &mockBlockBlob{}
	b.On("GetProperties", mock.Anything, mock.Anything).Return(blob.GetPropertiesResponse{}, nil)
	b.On("Delete", mock.Anything, mock.Anything).Return(blob.DeleteResponse{}, err)
	return b
}

// probeBlob returns a block-blob double whose GetProperties yields the given
// error, for exercising Delete's verification pass. Its Delete must never be
// called: a failed verification deletes nothing.
func probeBlob(err error) *mockBlockBlob {
	b := &mockBlockBlob{}
	b.On("GetProperties", mock.Anything, mock.Anything).Return(blob.GetPropertiesResponse{}, err)
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
	o *container.ListBlobsFlatOptions,
) *runtime.Pager[container.ListBlobsFlatResponse] {
	args := m.Called(o)
	return args.Get(0).(*runtime.Pager[container.ListBlobsFlatResponse])
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

// atFlatPrefix matches a flat-pager options argument by its Prefix.
func atFlatPrefix(prefix string) any {
	return mock.MatchedBy(func(o *container.ListBlobsFlatOptions) bool {
		return o != nil && o.Prefix != nil && *o.Prefix == prefix
	})
}

// flatBlob describes one entry of a flat listing.
type flatBlob struct {
	name string
	size int64
}

// flatPage builds one page of a flat listing.
func flatPage(blobs ...flatBlob) container.ListBlobsFlatResponse {
	seg := &container.BlobFlatListSegment{}
	for _, b := range blobs {
		seg.BlobItems = append(seg.BlobItems, &container.BlobItem{
			Name: to.Ptr(b.name),
			Properties: &container.BlobProperties{
				ContentLength: to.Ptr(b.size),
				LastModified:  to.Ptr(flatListModTime),
			},
		})
	}
	return container.ListBlobsFlatResponse{
		ListBlobsFlatSegmentResponse: container.ListBlobsFlatSegmentResponse{Segment: seg},
	}
}

// flatListModTime is the LastModified every flatPage entry carries.
var flatListModTime = time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)

// newFakeFlatPager returns a pager that yields the given pages in order.
func newFakeFlatPager(pages ...container.ListBlobsFlatResponse) *runtime.Pager[container.ListBlobsFlatResponse] {
	idx := 0
	return runtime.NewPager(runtime.PagingHandler[container.ListBlobsFlatResponse]{
		More: func(container.ListBlobsFlatResponse) bool { return idx < len(pages) },
		Fetcher: func(context.Context, *container.ListBlobsFlatResponse) (container.ListBlobsFlatResponse, error) {
			p := pages[idx]
			idx++
			return p, nil
		},
	})
}

// newErrorFlatPager returns a flat pager whose first fetch fails.
func newErrorFlatPager(err error) *runtime.Pager[container.ListBlobsFlatResponse] {
	return runtime.NewPager(runtime.PagingHandler[container.ListBlobsFlatResponse]{
		More: func(container.ListBlobsFlatResponse) bool { return true },
		Fetcher: func(context.Context, *container.ListBlobsFlatResponse) (container.ListBlobsFlatResponse, error) {
			return container.ListBlobsFlatResponse{}, err
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

// invalidRange builds an error that bloberror.HasCode recognizes as InvalidRange,
// which is how Azure answers a range it cannot satisfy.
func invalidRange() error {
	return &azcore.ResponseError{ErrorCode: string(bloberror.InvalidRange)}
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

func TestStorage_GetObjectRange(t *testing.T) {
	// downloadRanges returns the HTTPRange of every DownloadStream call, in order.
	downloadRanges := func(bb *mockBlockBlob) []blob.HTTPRange {
		var ranges []blob.HTTPRange
		for _, c := range bb.Calls {
			if c.Method != "DownloadStream" {
				continue
			}
			o, _ := c.Arguments.Get(1).(*blob.DownloadStreamOptions)
			require.NotNil(t, o, "DownloadStream must be given range options")
			ranges = append(ranges, o.Range)
		}
		return ranges
	}

	tests := []struct {
		name        string
		offset      int64
		length      int64
		downloadErr error
		wantRange   blob.HTTPRange
		wantErrIs   error
		wantErrText string
	}{
		{name: "bounded range", offset: 5, length: 4, wantRange: blob.HTTPRange{Offset: 5, Count: 4}},
		// Azure spells "to the end" as Count 0 (blob.CountToEnd).
		{name: "negative length reads to the end", offset: 15, length: -1, wantRange: blob.HTTPRange{Offset: 15, Count: blob.CountToEnd}},
		{name: "InvalidRange maps to ErrInvalidRange", offset: 0, length: 4, downloadErr: invalidRange(), wantRange: blob.HTTPRange{Offset: 0, Count: 4}, wantErrIs: storages.ErrInvalidRange},
		{name: "BlobNotFound maps to ErrFileNotFound", offset: 0, length: 4, downloadErr: blobNotFound(), wantRange: blob.HTTPRange{Offset: 0, Count: 4}, wantErrIs: storages.ErrFileNotFound},
		{name: "other error wrapped", offset: 0, length: 4, downloadErr: errors.New("network"), wantRange: blob.HTTPRange{Offset: 0, Count: 4}, wantErrText: "error getting object range"},
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
					DownloadResponse: blob.DownloadResponse{Body: io.NopCloser(strings.NewReader("chunk"))},
				}, nil)
			}
			c := &mockContainer{}
			c.On("NewBlockBlobClient", mock.Anything).Return(bb)
			st := newMockStorage(t, "dumps/", c)

			// Act
			reader, err := st.GetObjectRange(context.Background(), "a.txt", tt.offset, tt.length)

			// Assert
			assert.Equal(t, []string{"dumps/a.txt"}, c.blobNames)
			assert.Equal(t, []blob.HTTPRange{tt.wantRange}, downloadRanges(bb),
				"the server must be asked for exactly the requested bytes")
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
		})
	}
}

// Azure answers an offset sitting exactly at the end of a blob with an empty
// 200 rather than InvalidRange, so the backend has to recognize that shape
// itself — otherwise the caller would see a silently empty read where every
// other backend reports ErrInvalidRange.
func TestStorage_GetObjectRange_EmptyResponseIsInvalidRange(t *testing.T) {
	// Arrange
	body := &closeTrackingReader{Reader: strings.NewReader("")}
	bb := &mockBlockBlob{}
	bb.On("DownloadStream", mock.Anything, mock.Anything).Return(blob.DownloadStreamResponse{
		DownloadResponse: blob.DownloadResponse{Body: body, ContentLength: to.Ptr(int64(0))},
	}, nil)
	c := &mockContainer{}
	c.On("NewBlockBlobClient", mock.Anything).Return(bb)
	st := newMockStorage(t, "dumps/", c)

	// Act
	reader, err := st.GetObjectRange(context.Background(), "a.txt", 20, 4)

	// Assert
	assert.ErrorIs(t, err, storages.ErrInvalidRange)
	assert.Nil(t, reader)
	assert.True(t, body.closed, "the response body must not be leaked when the range is rejected")
}

// closeTrackingReader records whether it was closed.
type closeTrackingReader struct {
	io.Reader
	closed bool
}

func (r *closeTrackingReader) Close() error {
	r.closed = true
	return nil
}

// A range that cannot yield anything is a caller bug the backend rejects on the
// spot; no request is worth sending.
func TestStorage_GetObjectRange_RejectsImpossibleRangeWithoutCallingAzure(t *testing.T) {
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
			c := &mockContainer{}
			st := newMockStorage(t, "dumps/", c)

			// Act
			reader, err := st.GetObjectRange(context.Background(), "a.txt", tt.offset, tt.length)

			// Assert
			assert.ErrorIs(t, err, storages.ErrInvalidRange)
			assert.Nil(t, reader)
			assert.Empty(t, c.Calls, "an impossible range must not reach Azure")
		})
	}
}

func TestStorage_List(t *testing.T) {
	t.Run("paginates, strips prefix, carries sizes", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewListBlobsFlatPager", atFlatPrefix("p/")).Return(newFakeFlatPager(
			flatPage(flatBlob{name: "p/a.txt", size: 1}),
			flatPage(flatBlob{name: "p/dir/b.txt", size: 22}),
		))
		st := newMockStorage(t, "p/", c)

		// Act
		objects, err := st.List(context.Background(), "")

		// Assert
		require.NoError(t, err)
		require.Len(t, objects, 2)
		assert.Equal(t, "a.txt", objects[0].Name)
		assert.Equal(t, int64(1), objects[0].Size)
		assert.True(t, objects[0].Exist)
		assert.True(t, objects[0].LastModified.Equal(flatListModTime))
		assert.Equal(t, "dir/b.txt", objects[1].Name)
		assert.Equal(t, int64(22), objects[1].Size)
		c.AssertExpectations(t)
	})

	// The trailing slash is what stops "data" from also matching "database".
	t.Run("prefix is directory-like", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewListBlobsFlatPager", atFlatPrefix("p/data/")).
			Return(newFakeFlatPager(flatPage(flatBlob{name: "p/data/one.txt", size: 1})))
		st := newMockStorage(t, "p/", c)

		// Act
		objects, err := st.List(context.Background(), "data")

		// Assert
		require.NoError(t, err)
		require.Len(t, objects, 1)
		assert.Equal(t, "one.txt", objects[0].Name)
		c.AssertExpectations(t)
	})

	t.Run("a prefix holding nothing is an empty listing, not an error", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewListBlobsFlatPager", mock.Anything).Return(newFakeFlatPager(flatPage()))
		st := newMockStorage(t, "p/", c)

		// Act
		objects, err := st.List(context.Background(), "never_existed")

		// Assert
		require.NoError(t, err)
		assert.Empty(t, objects)
	})

	t.Run("error is wrapped", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewListBlobsFlatPager", mock.Anything).Return(newErrorFlatPager(errors.New("boom")))
		st := newMockStorage(t, "p/", c)

		// Act
		_, err := st.List(context.Background(), "")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error listing azure blobs")
	})
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
		wantAbsent   bool
		wantErrText  string
	}{
		{name: "present builds full path name", prefix: "dumps/", fileName: "a.txt", wantBlobName: "dumps/a.txt"},
		{name: "name keeps prefix and is not stripped", prefix: "a/b/", fileName: "c.txt", wantBlobName: "a/b/c.txt"},
		{name: "BlobNotFound reports Exist=false, not an error", prefix: "dumps/", fileName: "a.txt", getErr: blobNotFound(), wantBlobName: "dumps/a.txt", wantAbsent: true},
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
					Return(blob.GetPropertiesResponse{LastModified: to.Ptr(modTime), ContentLength: to.Ptr(int64(42))}, nil)
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
			if tt.wantAbsent {
				assert.False(t, stat.Exist)
				return
			}
			assert.True(t, stat.Exist)
			assert.True(t, stat.LastModified.Equal(modTime), "LastModified = %v", stat.LastModified)
			assert.Equal(t, int64(42), stat.Size, "Size must come from the blob's Content-Length")
		})
	}
}

func TestStorage_Delete(t *testing.T) {
	// Delete verifies every blob before removing any, so each requested path is
	// resolved twice on the happy path: once by the verification pass and once
	// by the delete pass.
	t.Run("deletes each path with prefix", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		for _, name := range []string{"dumps/a.txt", "dumps/b.txt"} {
			c.On("NewBlockBlobClient", name).Return(deleteBlob(nil))
		}
		st := newMockStorage(t, "dumps/", c)

		// Act
		err := st.Delete(context.Background(), "a.txt", "b.txt")

		// Assert
		require.NoError(t, err)
		assert.Equal(t,
			[]string{"dumps/a.txt", "dumps/b.txt", "dumps/a.txt", "dumps/b.txt"},
			c.blobNames,
			"verification pass then delete pass",
		)
	})

	t.Run("trims leading slash", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewBlockBlobClient", "dumps/a.txt").Return(deleteBlob(nil))
		st := newMockStorage(t, "dumps/", c)

		// Act & Assert
		require.NoError(t, st.Delete(context.Background(), "/a.txt"))
		assert.Equal(t, []string{"dumps/a.txt", "dumps/a.txt"}, c.blobNames)
	})

	t.Run("missing blob deletes nothing and is reported", func(t *testing.T) {
		// Arrange: "a.txt" is present, "ghost.txt" is not. The doubles for both
		// would panic if Delete were called, since only GetProperties is set up
		// on the probe and the verification pass must abort before deleting.
		c := &mockContainer{}
		c.On("NewBlockBlobClient", "dumps/a.txt").Return(probeBlob(nil))
		c.On("NewBlockBlobClient", "dumps/ghost.txt").Return(probeBlob(blobNotFound()))
		st := newMockStorage(t, "dumps/", c)

		// Act
		err := st.Delete(context.Background(), "a.txt", "ghost.txt")

		// Assert
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
		var missing *storages.MissingObjectsError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, []string{"ghost.txt"}, missing.Paths)
		assert.Equal(t,
			[]string{"dumps/a.txt", "dumps/ghost.txt"}, c.blobNames,
			"only the verification pass runs; nothing is deleted",
		)
	})

	t.Run("verification error other than not-found is wrapped", func(t *testing.T) {
		// Arrange
		c := &mockContainer{}
		c.On("NewBlockBlobClient", "dumps/a.txt").Return(probeBlob(nil))
		c.On("NewBlockBlobClient", "dumps/b.txt").Return(probeBlob(errors.New("boom")))
		st := newMockStorage(t, "dumps/", c)

		// Act
		err := st.Delete(context.Background(), "a.txt", "b.txt")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error checking object")
		assert.NotErrorIs(t, err, storages.ErrFileNotFound)
	})

	t.Run("delete error is wrapped and stops", func(t *testing.T) {
		// Arrange: both verify fine, but removing "b.txt" fails.
		c := &mockContainer{}
		c.On("NewBlockBlobClient", "dumps/a.txt").Return(deleteBlob(nil))
		c.On("NewBlockBlobClient", "dumps/b.txt").Return(deleteBlob(errors.New("boom")))
		c.On("NewBlockBlobClient", "dumps/c.txt").Return(deleteBlob(nil))
		st := newMockStorage(t, "dumps/", c)

		// Act
		err := st.Delete(context.Background(), "a.txt", "b.txt", "c.txt")

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error deleting object")
		assert.Equal(t,
			[]string{"dumps/a.txt", "dumps/b.txt", "dumps/c.txt", "dumps/a.txt", "dumps/b.txt"},
			c.blobNames,
			"all three verified, then deletion stops at the failing blob",
		)
	})
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
