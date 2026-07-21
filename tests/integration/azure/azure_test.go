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

// Package azure exercises the azure backend end to end against the Azurite blob
// emulator in a container.
//
// A single Azurite container is shared across all tests (started lazily on first
// use, terminated in TestMain). Each subtest gets its own freshly created blob
// container via newTestStorage, so cases stay isolated.
package azure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/azurite"

	"github.com/greenmaskio/storages"
	azstorage "github.com/greenmaskio/storages/azure"
)

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
// blob service URL. Tests here are skipped under -short.
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

// accountEndpoint returns the path-style endpoint including the account name
// (Azurite serves the well-known development account).
func accountEndpoint(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s/%s", azuriteEndpoint(t), azurite.AccountName)
}

// testStorage pairs a Storage with the name of the blob container backing it,
// which the raw SDK cross-checks need and the backend does not expose.
type testStorage struct {
	*azstorage.Storage
	container string
}

// newTestStorage returns a Storage backed by a unique, freshly created blob
// container in the shared Azurite emulator.
func newTestStorage(t *testing.T) *testStorage {
	t.Helper()
	ctx := context.Background()

	name := fmt.Sprintf("test-%d", containerCounter.Add(1))

	// The blob container must exist before any object operations. The backend
	// exposes no container-management surface, so provision it with an
	// independent SDK client.
	_, err := rawContainerClient(t, name).Create(ctx, nil)
	require.NoError(t, err)

	cfg := azstorage.DefaultConfig()
	cfg.Endpoint = accountEndpoint(t)
	cfg.StorageAccount = azurite.AccountName
	cfg.AccessKey = azurite.AccountKey
	cfg.Container = name

	st, err := azstorage.NewStorage(ctx, cfg)
	require.NoError(t, err)
	return &testStorage{Storage: st, container: name}
}

// rawContainerClient builds an SDK container client that bypasses the storage
// backend entirely, for provisioning and for cross-checking real blob state.
func rawContainerClient(t *testing.T, name string) *container.Client {
	t.Helper()
	cred, err := container.NewSharedKeyCredential(azurite.AccountName, azurite.AccountKey)
	require.NoError(t, err)

	client, err := container.NewClientWithSharedKeyCredential(
		fmt.Sprintf("%s/%s", accountEndpoint(t), name), cred, nil,
	)
	require.NoError(t, err)
	return client
}

// putObject is a small helper that writes content under key.
func putObject(t *testing.T, st storages.Storager, key string, content []byte) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), key, bytes.NewReader(content)))
}

// mustGet reads the object at key and returns its bytes.
func mustGet(t *testing.T, st storages.Storager, key string) []byte {
	t.Helper()
	r, err := st.GetObject(context.Background(), key)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

// rawListBlobs lists every blob in the storage's container directly through the
// SDK, independent of the Storager implementation. It is used to cross-check
// that the storage actually holds (or no longer holds) the expected objects.
func rawListBlobs(t *testing.T, st *testStorage) []string {
	t.Helper()
	ctx := context.Background()
	var names []string
	pager := rawContainerClient(t, st.container).NewListBlobsFlatPager(nil)
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
			name       string
			putKey     string
			statKey    string
			wantAbsent bool
		}{
			{"existing root", "f.txt", "f.txt", false},
			{"existing nested", "d/f.txt", "d/f.txt", false},
			{"missing reports Exist=false, not an error", "", "missing.txt", true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := newTestStorage(t)
				if tt.putKey != "" {
					putObject(t, st, tt.putKey, []byte("data"))
				}
				stat, err := st.Stat(tt.statKey)
				require.NoError(t, err)
				require.NotNil(t, stat)
				assert.Equal(t, tt.statKey, stat.Name)
				if tt.wantAbsent {
					assert.False(t, stat.Exist)
					return
				}
				assert.True(t, stat.Exist)
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
			name        string
			put         []string
			del         []string
			wantMissing []string // non-nil => the call must fail and delete nothing
			wantGone    []string
			wantKept    []string
		}{
			{name: "single", put: []string{"a.txt", "b.txt"}, del: []string{"a.txt"}, wantGone: []string{"a.txt"}, wantKept: []string{"b.txt"}},
			{name: "multiple", put: []string{"a.txt", "b.txt", "c.txt"}, del: []string{"a.txt", "c.txt"}, wantGone: []string{"a.txt", "c.txt"}, wantKept: []string{"b.txt"}},
			{name: "non-existent is reported and nothing is deleted", put: []string{"a.txt"}, del: []string{"a.txt", "ghost.txt"}, wantMissing: []string{"ghost.txt"}, wantKept: []string{"a.txt"}},
			{name: "nested", put: []string{"d/a.txt", "d/b.txt"}, del: []string{"d/a.txt"}, wantGone: []string{"d/a.txt"}, wantKept: []string{"d/b.txt"}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := context.Background()
				st := newTestStorage(t)
				for _, k := range tt.put {
					putObject(t, st, k, []byte("x"))
				}

				err := st.Delete(ctx, tt.del...)
				if tt.wantMissing != nil {
					var missing *storages.MissingObjectsError
					require.ErrorAs(t, err, &missing)
					assert.Equal(t, tt.wantMissing, missing.Paths)
				} else {
					require.NoError(t, err)
				}

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
