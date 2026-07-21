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

// Package storagetest provides a shared conformance suite exercising the
// storages.Storager contract. Any backend can be validated against the same set
// of behavioral checks by passing a factory to Run, which keeps the backends'
// observable behavior in sync. It currently drives the filesystem-like backends
// (directory over afero.OsFs, inmemory over afero.MemMapFs); running it on all
// three supported OSes doubles as cross-platform validation.
package storagetest

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
)

// Run executes the full Storager conformance suite against the backend produced
// by newStorage. The factory must return a fresh, empty, writable storage on
// every call so subtests stay isolated.
func Run(t *testing.T, newStorage func(t *testing.T) storages.Storager) {
	t.Helper()

	t.Run("PutGetRoundTrip", func(t *testing.T) {
		st := newStorage(t)
		content := []byte("hello world")
		require.NoError(t, st.PutObject(context.Background(), "test.txt", bytes.NewReader(content)))
		assert.Equal(t, content, mustGet(t, st, "test.txt"))
	})

	t.Run("PutGetNestedCreatesDirs", func(t *testing.T) {
		st := newStorage(t)
		content := []byte("nested")
		require.NoError(t, st.PutObject(context.Background(), "a/b/c.txt", bytes.NewReader(content)))
		assert.Equal(t, content, mustGet(t, st, "a/b/c.txt"))
	})

	t.Run("GetMissingReturnsErrFileNotFound", func(t *testing.T) {
		st := newStorage(t)
		_, err := st.GetObject(context.Background(), "missing.txt")
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})

	t.Run("Exists", func(t *testing.T) {
		st := newStorage(t)
		ok, err := st.Exists(context.Background(), "test.txt")
		require.NoError(t, err)
		assert.False(t, ok)

		put(t, st, "test.txt", "data")

		ok, err = st.Exists(context.Background(), "test.txt")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("StatMissing", func(t *testing.T) {
		st := newStorage(t)
		stat, err := st.Stat("missing.txt")
		require.NoError(t, err)
		assert.False(t, stat.Exist)
	})

	t.Run("StatExisting", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "info.txt", "some-data")
		stat, err := st.Stat("info.txt")
		require.NoError(t, err)
		assert.True(t, stat.Exist)
		assert.WithinDuration(t, time.Now(), stat.LastModified, 10*time.Second)
	})

	t.Run("ListDirSplitsFilesAndDirs", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "file1.txt", "a")
		put(t, st, "dir1/file2.txt", "b")

		files, dirs, err := st.ListDir(context.Background())
		require.NoError(t, err)
		assert.Contains(t, files, "file1.txt")
		require.Len(t, dirs, 1)
		assert.Equal(t, "dir1", dirs[0].Dirname())
	})

	t.Run("SubStorageRelative", func(t *testing.T) {
		st := newStorage(t)
		sub := st.SubStorage("subdir", true)
		put(t, sub, "deep.txt", "deep-data")

		// Paths are relative to each storage's cwd: the sub-storage sees the file
		// at its root, the parent sees it under the sub-path but not at its own root.
		assertExists(t, sub, "deep.txt", true)
		assertExists(t, st, "subdir/deep.txt", true)
		assertExists(t, st, "deep.txt", false)
	})

	t.Run("DeleteFile", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "to_delete.txt", "data")
		require.NoError(t, st.Delete(context.Background(), "to_delete.txt"))
		assertExists(t, st, "to_delete.txt", false)
	})

	t.Run("DeleteDirectory", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "dir/x.txt", "data")
		require.NoError(t, st.Delete(context.Background(), "dir"))
		assertExists(t, st, "dir/x.txt", false)
	})

	t.Run("DeleteMissingIsNotError", func(t *testing.T) {
		st := newStorage(t)
		assert.NoError(t, st.Delete(context.Background(), "never_existed.txt"))
	})

	t.Run("DeleteAllPrefixIsolation", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "data/one.txt", "1")
		put(t, st, "data/two.txt", "2")
		put(t, st, "data2/three.txt", "3")

		require.NoError(t, st.DeleteAll(context.Background(), "data"))

		assertExists(t, st, "data/one.txt", false)
		assertExists(t, st, "data/two.txt", false)
		// The "data" prefix must not swallow the sibling "data2".
		assertExists(t, st, "data2/three.txt", true)
	})

	t.Run("Ping", func(t *testing.T) {
		st := newStorage(t)
		assert.NoError(t, st.Ping(context.Background()))
	})

	t.Run("Close", func(t *testing.T) {
		st := newStorage(t)
		assert.NoError(t, st.Close())
	})
}

func put(t *testing.T, st storages.Storager, name, content string) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), name, bytes.NewReader([]byte(content))))
}

func mustGet(t *testing.T, st storages.Storager, name string) []byte {
	t.Helper()
	r, err := st.GetObject(context.Background(), name)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

func assertExists(t *testing.T, st storages.Storager, name string, want bool) {
	t.Helper()
	ok, err := st.Exists(context.Background(), name)
	require.NoError(t, err)
	assert.Equal(t, want, ok, "Exists(%q)", name)
}
