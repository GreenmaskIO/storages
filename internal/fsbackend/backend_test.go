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

package fsbackend

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
)

// newStorage is a factory for a backend rooted at a fresh, empty directory. Each
// implementation returns both the storage and its cwd.
type newStorage func(t *testing.T) (*Storage, string)

// backends returns one factory per afero.Fs implementation so every test runs
// against both the in-memory and the real on-disk filesystem, proving parity.
func backends() map[string]newStorage {
	return map[string]newStorage{
		"memmap": func(t *testing.T) (*Storage, string) {
			t.Helper()
			fs := afero.NewMemMapFs()
			cwd := "/root"
			require.NoError(t, fs.MkdirAll(cwd, DirMode))
			return New(fs, cwd), cwd
		},
		"osfs": func(t *testing.T) (*Storage, string) {
			t.Helper()
			cwd := t.TempDir()
			// The backend keeps cwd in its forward-slash convention, so the
			// expected cwd returned to the tests is normalized the same way —
			// otherwise every path assertion would be Windows-only noise.
			return New(afero.NewOsFs(), cwd), filepath.ToSlash(cwd)
		},
	}
}

// forEachBackend runs fn as a sub-test against every backend implementation.
func forEachBackend(t *testing.T, fn func(t *testing.T, mk newStorage)) {
	t.Helper()
	for name, mk := range backends() {
		t.Run(name, func(t *testing.T) { fn(t, mk) })
	}
}

func put(t *testing.T, s *Storage, p, data string) {
	t.Helper()
	require.NoError(t, s.PutObject(context.Background(), p, bytes.NewReader([]byte(data))))
}

func read(t *testing.T, s *Storage, p string) (string, error) {
	t.Helper()
	r, err := s.GetObject(context.Background(), p)
	if err != nil {
		return "", err
	}
	defer func() { require.NoError(t, r.Close()) }()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(b), nil
}

func TestNewAndCwd(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, cwd := mk(t)
		assert.Equal(t, cwd, s.GetCwd())
		assert.Equal(t, path.Base(cwd), s.Dirname())
	})
}

func TestPutAndGetObject(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "hello.txt", "hello world")

		got, err := read(t, s, "hello.txt")
		require.NoError(t, err)
		assert.Equal(t, "hello world", got)
	})
}

func TestPutObjectCreatesIntermediateDirs(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "a/b/c/deep.txt", "deep")

		got, err := read(t, s, "a/b/c/deep.txt")
		require.NoError(t, err)
		assert.Equal(t, "deep", got)

		files, dirs, err := s.ListDir(context.Background())
		require.NoError(t, err)
		assert.Empty(t, files)
		require.Len(t, dirs, 1)
		assert.Equal(t, "a", dirs[0].Dirname())
	})
}

func TestPutObjectOverwrites(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "f.txt", "first")
		put(t, s, "f.txt", "second")

		got, err := read(t, s, "f.txt")
		require.NoError(t, err)
		assert.Equal(t, "second", got)
	})
}

func TestGetObjectNotFound(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		_, err := s.GetObject(context.Background(), "missing.txt")
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})
}

func TestExists(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)

		ok, err := s.Exists(context.Background(), "f.txt")
		require.NoError(t, err)
		assert.False(t, ok)

		put(t, s, "f.txt", "data")

		ok, err = s.Exists(context.Background(), "f.txt")
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestDeleteFile(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "f.txt", "data")

		require.NoError(t, s.Delete(context.Background(), "f.txt"))

		ok, err := s.Exists(context.Background(), "f.txt")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestDeleteMissingIsAnError(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		err := s.Delete(context.Background(), "does-not-exist.txt")
		assert.ErrorIs(t, err, storages.ErrFileNotFound)

		var missing *storages.MissingObjectsError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, []string{"does-not-exist.txt"}, missing.Paths)
	})
}

func TestDeleteMultipleAndDir(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "a.txt", "a")
		put(t, s, "sub/b.txt", "b")
		put(t, s, "sub/c.txt", "c")

		// A mix of an existing file, a directory, and a missing entry. Delete
		// verifies everything up front, so the whole call fails and even the
		// valid "a.txt" is left alone. Both the directory and the absent entry
		// are reported, in the order they were passed.
		err := s.Delete(context.Background(), "a.txt", "sub", "missing.txt")
		assert.ErrorIs(t, err, storages.ErrFileNotFound)

		var missing *storages.MissingObjectsError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, []string{"sub", "missing.txt"}, missing.Paths)

		for _, p := range []string{"a.txt", "sub/b.txt", "sub/c.txt"} {
			ok, err := s.Exists(context.Background(), p)
			require.NoError(t, err)
			assert.Truef(t, ok, "expected %s to survive a failed Delete", p)
		}
	})
}

func TestDeleteAll(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "sub/one.txt", "1")
		put(t, s, "sub/two.txt", "2")
		put(t, s, "other/three.txt", "3")

		require.NoError(t, s.DeleteAll(context.Background(), "sub"))

		for _, p := range []string{"sub/one.txt", "sub/two.txt"} {
			ok, _ := s.Exists(context.Background(), p)
			assert.Falsef(t, ok, "expected %s to be gone", p)
		}
		ok, _ := s.Exists(context.Background(), "other/three.txt")
		assert.True(t, ok)
	})
}

func TestDeleteAllMissingIsAnError(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		assert.ErrorIs(t, s.DeleteAll(context.Background(), "nope"), storages.ErrFileNotFound)
	})
}

func TestListDir(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "file1.txt", "a")
		put(t, s, "file2.txt", "b")
		put(t, s, "dir1/nested.txt", "c")

		files, dirs, err := s.ListDir(context.Background())
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"file1.txt", "file2.txt"}, files)
		require.Len(t, dirs, 1)
		assert.Equal(t, "dir1", dirs[0].Dirname())
	})
}

func TestStat(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		put(t, s, "info.txt", "some-data")

		stat, err := s.Stat("info.txt")
		require.NoError(t, err)
		assert.True(t, stat.Exist)
		assert.WithinDuration(t, time.Now(), stat.LastModified, 5*time.Second)
	})
}

func TestStatMissing(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		stat, err := s.Stat("missing.txt")
		require.NoError(t, err)
		assert.False(t, stat.Exist)
	})
}

func TestSubStorageRelative(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, cwd := mk(t)
		sub := s.SubStorage("subdir", true).(*Storage)
		assert.Equal(t, path.Join(cwd, "subdir"), sub.GetCwd())

		require.NoError(t, sub.PutObject(context.Background(), "deep.txt", bytes.NewReader([]byte("deep"))))

		// The sub-storage sees the file at its own root...
		ok, err := sub.Exists(context.Background(), "deep.txt")
		require.NoError(t, err)
		assert.True(t, ok)

		// ...the parent sees it only under the sub-path, not at its own root.
		ok, err = s.Exists(context.Background(), "subdir/deep.txt")
		require.NoError(t, err)
		assert.True(t, ok)

		ok, err = s.Exists(context.Background(), "deep.txt")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestSubStorageAbsolute(t *testing.T) {
	// Absolute SubStorage roots at the given path directly. Use the in-memory fs
	// so the absolute path is well-defined and isolated.
	fs := afero.NewMemMapFs()
	require.NoError(t, fs.MkdirAll("/root", DirMode))
	s := New(fs, "/root")

	sub := s.SubStorage("/elsewhere", false).(*Storage)
	assert.Equal(t, "/elsewhere", sub.GetCwd())

	require.NoError(t, sub.PutObject(context.Background(), "x.txt", bytes.NewReader([]byte("x"))))

	ok, err := s.Exists(context.Background(), "x.txt")
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = sub.Exists(context.Background(), "x.txt")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestPing(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		assert.NoError(t, s.Ping(context.Background()))
	})
}

func TestClose(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)
		assert.NoError(t, s.Close())
	})
}

func TestNewWithLogger(t *testing.T) {
	s := New(afero.NewMemMapFs(), "/", WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	assert.NotNil(t, s.logger)
}

// readOnlyStorage returns a backend over a read-only fs pre-populated with a
// file and a directory, so write operations fail with a non-"not found" error.
func readOnlyStorage(t *testing.T) *Storage {
	t.Helper()
	base := afero.NewMemMapFs()
	require.NoError(t, base.MkdirAll("/root/sub", DirMode))
	require.NoError(t, afero.WriteFile(base, "/root/f.txt", []byte("x"), FileMode))
	return New(afero.NewReadOnlyFs(base), "/root")
}

func TestPutObjectWriteError(t *testing.T) {
	s := readOnlyStorage(t)
	err := s.PutObject(context.Background(), "new.txt", bytes.NewReader([]byte("data")))
	assert.Error(t, err)
}

func TestDeleteWriteError(t *testing.T) {
	s := readOnlyStorage(t)
	// The file exists (Stat succeeds) but Remove fails on the read-only fs.
	err := s.Delete(context.Background(), "f.txt")
	assert.Error(t, err)
}

func TestDeleteAllWriteError(t *testing.T) {
	s := readOnlyStorage(t)
	err := s.DeleteAll(context.Background(), "sub")
	assert.Error(t, err)
}

// blockingReader never returns from Read until unblocked, letting us exercise
// the context-cancellation path of PutObject deterministically.
type blockingReader struct {
	release chan struct{}
}

func (b *blockingReader) Read([]byte) (int, error) {
	<-b.release
	return 0, io.EOF
}

func TestPutObjectContextCancelled(t *testing.T) {
	forEachBackend(t, func(t *testing.T, mk newStorage) {
		s, _ := mk(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled

		r := &blockingReader{release: make(chan struct{})}
		defer close(r.release) // let the copy goroutine finish after the test

		err := s.PutObject(ctx, "f.txt", r)
		assert.ErrorIs(t, err, context.Canceled)
	})
}
