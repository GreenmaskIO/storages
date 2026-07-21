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
// storages.Storager contract. Any backend — including one implemented outside
// this repository — can be validated against the same set of behavioral checks
// by passing a factory to Run, which is what keeps every backend's observable
// behavior in sync.
//
// The suite is used three ways here: the filesystem-like backends (directory
// over afero.OsFs, inmemory over afero.MemMapFs) run it on every supported OS,
// which doubles as cross-platform validation; and the s3, azure and ssh
// backends run it against real servers from the tests/integration module.
//
// The suite deliberately imports nothing beyond the standard library and
// storages itself, so importing it never compiles a test framework into a
// backend implementer's module or writes one to their vendor directory.
// (storages does require testify for its own unit tests, so it acts as a
// version floor in the importer's module graph — but no testify package is
// ever built on their side.)
package storagetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

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
		if err := st.PutObject(context.Background(), "test.txt", bytes.NewReader(content)); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
		if got := mustGet(t, st, "test.txt"); !bytes.Equal(got, content) {
			t.Errorf("GetObject = %q, want %q", got, content)
		}
	})

	t.Run("PutGetNestedCreatesDirs", func(t *testing.T) {
		st := newStorage(t)
		content := []byte("nested")
		if err := st.PutObject(context.Background(), "a/b/c.txt", bytes.NewReader(content)); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
		if got := mustGet(t, st, "a/b/c.txt"); !bytes.Equal(got, content) {
			t.Errorf("GetObject = %q, want %q", got, content)
		}
	})

	t.Run("GetMissingReturnsErrFileNotFound", func(t *testing.T) {
		st := newStorage(t)
		_, err := st.GetObject(context.Background(), "missing.txt")
		if !errors.Is(err, storages.ErrFileNotFound) {
			t.Errorf("GetObject error = %v, want ErrFileNotFound", err)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		st := newStorage(t)
		assertExists(t, st, "test.txt", false)
		put(t, st, "test.txt", "data")
		assertExists(t, st, "test.txt", true)
	})

	t.Run("StatMissing", func(t *testing.T) {
		st := newStorage(t)
		stat, err := st.Stat("missing.txt")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if stat.Exist {
			t.Error("Stat(missing).Exist = true, want false")
		}
	})

	t.Run("StatExisting", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "info.txt", "some-data")
		stat, err := st.Stat("info.txt")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if !stat.Exist {
			t.Error("Stat.Exist = false, want true")
		}
		if skew := time.Since(stat.LastModified).Abs(); skew > 10*time.Second {
			t.Errorf("Stat.LastModified is %v away from now, want within 10s", skew)
		}
	})

	t.Run("ListDirSplitsFilesAndDirs", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "file1.txt", "a")
		put(t, st, "dir1/file2.txt", "b")

		files, dirs, err := st.ListDir(context.Background())
		if err != nil {
			t.Fatalf("ListDir: %v", err)
		}
		if !slices.Contains(files, "file1.txt") {
			t.Errorf("ListDir files = %v, want to contain file1.txt", files)
		}
		if len(dirs) != 1 {
			t.Fatalf("ListDir returned %d dirs, want 1", len(dirs))
		}
		if got := dirs[0].Dirname(); got != "dir1" {
			t.Errorf("dirs[0].Dirname() = %q, want dir1", got)
		}
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
		if err := st.Delete(context.Background(), "to_delete.txt"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		assertExists(t, st, "to_delete.txt", false)
	})

	// Delete is object-level, never recursive. A directory is not an object — on
	// an object store the path simply does not resolve to a key — so it is
	// reported as not found and the sub-tree is left alone. DeleteAll is the
	// recursive operation.
	t.Run("DeleteDirectoryIsAnError", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "dir/x.txt", "data")
		if err := st.Delete(context.Background(), "dir"); !errors.Is(err, storages.ErrFileNotFound) {
			t.Fatalf("Delete(dir) = %v, want ErrFileNotFound", err)
		}
		assertExists(t, st, "dir/x.txt", true)
	})

	t.Run("DeleteMissingIsAnError", func(t *testing.T) {
		st := newStorage(t)
		err := st.Delete(context.Background(), "never_existed.txt")
		if !errors.Is(err, storages.ErrFileNotFound) {
			t.Fatalf("Delete(missing) = %v, want ErrFileNotFound", err)
		}
		var missing *storages.MissingObjectsError
		if !errors.As(err, &missing) {
			t.Fatalf("Delete(missing) error is %T, want *MissingObjectsError", err)
		}
		if len(missing.Paths) != 1 || missing.Paths[0] != "never_existed.txt" {
			t.Errorf("Paths = %q, want [never_existed.txt]", missing.Paths)
		}
	})

	// Delete verifies every path before removing anything, so one bad path
	// leaves the storage untouched rather than partly deleted.
	t.Run("DeleteWithOneMissingDeletesNothing", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "a.txt", "a")
		put(t, st, "c.txt", "c")

		err := st.Delete(context.Background(), "a.txt", "b.txt", "c.txt")
		if !errors.Is(err, storages.ErrFileNotFound) {
			t.Fatalf("Delete = %v, want ErrFileNotFound", err)
		}
		var missing *storages.MissingObjectsError
		if errors.As(err, &missing) {
			if len(missing.Paths) != 1 || missing.Paths[0] != "b.txt" {
				t.Errorf("Paths = %q, want [b.txt]", missing.Paths)
			}
		}
		assertExists(t, st, "a.txt", true)
		assertExists(t, st, "c.txt", true)
	})

	t.Run("DeleteAllPrefixIsolation", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "data/one.txt", "1")
		put(t, st, "data/two.txt", "2")
		put(t, st, "data2/three.txt", "3")

		if err := st.DeleteAll(context.Background(), "data"); err != nil {
			t.Fatalf("DeleteAll: %v", err)
		}

		assertExists(t, st, "data/one.txt", false)
		assertExists(t, st, "data/two.txt", false)
		// The "data" prefix must not swallow the sibling "data2".
		assertExists(t, st, "data2/three.txt", true)
	})

	// Removing a prefix that holds nothing is an error, the same rule Delete
	// follows. Note this makes DeleteAll non-idempotent: a retried or re-run
	// deletion fails the second time because the target is already gone.
	t.Run("DeleteAllMissingPrefixIsAnError", func(t *testing.T) {
		st := newStorage(t)
		if err := st.DeleteAll(context.Background(), "never_existed"); !errors.Is(err, storages.ErrFileNotFound) {
			t.Errorf("DeleteAll(missing) = %v, want ErrFileNotFound", err)
		}

		// Also on a storage that holds unrelated objects, not just an empty one.
		put(t, st, "kept/a.txt", "a")
		if err := st.DeleteAll(context.Background(), "still_missing"); !errors.Is(err, storages.ErrFileNotFound) {
			t.Errorf("DeleteAll(missing) = %v, want ErrFileNotFound", err)
		}
		assertExists(t, st, "kept/a.txt", true)
	})

	t.Run("Ping", func(t *testing.T) {
		st := newStorage(t)
		if err := st.Ping(context.Background()); err != nil {
			t.Errorf("Ping: %v", err)
		}
	})

	t.Run("Close", func(t *testing.T) {
		st := newStorage(t)
		if err := st.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

func put(t *testing.T, st storages.Storager, name, content string) {
	t.Helper()
	if err := st.PutObject(context.Background(), name, bytes.NewReader([]byte(content))); err != nil {
		t.Fatalf("PutObject(%q): %v", name, err)
	}
}

func mustGet(t *testing.T, st storages.Storager, name string) []byte {
	t.Helper()
	r, err := st.GetObject(context.Background(), name)
	if err != nil {
		t.Fatalf("GetObject(%q): %v", name, err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Close reader for %q: %v", name, err)
		}
	}()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read %q: %v", name, err)
	}
	return data
}

func assertExists(t *testing.T, st storages.Storager, name string, want bool) {
	t.Helper()
	ok, err := st.Exists(context.Background(), name)
	if err != nil {
		t.Fatalf("Exists(%q): %v", name, err)
	}
	if ok != want {
		t.Errorf("Exists(%q) = %v, want %v", name, ok, want)
	}
}
