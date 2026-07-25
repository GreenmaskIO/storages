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
//
// The storage must be guarded — built the way its own constructor builds one,
// which for every backend here means storages.Guard is applied unless the
// caller opted out. The Guard* cases below hold it to that: a backend built
// without the guard fails them, since a key escaping the storage would reach
// the backend and be resolved.
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

	t.Run("StatReportsSize", func(t *testing.T) {
		st := newStorage(t)
		content := "0123456789"
		put(t, st, "sized.txt", content)
		stat, err := st.Stat("sized.txt")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if stat.Size != int64(len(content)) {
			t.Errorf("Stat.Size = %d, want %d", stat.Size, len(content))
		}
	})

	// rangeContent is the object every GetObjectRange case reads from. Its bytes
	// are all distinct, so a range that comes back shifted or truncated is
	// visible in the failure message rather than silently matching.
	const rangeContent = "0123456789abcdefghij"

	t.Run("GetObjectRangeInside", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "ranged.txt", rangeContent)
		if got := mustGetRange(t, st, "ranged.txt", 5, 4); string(got) != "5678" {
			t.Errorf("GetObjectRange(5, 4) = %q, want %q", got, "5678")
		}
	})

	// A negative length means "to the end of the object".
	t.Run("GetObjectRangeToEOF", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "ranged.txt", rangeContent)
		if got := mustGetRange(t, st, "ranged.txt", 15, -1); string(got) != "fghij" {
			t.Errorf("GetObjectRange(15, -1) = %q, want %q", got, "fghij")
		}
	})

	// A range that runs past the end is clamped, not rejected: the reader just
	// ends early.
	t.Run("GetObjectRangeCrossingEOF", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "ranged.txt", rangeContent)
		if got := mustGetRange(t, st, "ranged.txt", 16, 100); string(got) != "ghij" {
			t.Errorf("GetObjectRange(16, 100) = %q, want %q", got, "ghij")
		}
	})

	// An offset with no bytes behind it cannot be clamped into anything, so it is
	// rejected the way HTTP answers 416.
	t.Run("GetObjectRangeOffsetAtOrPastSize", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "ranged.txt", rangeContent)
		for _, offset := range []int64{int64(len(rangeContent)), int64(len(rangeContent)) + 100} {
			_, err := st.GetObjectRange(context.Background(), "ranged.txt", offset, 1)
			if !errors.Is(err, storages.ErrInvalidRange) {
				t.Errorf("GetObjectRange(%d, 1) error = %v, want ErrInvalidRange", offset, err)
			}
		}
	})

	t.Run("GetObjectRangeZeroLength", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "ranged.txt", rangeContent)
		_, err := st.GetObjectRange(context.Background(), "ranged.txt", 0, 0)
		if !errors.Is(err, storages.ErrInvalidRange) {
			t.Errorf("GetObjectRange(0, 0) error = %v, want ErrInvalidRange", err)
		}
	})

	t.Run("GetObjectRangeNegativeOffset", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "ranged.txt", rangeContent)
		_, err := st.GetObjectRange(context.Background(), "ranged.txt", -1, 4)
		if !errors.Is(err, storages.ErrInvalidRange) {
			t.Errorf("GetObjectRange(-1, 4) error = %v, want ErrInvalidRange", err)
		}
	})

	t.Run("GetObjectRangeMissing", func(t *testing.T) {
		st := newStorage(t)
		_, err := st.GetObjectRange(context.Background(), "missing.txt", 0, 4)
		if !errors.Is(err, storages.ErrFileNotFound) {
			t.Errorf("GetObjectRange(missing) error = %v, want ErrFileNotFound", err)
		}
	})

	// List is flat and recursive: names are relative to the prefix, slash
	// separated and lexicographically sorted, with the size of each object.
	t.Run("ListNestedTree", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "a.txt", "1")
		put(t, st, "dir/b.txt", "22")
		put(t, st, "dir/sub/c.txt", "333")
		// "dir.txt" sorts before "dir/b.txt" ('.' < '/'), which is the order an
		// object store lists keys in — and the reverse of the order a filesystem
		// walk meets them, since it descends "dir" before reading "dir.txt".
		put(t, st, "dir.txt", "4444")

		objects, err := st.List(context.Background(), "")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		assertObjects(t, objects, map[string]int64{
			"a.txt":         1,
			"dir.txt":       4,
			"dir/b.txt":     2,
			"dir/sub/c.txt": 3,
		})
		for _, o := range objects {
			if !o.Exist {
				t.Errorf("List entry %q has Exist = false, want true", o.Name)
			}
			if skew := time.Since(o.LastModified).Abs(); skew > 10*time.Second {
				t.Errorf("List entry %q LastModified is %v away from now, want within 10s", o.Name, skew)
			}
		}

		// A prefix roots the listing: names come back relative to it.
		nested, err := st.List(context.Background(), "dir")
		if err != nil {
			t.Fatalf("List(dir): %v", err)
		}
		assertObjects(t, nested, map[string]int64{"b.txt": 2, "sub/c.txt": 3})
	})

	// Unlike DeleteAll, a prefix holding nothing is an ordinary empty answer.
	t.Run("ListMissingPrefix", func(t *testing.T) {
		st := newStorage(t)
		objects, err := st.List(context.Background(), "never_existed")
		if err != nil {
			t.Fatalf("List(missing): %v", err)
		}
		if len(objects) != 0 {
			t.Errorf("List(missing) = %v, want empty", names(objects))
		}

		// Also on a storage that holds unrelated objects, not just an empty one.
		put(t, st, "kept/a.txt", "a")
		objects, err = st.List(context.Background(), "still_missing")
		if err != nil {
			t.Fatalf("List(missing): %v", err)
		}
		if len(objects) != 0 {
			t.Errorf("List(missing) = %v, want empty", names(objects))
		}
	})

	// The prefix is directory-like, so naming an object with it lists nothing:
	// on an object store "a.txt" as a prefix means "a.txt/", which no key carries.
	t.Run("ListPrefixThatIsAnObject", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "a.txt", "1")

		objects, err := st.List(context.Background(), "a.txt")
		if err != nil {
			t.Fatalf("List(a.txt): %v", err)
		}
		if len(objects) != 0 {
			t.Errorf("List(a.txt) = %v, want empty", names(objects))
		}
	})

	t.Run("ListPrefixIsolation", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "data/one.txt", "1")
		put(t, st, "database/two.txt", "22")

		objects, err := st.List(context.Background(), "data")
		if err != nil {
			t.Fatalf("List(data): %v", err)
		}
		// The "data" prefix is directory-like, so it must not swallow "database".
		assertObjects(t, objects, map[string]int64{"one.txt": 1})
	})

	t.Run("ListAfterSubStorage", func(t *testing.T) {
		st := newStorage(t)
		sub := mustSub(t, st, "subdir")
		put(t, sub, "x/y.txt", "data")
		put(t, st, "outside.txt", "ignored")

		// The sub-storage lists relative to its own cwd and sees nothing above it.
		objects, err := sub.List(context.Background(), "")
		if err != nil {
			t.Fatalf("sub.List: %v", err)
		}
		assertObjects(t, objects, map[string]int64{"x/y.txt": 4})

		// The parent reaches the same object through the sub-path prefix.
		objects, err = st.List(context.Background(), "subdir")
		if err != nil {
			t.Fatalf("List(subdir): %v", err)
		}
		assertObjects(t, objects, map[string]int64{"x/y.txt": 4})
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
		sub := mustSub(t, st, "subdir")
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

	// A key that walks out of the storage is refused before it reaches the
	// backend, so untrusted input cannot address a neighbour of the storage.
	t.Run("GuardRejectsEscapingKeys", func(t *testing.T) {
		st := newStorage(t)
		for _, key := range []string{"../escape.txt", "a/../../escape.txt", "/absolute.txt"} {
			if err := st.PutObject(context.Background(), key, bytes.NewReader([]byte("x"))); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("PutObject(%q) error = %v, want ErrUnsafeKey", key, err)
			}
			if _, err := st.GetObject(context.Background(), key); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("GetObject(%q) error = %v, want ErrUnsafeKey", key, err)
			}
			if _, err := st.Exists(context.Background(), key); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("Exists(%q) error = %v, want ErrUnsafeKey", key, err)
			}
		}
	})

	// The storage root is not an object key. It matters most for DeleteAll,
	// where an empty prefix would otherwise remove the storage itself.
	t.Run("GuardRejectsTheStorageRoot", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "kept.txt", "kept")

		for _, key := range []string{"", "."} {
			if err := st.DeleteAll(context.Background(), key); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("DeleteAll(%q) error = %v, want ErrUnsafeKey", key, err)
			}
			if _, err := st.Stat(key); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("Stat(%q) error = %v, want ErrUnsafeKey", key, err)
			}
		}
		assertExists(t, st, "kept.txt", true)
	})

	// Navigating downwards must not navigate out of the guard: the storages
	// SubStorage and ListDir return are guarded too.
	t.Run("GuardSurvivesNavigation", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "dir/x.txt", "x")

		sub := mustSub(t, st, "dir")
		if err := sub.PutObject(context.Background(), "../../escape.txt", bytes.NewReader([]byte("x"))); !errors.Is(err, storages.ErrUnsafeKey) {
			t.Errorf("SubStorage.PutObject(escaping) error = %v, want ErrUnsafeKey", err)
		}

		_, dirs, err := st.ListDir(context.Background())
		if err != nil {
			t.Fatalf("ListDir: %v", err)
		}
		if len(dirs) != 1 {
			t.Fatalf("ListDir returned %d dirs, want 1", len(dirs))
		}
		if err := dirs[0].PutObject(context.Background(), "../../escape.txt", bytes.NewReader([]byte("x"))); !errors.Is(err, storages.ErrUnsafeKey) {
			t.Errorf("ListDir dir PutObject(escaping) error = %v, want ErrUnsafeKey", err)
		}
	})

	// A sub-path is a key like any other, so SubStorage refuses one that escapes
	// instead of handing back a storage rooted outside.
	t.Run("GuardRejectsEscapingSubStorage", func(t *testing.T) {
		st := newStorage(t)
		for _, subPath := range []string{"../escape", "a/../../escape", "/absolute"} {
			escaped, err := st.SubStorage(subPath, true)
			if !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("SubStorage(%q) error = %v, want ErrUnsafeKey", subPath, err)
			}
			if escaped != nil {
				t.Errorf("SubStorage(%q) returned a storage rooted at %q, want nil", subPath, escaped.GetCwd())
			}
		}
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

// mustSub re-roots st at subPath, failing the test if the storage refuses.
func mustSub(t *testing.T, st storages.Storager, subPath string) storages.Storager {
	t.Helper()
	sub, err := st.SubStorage(subPath, true)
	if err != nil {
		t.Fatalf("SubStorage(%q): %v", subPath, err)
	}
	return sub
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

func mustGetRange(t *testing.T, st storages.Storager, name string, offset, length int64) []byte {
	t.Helper()
	r, err := st.GetObjectRange(context.Background(), name, offset, length)
	if err != nil {
		t.Fatalf("GetObjectRange(%q, %d, %d): %v", name, offset, length, err)
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

// assertObjects checks a List result against the objects it should hold: exactly
// the wanted names, each with the wanted size, in lexicographic order.
func assertObjects(t *testing.T, got []storages.ObjectStat, want map[string]int64) {
	t.Helper()

	wantNames := make([]string, 0, len(want))
	for name := range want {
		wantNames = append(wantNames, name)
	}
	slices.Sort(wantNames)

	if gotNames := names(got); !slices.Equal(gotNames, wantNames) {
		t.Fatalf("List names = %v, want %v (sorted)", gotNames, wantNames)
	}
	for _, o := range got {
		if o.Size != want[o.Name] {
			t.Errorf("List entry %q Size = %d, want %d", o.Name, o.Size, want[o.Name])
		}
	}
}

func names(objects []storages.ObjectStat) []string {
	res := make([]string, 0, len(objects))
	for _, o := range objects {
		res = append(res, o.Name)
	}
	return res
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
