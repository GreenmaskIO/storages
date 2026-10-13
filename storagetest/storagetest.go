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

	// A backend that reports an ETag reports the same one from List and Stat,
	// and a new one once the object is overwritten.
	t.Run("ETagAgreesAndChanges", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "dir/a.txt", "one")
		listed, err := st.List(context.Background(), "dir")
		if err != nil {
			t.Fatalf("List(dir): %v", err)
		}
		if len(listed) != 1 {
			t.Fatalf("List(dir) = %v, want one object", names(listed))
		}
		if listed[0].ETag == "" {
			t.Skip("the backend reports no ETag")
		}
		stat, err := st.Stat("dir/a.txt")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if stat.ETag != listed[0].ETag {
			t.Errorf("Stat ETag = %q, List ETag = %q, want them equal", stat.ETag, listed[0].ETag)
		}
		put(t, st, "dir/a.txt", "two")
		again, err := st.Stat("dir/a.txt")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if again.ETag == stat.ETag {
			t.Errorf("ETag after overwriting = %q, want it to change", again.ETag)
		}
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

	t.Run("CopyRoundTrip", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src.txt", "payload")
		stat, err := st.Copy(context.Background(), "src.txt", "nested/dir/dst.txt")
		if err != nil {
			t.Fatalf("Copy: %v", err)
		}
		if stat.Bytes != int64(len("payload")) {
			t.Errorf("CopyStat.Bytes = %d, want %d", stat.Bytes, len("payload"))
		}
		if got := string(mustGet(t, st, "nested/dir/dst.txt")); got != "payload" {
			t.Errorf("copy = %q, want %q", got, "payload")
		}
		if got := string(mustGet(t, st, "src.txt")); got != "payload" {
			t.Errorf("source after copy = %q, want %q", got, "payload")
		}
	})

	t.Run("CopyEmptyObject", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "empty.txt", "")
		stat, err := st.Copy(context.Background(), "empty.txt", "empty-copy.txt")
		if err != nil {
			t.Fatalf("Copy: %v", err)
		}
		if stat.Bytes != 0 {
			t.Errorf("CopyStat.Bytes = %d, want 0", stat.Bytes)
		}
		if got := mustGet(t, st, "empty-copy.txt"); len(got) != 0 {
			t.Errorf("copy = %q, want empty", got)
		}
	})

	t.Run("CopyOverwritesDestination", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src.txt", "new")
		put(t, st, "dst.txt", "old and longer")
		stat, err := st.Copy(context.Background(), "src.txt", "dst.txt")
		if err != nil {
			t.Fatalf("Copy: %v", err)
		}
		// The size of the copy, not of what it replaced.
		if stat.Bytes != int64(len("new")) {
			t.Errorf("CopyStat.Bytes = %d, want %d", stat.Bytes, len("new"))
		}
		if got := string(mustGet(t, st, "dst.txt")); got != "new" {
			t.Errorf("copy = %q, want %q", got, "new")
		}
	})

	// A hard link or reference would pass the round trip above but fail here.
	t.Run("CopyIsIndependentOfSource", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src.txt", "original")
		if _, err := st.Copy(context.Background(), "src.txt", "dst.txt"); err != nil {
			t.Fatalf("Copy: %v", err)
		}
		put(t, st, "src.txt", "changed")
		if got := string(mustGet(t, st, "dst.txt")); got != "original" {
			t.Errorf("copy after overwriting the source = %q, want %q", got, "original")
		}
		if err := st.Delete(context.Background(), "src.txt"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if got := string(mustGet(t, st, "dst.txt")); got != "original" {
			t.Errorf("copy after deleting the source = %q, want %q", got, "original")
		}
	})

	t.Run("CopyOntoItselfIsANoop", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "same.txt", "keep me")
		for _, dst := range []string{"same.txt", "./same.txt", "dir/../same.txt"} {
			stat, err := st.Copy(context.Background(), "same.txt", dst)
			if err != nil {
				t.Errorf("Copy(same.txt, %q): %v", dst, err)
			}
			if stat.Bytes != 0 {
				t.Errorf("Copy(same.txt, %q) CopyStat.Bytes = %d, want 0", dst, stat.Bytes)
			}
		}
		if got := string(mustGet(t, st, "same.txt")); got != "keep me" {
			t.Errorf("object after copying onto itself = %q, want %q", got, "keep me")
		}
	})

	t.Run("CopyMissingSourceIsAnError", func(t *testing.T) {
		st := newStorage(t)
		_, err := st.Copy(context.Background(), "missing.txt", "dst.txt")
		if !errors.Is(err, storages.ErrFileNotFound) {
			t.Errorf("Copy(missing) error = %v, want ErrFileNotFound", err)
		}
		assertExists(t, st, "dst.txt", false)

		// An object copied onto itself has to exist too.
		_, err = st.Copy(context.Background(), "missing.txt", "missing.txt")
		if !errors.Is(err, storages.ErrFileNotFound) {
			t.Errorf("Copy(missing onto itself) error = %v, want ErrFileNotFound", err)
		}
	})

	t.Run("CopyDirectoryIsAnError", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "dir/a.txt", "a")
		_, err := st.Copy(context.Background(), "dir", "dir-copy")
		if !errors.Is(err, storages.ErrFileNotFound) {
			t.Errorf("Copy(directory) error = %v, want ErrFileNotFound", err)
		}
		assertExists(t, st, "dir-copy", false)
	})

	t.Run("CopyWithinSubStorage", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "sub/src.txt", "scoped")
		sub := mustSub(t, st, "sub")
		if _, err := sub.Copy(context.Background(), "src.txt", "dst.txt"); err != nil {
			t.Fatalf("Copy: %v", err)
		}
		if got := string(mustGet(t, st, "sub/dst.txt")); got != "scoped" {
			t.Errorf("copy = %q, want %q", got, "scoped")
		}
		assertExists(t, st, "dst.txt", false)
	})

	t.Run("CopyAllTree", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src/a.txt", "1")
		put(t, st, "src/d/b.txt", "22")
		put(t, st, "src/d/e/c.txt", "333")
		stat, err := st.CopyAll(context.Background(), "src", "dst")
		if err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if stat.Objects != 3 || stat.Bytes != 6 {
			t.Errorf("CopyStat = %+v, want Objects 3, Bytes 6", stat)
		}
		objects, err := st.List(context.Background(), "dst")
		if err != nil {
			t.Fatalf("List(dst): %v", err)
		}
		assertObjects(t, objects, map[string]int64{"a.txt": 1, "d/b.txt": 2, "d/e/c.txt": 3})
		for name, want := range map[string]string{"a.txt": "1", "d/b.txt": "22", "d/e/c.txt": "333"} {
			if got := string(mustGet(t, st, "dst/"+name)); got != want {
				t.Errorf("dst/%s = %q, want %q", name, got, want)
			}
			if got := string(mustGet(t, st, "src/"+name)); got != want {
				t.Errorf("source src/%s after CopyAll = %q, want %q", name, got, want)
			}
		}
	})

	t.Run("CopyAllEmptyObjects", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src/empty.txt", "")
		put(t, st, "src/d/empty.txt", "")
		stat, err := st.CopyAll(context.Background(), "src", "dst")
		if err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if stat.Objects != 2 || stat.Bytes != 0 {
			t.Errorf("CopyStat = %+v, want Objects 2, Bytes 0", stat)
		}
		assertExists(t, st, "dst/empty.txt", true)
		assertExists(t, st, "dst/d/empty.txt", true)
	})

	t.Run("CopyAllOverwritesDestination", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src/a.txt", "new")
		put(t, st, "dst/a.txt", "old and longer")
		put(t, st, "dst/unrelated.txt", "stays")
		if _, err := st.CopyAll(context.Background(), "src", "dst"); err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if got := string(mustGet(t, st, "dst/a.txt")); got != "new" {
			t.Errorf("dst/a.txt = %q, want %q", got, "new")
		}
		if got := string(mustGet(t, st, "dst/unrelated.txt")); got != "stays" {
			t.Errorf("dst/unrelated.txt = %q, want %q", got, "stays")
		}
	})

	t.Run("CopyAllPrefixIsolation", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "data/one.txt", "1")
		put(t, st, "database/two.txt", "22")
		stat, err := st.CopyAll(context.Background(), "data", "copy")
		if err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if stat.Objects != 1 {
			t.Errorf("CopyStat.Objects = %d, want 1", stat.Objects)
		}
		objects, err := st.List(context.Background(), "copy")
		if err != nil {
			t.Fatalf("List(copy): %v", err)
		}
		// The "data" prefix is directory-like, so it must not swallow "database".
		assertObjects(t, objects, map[string]int64{"one.txt": 1})
	})

	t.Run("CopyAllMissingPrefixIsAnError", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "kept/a.txt", "a")
		// A prefix naming an object rather than a directory holds nothing either.
		put(t, st, "file.txt", "f")
		for _, prefix := range []string{"never_existed", "file.txt"} {
			stat, err := st.CopyAll(context.Background(), prefix, "dst")
			if !errors.Is(err, storages.ErrFileNotFound) {
				t.Errorf("CopyAll(%q) error = %v, want ErrFileNotFound", prefix, err)
			}
			if stat != (storages.CopyStat{}) {
				t.Errorf("CopyAll(%q) CopyStat = %+v, want zero", prefix, stat)
			}
		}
		objects, err := st.List(context.Background(), "dst")
		if err != nil {
			t.Fatalf("List(dst): %v", err)
		}
		if len(objects) != 0 {
			t.Errorf("a failed CopyAll wrote %v", names(objects))
		}
	})

	t.Run("CopyAllOntoItself", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src/a.txt", "keep me")
		put(t, st, "src/d/b.txt", "me too")
		if _, err := st.CopyAll(context.Background(), "src", "src"); err != nil {
			t.Fatalf("CopyAll onto itself: %v", err)
		}
		if got := string(mustGet(t, st, "src/a.txt")); got != "keep me" {
			t.Errorf("src/a.txt = %q, want %q", got, "keep me")
		}
		if got := string(mustGet(t, st, "src/d/b.txt")); got != "me too" {
			t.Errorf("src/d/b.txt = %q, want %q", got, "me too")
		}
	})

	t.Run("CopyAllIntoOwnSubtree", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "a/x.txt", "x")
		put(t, st, "a/d/y.txt", "yy")
		stat, err := st.CopyAll(context.Background(), "a", "a/b")
		if err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if stat.Objects != 2 {
			t.Errorf("CopyStat.Objects = %d, want 2", stat.Objects)
		}
		objects, err := st.List(context.Background(), "a")
		if err != nil {
			t.Fatalf("List(a): %v", err)
		}
		assertObjects(t, objects, map[string]int64{"x.txt": 1, "d/y.txt": 2, "b/x.txt": 1, "b/d/y.txt": 2})
	})

	t.Run("CopyAllIntoOwnSubtreeLeavesOutTheDestination", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "a/x.txt", "new")
		put(t, st, "a/b/x.txt", "old")
		put(t, st, "a/b/kept.txt", "kept")
		stat, err := st.CopyAll(context.Background(), "a", "a/b")
		if err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if stat.Objects != 1 {
			t.Errorf("CopyStat.Objects = %d, want 1", stat.Objects)
		}
		objects, err := st.List(context.Background(), "a")
		if err != nil {
			t.Fatalf("List(a): %v", err)
		}
		assertObjects(t, objects, map[string]int64{"x.txt": 3, "b/x.txt": 3, "b/kept.txt": 4})
		if got := string(mustGet(t, st, "a/b/x.txt")); got != "new" {
			t.Errorf("a/b/x.txt = %q, want %q", got, "new")
		}
	})

	t.Run("CopyAllFromRoot", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "x.txt", "x")
		put(t, st, "d/y.txt", "yy")
		stat, err := st.CopyAll(context.Background(), "", "backup")
		if err != nil {
			t.Fatalf("CopyAll from the root: %v", err)
		}
		if stat.Objects != 2 {
			t.Errorf("CopyStat.Objects = %d, want 2", stat.Objects)
		}
		objects, err := st.List(context.Background(), "")
		if err != nil {
			t.Fatalf("List(root): %v", err)
		}
		assertObjects(t, objects, map[string]int64{"x.txt": 1, "d/y.txt": 2, "backup/x.txt": 1, "backup/d/y.txt": 2})
	})

	t.Run("CopyAllIsIndependentOfSource", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src/a.txt", "original")
		if _, err := st.CopyAll(context.Background(), "src", "dst"); err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if err := st.DeleteAll(context.Background(), "src"); err != nil {
			t.Fatalf("DeleteAll(src): %v", err)
		}
		if got := string(mustGet(t, st, "dst/a.txt")); got != "original" {
			t.Errorf("copy after deleting the source = %q, want %q", got, "original")
		}
	})

	t.Run("CopyAllWithinSubStorage", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "sub/src/a.txt", "scoped")
		put(t, st, "src/a.txt", "outside")
		sub := mustSub(t, st, "sub")
		if _, err := sub.CopyAll(context.Background(), "src", "dst"); err != nil {
			t.Fatalf("CopyAll: %v", err)
		}
		if got := string(mustGet(t, st, "sub/dst/a.txt")); got != "scoped" {
			t.Errorf("copy = %q, want %q", got, "scoped")
		}
		assertExists(t, st, "dst/a.txt", false)
	})

	t.Run("CopyAllGuardRejectsEscapingPrefixes", func(t *testing.T) {
		st := newStorage(t)
		put(t, st, "src/a.txt", "a")
		for _, tc := range []struct{ src, dst string }{
			{"../src", "dst"},
			{"src", "../dst"},
			{"src", "dst/../../dst"},
		} {
			if _, err := st.CopyAll(context.Background(), tc.src, tc.dst); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("CopyAll(%q, %q) error = %v, want ErrUnsafeKey", tc.src, tc.dst, err)
			}
		}
		assertExists(t, st, "dst/a.txt", false)
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

		put(t, st, "inside.txt", "x")
		for _, key := range []string{"../escape.txt", "a/../../escape.txt", "/absolute.txt"} {
			if _, err := st.Copy(context.Background(), key, "copy.txt"); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("Copy(%q, copy.txt) error = %v, want ErrUnsafeKey", key, err)
			}
			if _, err := st.Copy(context.Background(), "inside.txt", key); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("Copy(inside.txt, %q) error = %v, want ErrUnsafeKey", key, err)
			}
		}
		assertExists(t, st, "copy.txt", false)
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
			if _, err := st.Copy(context.Background(), "kept.txt", key); !errors.Is(err, storages.ErrUnsafeKey) {
				t.Errorf("Copy(kept.txt, %q) error = %v, want ErrUnsafeKey", key, err)
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
