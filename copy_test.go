// Copyright 2026 Greenmask
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

package storages_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/inmemory"
)

// StreamCopy only uses the Storager surface, so the in-memory backend stands in
// for any backend that delegates to it.

func TestStreamCopy_CopiesThroughGetAndPut(t *testing.T) {
	ctx := context.Background()
	st := inmemory.New("/store")
	require.NoError(t, st.PutObject(ctx, "src.txt", strings.NewReader("payload")))

	stat, err := storages.StreamCopy(ctx, st, "src.txt", "a/b/dst.txt")
	require.NoError(t, err)

	assert.Equal(t, storages.CopyStat{Bytes: int64(len("payload")), Streamed: true}, stat)

	assertContent(t, st, "a/b/dst.txt", "payload")
	assertContent(t, st, "src.txt", "payload")
}

// TestStreamCopy_OntoItselfLeavesTheObjectIntact checks that a same-key copy
// does not truncate the object.
func TestStreamCopy_OntoItselfLeavesTheObjectIntact(t *testing.T) {
	ctx := context.Background()
	st := inmemory.New("/store")
	require.NoError(t, st.PutObject(ctx, "same.txt", strings.NewReader("keep me")))

	for _, dst := range []string{"same.txt", "./same.txt", "dir/../same.txt"} {
		stat, err := storages.StreamCopy(ctx, st, "same.txt", dst)
		require.NoError(t, err, dst)
		assert.Equal(t, storages.CopyStat{}, stat, "nothing was written")
	}

	assertContent(t, st, "same.txt", "keep me")
}

func TestStreamCopy_MissingSourceIsErrFileNotFound(t *testing.T) {
	ctx := context.Background()
	st := inmemory.New("/store")

	_, err := storages.StreamCopy(ctx, st, "missing.txt", "dst.txt")
	assert.ErrorIs(t, err, storages.ErrFileNotFound)
	_, err = storages.StreamCopy(ctx, st, "missing.txt", "missing.txt")
	assert.ErrorIs(t, err, storages.ErrFileNotFound)

	exists, err := st.Exists(ctx, "dst.txt")
	require.NoError(t, err)
	assert.False(t, exists, "a failed copy must not create the destination")
}

func TestNewCopyOptions(t *testing.T) {
	assert.Equal(t,
		storages.CopyOptions{PollInterval: time.Second, PollMaxInterval: time.Minute},
		storages.NewCopyOptions(storages.WithCopyPollInterval(time.Second, time.Minute)))
	assert.Equal(t, storages.CopyOptions{}, storages.NewCopyOptions())
	assert.Equal(t, storages.CopyOptions{}, storages.NewCopyOptions(nil), "a nil option is skipped")
	assert.Equal(t,
		storages.CopyOptions{Concurrency: 4, PartSize: 8 << 20},
		storages.NewCopyOptions(storages.WithCopyConcurrency(2), storages.WithCopyPartSize(8<<20), storages.WithCopyConcurrency(4)),
		"a later option overrides an earlier one",
	)
	assert.Equal(t,
		storages.CopyOptions{},
		storages.NewCopyOptions(storages.WithCopyConcurrency(-1), storages.WithCopyPartSize(-1), storages.WithCopyPollInterval(-1, -1)),
		"a negative value means the backend's default",
	)
}

// copyHookStorage intercepts every Copy; the inner Copy is skipped when the
// hook returns handled true.
type copyHookStorage struct {
	storages.Storager
	hook func(ctx context.Context, src, dst string, opts []storages.CopyOption) (stat storages.CopyStat, err error, handled bool)
}

func (s *copyHookStorage) Copy(ctx context.Context, src, dst string, opts ...storages.CopyOption) (storages.CopyStat, error) {
	if s.hook != nil {
		if stat, err, handled := s.hook(ctx, src, dst, opts); handled {
			return stat, err
		}
	}
	return s.Storager.Copy(ctx, src, dst, opts...)
}

// listErrStorage fails every List.
type listErrStorage struct {
	storages.Storager
	err error
}

func (s *listErrStorage) List(context.Context, string) ([]storages.ObjectStat, error) {
	return nil, s.err
}

func putAll(t *testing.T, st storages.Storager, objects map[string]string) {
	t.Helper()
	for name, content := range objects {
		require.NoError(t, st.PutObject(context.Background(), name, strings.NewReader(content)))
	}
}

func objectKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("src/obj-%04d", i)
	}
	return keys
}

func TestCopyAll_CopiesEveryObjectUnderThePrefix(t *testing.T) {
	ctx := context.Background()
	st := inmemory.New("/store")
	putAll(t, st, map[string]string{
		"src/a":       "1",
		"src/d/b":     "22",
		"src/d/e/c":   "333",
		"srcsibling":  "x",
		"other/z.txt": "z",
	})

	stat, err := storages.CopyEach(ctx, st, "src", "dst")
	require.NoError(t, err)
	assert.Equal(t, storages.CopyStat{Bytes: 6, Objects: 3}, stat)

	assertContent(t, st, "dst/a", "1")
	assertContent(t, st, "dst/d/b", "22")
	assertContent(t, st, "dst/d/e/c", "333")
	got, err := st.List(ctx, "dst")
	require.NoError(t, err)
	assert.Len(t, got, 3, "nothing outside the prefix is copied")
}

func TestCopyAll_EmptyPrefixIsErrFileNotFound(t *testing.T) {
	ctx := context.Background()
	st := inmemory.New("/store")
	putAll(t, st, map[string]string{"a.txt": "a"})

	for _, prefix := range []string{"missing", "a.txt"} {
		stat, err := storages.CopyEach(ctx, st, prefix, "dst")
		assert.ErrorIs(t, err, storages.ErrFileNotFound, prefix)
		assert.Equal(t, storages.CopyStat{}, stat, prefix)
	}
}

func TestCopyAll_ListErrorCopiesNothing(t *testing.T) {
	listErr := errors.New("boom")
	var copies atomic.Int32
	st := &copyHookStorage{
		Storager: &listErrStorage{Storager: inmemory.New("/store"), err: listErr},
		hook: func(context.Context, string, string, []storages.CopyOption) (storages.CopyStat, error, bool) {
			copies.Add(1)
			return storages.CopyStat{}, nil, true
		},
	}

	_, err := storages.CopyEach(context.Background(), st, "src", "dst")
	assert.ErrorIs(t, err, listErr)
	assert.Zero(t, copies.Load())
}

// TestCopyAll_FirstFailureStopsTheCopy checks that the first failure stops
// dispatch, names the object, and only finished copies are counted.
func TestCopyAll_FirstFailureStopsTheCopy(t *testing.T) {
	ctx := context.Background()
	inner := inmemory.New("/store")
	keys := objectKeys(50)
	for _, k := range keys {
		require.NoError(t, inner.PutObject(ctx, k, strings.NewReader("ab")))
	}

	var (
		mu      sync.Mutex
		started []string
	)
	failing := "src/obj-0010"
	st := &copyHookStorage{Storager: inner, hook: func(_ context.Context, src, _ string, _ []storages.CopyOption) (storages.CopyStat, error, bool) {
		mu.Lock()
		started = append(started, src)
		mu.Unlock()
		if src == failing {
			return storages.CopyStat{}, storages.ErrSourceChanged, true
		}
		return storages.CopyStat{}, nil, false
	}}

	stat, err := storages.CopyEach(ctx, st, "src", "dst", storages.WithCopyAllObjectConcurrency(1))
	require.Error(t, err)
	assert.ErrorIs(t, err, storages.ErrSourceChanged)
	assert.Contains(t, err.Error(), failing)

	// Sequential, so exactly the objects before the failing one were copied.
	assert.Equal(t, keys[:11], started)
	assert.Equal(t, storages.CopyStat{Bytes: 20, Objects: 10}, stat)
	exists, err := inner.Exists(ctx, "dst/obj-0011")
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestCopyAll_FailureCancelsRunningCopies checks that a failure cancels the
// copies still running and starts no more, and that only the failure is reported.
func TestCopyAll_FailureCancelsRunningCopies(t *testing.T) {
	ctx := context.Background()
	inner := inmemory.New("/store")
	for _, k := range objectKeys(8) {
		require.NoError(t, inner.PutObject(ctx, k, strings.NewReader("x")))
	}

	cause := errors.New("copy refused")
	var (
		started, cancelled atomic.Int32
		running            sync.WaitGroup
	)
	running.Add(4)
	st := &copyHookStorage{Storager: inner, hook: func(ctx context.Context, src, _ string, _ []storages.CopyOption) (storages.CopyStat, error, bool) {
		started.Add(1)
		running.Done()
		running.Wait() // all four are in flight
		if src == "src/obj-0000" {
			return storages.CopyStat{}, cause, true
		}
		<-ctx.Done()
		cancelled.Add(1)
		return storages.CopyStat{}, ctx.Err(), true
	}}

	stat, err := storages.CopyEach(ctx, st, "src", "dst", storages.WithCopyAllObjectConcurrency(4))

	assert.ErrorIs(t, err, cause)
	assert.NotErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(3), cancelled.Load(), "the other running copies are cancelled")
	assert.Equal(t, int32(4), started.Load(), "no copy starts after the failure")
	assert.Equal(t, storages.CopyStat{}, stat)
}

func TestCopyAll_CallerCancellation(t *testing.T) {
	inner := inmemory.New("/store")
	for _, k := range objectKeys(20) {
		require.NoError(t, inner.PutObject(context.Background(), k, strings.NewReader("x")))
	}

	ctx, cancel := context.WithCancel(context.Background())
	var copies atomic.Int32
	// The hook ignores ctx, so in-flight copies finish; none may start after.
	st := &copyHookStorage{Storager: inner, hook: func(context.Context, string, string, []storages.CopyOption) (storages.CopyStat, error, bool) {
		if copies.Add(1) == 5 {
			cancel()
		}
		return storages.CopyStat{Bytes: 1}, nil, true
	}}

	stat, err := storages.CopyEach(ctx, st, "src", "dst", storages.WithCopyAllObjectConcurrency(1))
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(5), copies.Load(), "no copy starts once the caller cancels")
	assert.Equal(t, storages.CopyStat{Bytes: 5, Objects: 5}, stat, "the copies that finished are still reported")
}

func TestCopyAll_ConcurrencyLimit(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []storages.CopyAllOption
		want int
	}{
		{name: "one", opts: []storages.CopyAllOption{storages.WithCopyAllObjectConcurrency(1)}, want: 1},
		{name: "three", opts: []storages.CopyAllOption{storages.WithCopyAllObjectConcurrency(3)}, want: 3},
		{name: "default", want: 8},
		{name: "below one means default", opts: []storages.CopyAllOption{storages.WithCopyAllObjectConcurrency(-5)}, want: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			inner := inmemory.New("/store")
			for _, k := range objectKeys(40) {
				require.NoError(t, inner.PutObject(ctx, k, strings.NewReader("x")))
			}

			var inFlight, peak atomic.Int32
			st := &copyHookStorage{Storager: inner, hook: func(context.Context, string, string, []storages.CopyOption) (storages.CopyStat, error, bool) {
				n := inFlight.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				// Linger so that copies overlap whenever the limit lets them.
				time.Sleep(5 * time.Millisecond)
				inFlight.Add(-1)
				return storages.CopyStat{}, nil, false
			}}

			stat, err := storages.CopyEach(ctx, st, "src", "dst", tc.opts...)
			require.NoError(t, err)
			assert.Equal(t, 40, stat.Objects)
			assert.LessOrEqual(t, int(peak.Load()), tc.want)
			assert.Equal(t, tc.want, int(peak.Load()), "the limit is reached, not just respected")
		})
	}
}

func TestCopyAll_PassesPerObjectOptionsToEveryCopy(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []storages.CopyAllOption
		want storages.CopyOptions
	}{
		{name: "none", want: storages.CopyOptions{}},
		{
			name: "all",
			opts: []storages.CopyAllOption{
				storages.WithCopyAllObjectConcurrency(2),
				storages.WithCopyAllPartConcurrency(3),
				storages.WithCopyAllPartSize(16 << 20),
				storages.WithCopyAllClientSideFallback(),
				storages.WithCopyAllPollInterval(time.Second, time.Minute),
			},
			want: storages.CopyOptions{
				Concurrency:        3,
				PartSize:           16 << 20,
				ClientSideFallback: true,
				PollInterval:       time.Second,
				PollMaxInterval:    time.Minute,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			inner := inmemory.New("/store")
			putAll(t, inner, map[string]string{"src/a": "a", "src/b/c": "c"})

			var (
				mu  sync.Mutex
				got []storages.CopyOptions
			)
			st := &copyHookStorage{Storager: inner, hook: func(_ context.Context, _, _ string, opts []storages.CopyOption) (storages.CopyStat, error, bool) {
				mu.Lock()
				got = append(got, storages.NewCopyOptions(opts...))
				mu.Unlock()
				return storages.CopyStat{}, nil, false
			}}

			_, err := storages.CopyEach(ctx, st, "src", "dst", tc.opts...)
			require.NoError(t, err)
			assert.Equal(t, []storages.CopyOptions{tc.want, tc.want}, got)
		})
	}
}

// TestCopyAllOptions_MirrorsCopyOptions checks that CopyAllOptions has every
// CopyOptions field, plus ObjectConcurrency.
func TestCopyAllOptions_MirrorsCopyOptions(t *testing.T) {
	copyFields := reflect.TypeFor[storages.CopyOptions]().NumField()
	copyAllFields := reflect.TypeFor[storages.CopyAllOptions]().NumField()
	assert.Equal(t, copyFields+1, copyAllFields,
		"CopyAllOptions must mirror every CopyOptions field (plus ObjectConcurrency); update copyOptions too")
}

func TestNewCopyAllOptions(t *testing.T) {
	assert.Equal(t, storages.CopyAllOptions{}, storages.NewCopyAllOptions())
	assert.Equal(t, storages.CopyAllOptions{}, storages.NewCopyAllOptions(nil), "a nil option is skipped")
	assert.Equal(t,
		storages.CopyAllOptions{ObjectConcurrency: 4, PartSize: 8 << 20},
		storages.NewCopyAllOptions(
			storages.WithCopyAllObjectConcurrency(2),
			storages.WithCopyAllPartSize(8<<20),
			storages.WithCopyAllObjectConcurrency(4),
		),
		"a later option overrides an earlier one",
	)
	assert.Equal(t,
		storages.CopyAllOptions{},
		storages.NewCopyAllOptions(
			storages.WithCopyAllObjectConcurrency(-1),
			storages.WithCopyAllPartConcurrency(-1),
			storages.WithCopyAllPartSize(-1),
			storages.WithCopyAllPollInterval(-1, -1),
		),
		"a negative value means the default",
	)
}

// markerStorage adds S3-style folder markers ("dir/" keys) to every listing.
type markerStorage struct {
	storages.Storager
	markers []string
}

func (s *markerStorage) List(ctx context.Context, prefix string) ([]storages.ObjectStat, error) {
	objects, err := s.Storager.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for _, m := range s.markers {
		objects = append(objects, storages.ObjectStat{Name: m, Exist: true})
	}
	return objects, nil
}

// TestCopyAll_SkipsFolderMarkers checks that a "dir/" key in the listing is
// skipped rather than copied.
func TestCopyAll_SkipsFolderMarkers(t *testing.T) {
	ctx := context.Background()
	inner := inmemory.New("/store")
	putAll(t, inner, map[string]string{"src/a.txt": "a", "src/sub/b.txt": "bb"})
	st := &markerStorage{Storager: inner, markers: []string{"sub/", "empty/"}}

	stat, err := storages.CopyEach(ctx, st, "src", "dst")
	require.NoError(t, err)

	assert.Equal(t, storages.CopyStat{Bytes: 3, Objects: 2}, stat)
	assertContent(t, inner, "dst/a.txt", "a")
	assertContent(t, inner, "dst/sub/b.txt", "bb")
}

// TestCopyAll_OnlyFolderMarkersIsErrFileNotFound checks that a prefix holding
// nothing but folder markers holds nothing to copy, as an empty directory does.
func TestCopyAll_OnlyFolderMarkersIsErrFileNotFound(t *testing.T) {
	st := &markerStorage{Storager: inmemory.New("/store"), markers: []string{"sub/"}}

	_, err := storages.CopyEach(context.Background(), st, "src", "dst")
	assert.ErrorIs(t, err, storages.ErrFileNotFound)
}

// TestCopyAll_LeavesOutAnExistingNestedDestination checks that objects already
// under a nested destination are not copied, however the prefixes are spelled.
func TestCopyAll_LeavesOutAnExistingNestedDestination(t *testing.T) {
	for _, tc := range []struct {
		name     string
		src, dst string
		objects  map[string]string
		want     map[string]string
		absent   []string
		objectsN int
	}{
		{
			name: "a sub-directory", src: "a", dst: "a/b",
			objects:  map[string]string{"a/x": "x", "a/b/old": "old"},
			want:     map[string]string{"a/b/x": "x", "a/b/old": "old"},
			absent:   []string{"a/b/b/old"},
			objectsN: 1,
		},
		{
			name: "the storage root", src: "", dst: "backup",
			objects:  map[string]string{"f": "new", "backup/f": "old", "backup/g": "g"},
			want:     map[string]string{"backup/f": "new", "backup/g": "g"},
			absent:   []string{"backup/backup/f", "backup/backup/g"},
			objectsN: 1,
		},
		{
			name: "spelled differently", src: "./a/", dst: "a//b/",
			objects:  map[string]string{"a/x": "x", "a/b/old": "old"},
			want:     map[string]string{"a/b/x": "x"},
			absent:   []string{"a/b/b/old"},
			objectsN: 1,
		},
		{
			name: "a leading slash names the same prefix", src: "", dst: "/backup",
			objects:  map[string]string{"f": "new", "backup/g": "g"},
			want:     map[string]string{"backup/f": "new", "backup/g": "g"},
			absent:   []string{"backup/backup/g"},
			objectsN: 1,
		},
		{
			name: "a sibling sharing the name's start is not nested", src: "a", dst: "ab",
			objects:  map[string]string{"a/x": "x", "ab/y": "y"},
			want:     map[string]string{"ab/x": "x", "ab/y": "y"},
			objectsN: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// Unguarded, as CopyEach is called by a backend: "/a" reaches it as given.
			st := inmemory.New("/store", inmemory.WithUnsafe())
			putAll(t, st, tc.objects)

			stat, err := storages.CopyEach(ctx, st, tc.src, tc.dst)
			require.NoError(t, err)

			assert.Equal(t, tc.objectsN, stat.Objects)
			for key, want := range tc.want {
				assertContent(t, st, key, want)
			}
			for _, key := range tc.absent {
				exists, err := st.Exists(ctx, key)
				require.NoError(t, err)
				assert.False(t, exists, key)
			}
		})
	}
}

// TestCopyAll_NothingLeftOutsideTheNestedDestination checks that a source lying
// entirely under the nested destination copies nothing and succeeds.
func TestCopyAll_NothingLeftOutsideTheNestedDestination(t *testing.T) {
	st := inmemory.New("/store")
	putAll(t, st, map[string]string{"a/b/old": "old"})

	stat, err := storages.CopyEach(context.Background(), st, "a", "a/b")
	require.NoError(t, err)
	assert.Equal(t, storages.CopyStat{}, stat)
}

// TestCopyEach_RefusesUnsafeListedKeys checks that a listed key which would not
// land where its name says, once joined onto a prefix, stops the copy before
// any object is copied.
func TestCopyEach_RefusesUnsafeListedKeys(t *testing.T) {
	for _, bad := range []string{"../escape", "a/../../escape", "a//b", "./a", "/abs"} {
		t.Run(bad, func(t *testing.T) {
			inner := inmemory.New("/store", inmemory.WithUnsafe())
			putAll(t, inner, map[string]string{"src/ok.txt": "ok"})
			var copies atomic.Int32
			st := &copyHookStorage{
				Storager: &markerStorage{Storager: inner, markers: []string{bad}},
				hook: func(context.Context, string, string, []storages.CopyOption) (storages.CopyStat, error, bool) {
					copies.Add(1)
					return storages.CopyStat{}, nil, false
				},
			}

			_, err := storages.CopyEach(context.Background(), st, "src", "dst")

			assert.ErrorIs(t, err, storages.ErrUnsafeKey)
			assert.Contains(t, err.Error(), bad)
			assert.Zero(t, copies.Load(), "nothing is copied once a key is refused")
		})
	}
}

// TestCopyAll_CopiesAnObjectNamedLikeTheDestination checks that, on an object
// store where "b" and "b/..." coexist, CopyAll("", "b") copies the object "b".
func TestCopyAll_CopiesAnObjectNamedLikeTheDestination(t *testing.T) {
	inner := inmemory.New("/store")
	putAll(t, inner, map[string]string{"b": "b", "c": "c"})
	var (
		mu  sync.Mutex
		dst []string
	)
	st := &copyHookStorage{Storager: inner, hook: func(_ context.Context, _, d string, _ []storages.CopyOption) (storages.CopyStat, error, bool) {
		mu.Lock()
		dst = append(dst, d)
		mu.Unlock()
		return storages.CopyStat{}, nil, true
	}}

	stat, err := storages.CopyEach(context.Background(), st, "", "b")

	require.NoError(t, err)
	assert.Equal(t, 2, stat.Objects)
	assert.ElementsMatch(t, []string{"b/b", "b/c"}, dst)
}

// TestCopyAll_CancelAfterEveryCopySucceededIsNotAnError checks that a ctx
// ending once every object has been copied does not fail the copy.
func TestCopyAll_CancelAfterEveryCopySucceededIsNotAnError(t *testing.T) {
	inner := inmemory.New("/store")
	for _, k := range objectKeys(3) {
		require.NoError(t, inner.PutObject(context.Background(), k, strings.NewReader("x")))
	}
	ctx, cancel := context.WithCancel(context.Background())
	var copies atomic.Int32
	st := &copyHookStorage{Storager: inner, hook: func(context.Context, string, string, []storages.CopyOption) (storages.CopyStat, error, bool) {
		if copies.Add(1) == 3 {
			cancel()
		}
		return storages.CopyStat{Bytes: 1}, nil, true
	}}

	stat, err := storages.CopyEach(ctx, st, "src", "dst", storages.WithCopyAllObjectConcurrency(1))

	require.NoError(t, err)
	assert.Equal(t, storages.CopyStat{Bytes: 3, Objects: 3}, stat)
}

// listingOnly is an Inspector whose List returns fixed objects.
type listingOnly struct {
	storages.Inspector
	objects []storages.ObjectStat
}

func (l listingOnly) List(context.Context, string) ([]storages.ObjectStat, error) {
	return l.objects, nil
}

// TestCopyListed_PassesTheListedObject checks that copyObject receives each
// object as listed, with its source and destination paths and the copy options.
func TestCopyListed_PassesTheListedObject(t *testing.T) {
	listed := []storages.ObjectStat{
		{Name: "a.txt", Size: 1, ETag: `"e1"`, Exist: true},
		{Name: "d/b.txt", Size: 2, ETag: `"e2"`, Exist: true},
	}
	var (
		mu  sync.Mutex
		got = map[string]storages.ObjectStat{}
	)
	copyObject := func(_ context.Context, obj storages.ObjectStat, src, dst string, opts ...storages.CopyOption) (storages.CopyStat, error) {
		assert.Equal(t, storages.CopyOptions{PartSize: 8 << 20}, storages.NewCopyOptions(opts...))
		mu.Lock()
		got[src+" -> "+dst] = obj
		mu.Unlock()
		return storages.CopyStat{Bytes: obj.Size}, nil
	}

	stat, err := storages.CopyListed(context.Background(), listingOnly{objects: listed}, "src", "dst", copyObject,
		storages.WithCopyAllPartSize(8<<20))

	require.NoError(t, err)
	assert.Equal(t, storages.CopyStat{Bytes: 3, Objects: 2}, stat)
	assert.Equal(t, map[string]storages.ObjectStat{
		"src/a.txt -> dst/a.txt":     listed[0],
		"src/d/b.txt -> dst/d/b.txt": listed[1],
	}, got)
}
