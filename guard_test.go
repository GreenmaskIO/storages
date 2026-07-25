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

// The guard is a decorator that knows nothing about the storage underneath it,
// so its correctness is established once, here, over the in-memory backend, and
// holds for every backend that applies it. What each backend's own tests pin is
// only that its constructor applies it.
//
// This is an external test package: the guard is exercised through the exported
// surface, and the in-memory backend it wraps imports storages itself.
package storages_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/inmemory"
)

// newGuarded returns a guarded storage rooted at /store and the unguarded view
// of the same filesystem, which the assertions use to check what did (or did
// not) end up outside the storage. /victim holds a sibling the escape attempts
// aim at.
func newGuarded(t *testing.T) (guarded, raw storages.Storager) {
	t.Helper()

	raw = inmemory.New("/store", inmemory.WithUnsafe())
	require.NoError(t, raw.PutObject(context.Background(), "../victim/secret.txt", strings.NewReader("SECRET")))
	require.NoError(t, raw.PutObject(context.Background(), "inside.txt", strings.NewReader("inside")))
	return storages.Guard(raw), raw
}

// unsafeKeys are the keys no object method may let through: the storage root
// under either spelling, anything that still walks out after path.Clean, and
// anything absolute.
var unsafeKeys = []string{
	"",
	".",
	"..",
	"../victim/secret.txt",
	"a/../../victim/secret.txt",
	"./../victim",
	"/store/inside.txt",
	"/etc/passwd",
}

// Every object method gates its key, so the matrix is run against all of them
// rather than against one and assumed of the rest.
func TestGuard_ObjectMethodsRejectUnsafeKeys(t *testing.T) {
	ctx := context.Background()

	methods := map[string]func(st storages.Storager, key string) error{
		"GetObject": func(st storages.Storager, key string) error {
			_, err := st.GetObject(ctx, key)
			return err
		},
		"GetObjectRange": func(st storages.Storager, key string) error {
			_, err := st.GetObjectRange(ctx, key, 0, 1)
			return err
		},
		"PutObject": func(st storages.Storager, key string) error {
			return st.PutObject(ctx, key, strings.NewReader("planted"))
		},
		"Delete": func(st storages.Storager, key string) error {
			return st.Delete(ctx, key)
		},
		"DeleteAll": func(st storages.Storager, key string) error {
			return st.DeleteAll(ctx, key)
		},
		"Exists": func(st storages.Storager, key string) error {
			_, err := st.Exists(ctx, key)
			return err
		},
		"Stat": func(st storages.Storager, key string) error {
			_, err := st.Stat(key)
			return err
		},
	}

	for name, call := range methods {
		t.Run(name, func(t *testing.T) {
			guarded, raw := newGuarded(t)
			for _, key := range unsafeKeys {
				t.Run(quoted(key), func(t *testing.T) {
					assert.ErrorIs(t, call(guarded, key), storages.ErrUnsafeKey)
				})
			}

			// Nothing outside the storage was touched, whichever method was called.
			assertContent(t, raw, "../victim/secret.txt", "SECRET")
			assertContent(t, raw, "inside.txt", "inside")
		})
	}
}

// A key that only travels through ".." on its way to a destination inside the
// storage is fine, and reaches the backend cleaned.
func TestGuard_PassesSafeKeysThroughCleaned(t *testing.T) {
	ctx := context.Background()
	guarded, raw := newGuarded(t)

	for _, key := range []string{"a.txt", "./b.txt", "dir/../c.txt", "dir/sub/../d.txt"} {
		require.NoError(t, guarded.PutObject(ctx, key, strings.NewReader(key)))
	}

	// The cleaned key is what the backend stored under, not the original spelling.
	assertContent(t, raw, "a.txt", "a.txt")
	assertContent(t, raw, "b.txt", "./b.txt")
	assertContent(t, raw, "c.txt", "dir/../c.txt")
	assertContent(t, raw, "dir/d.txt", "dir/sub/../d.txt")
}

// The storage root is a legitimate listing prefix — listing everything destroys
// nothing — so List only rejects the keys that leave the storage.
func TestGuard_ListGatesPrefix(t *testing.T) {
	ctx := context.Background()
	guarded, _ := newGuarded(t)

	t.Run("the root is an ordinary prefix", func(t *testing.T) {
		for _, prefix := range []string{"", "."} {
			objects, err := guarded.List(ctx, prefix)
			require.NoError(t, err)
			assert.Equal(t, []string{"inside.txt"}, names(objects))
		}
	})

	t.Run("escaping prefixes are rejected", func(t *testing.T) {
		for _, prefix := range []string{"..", "../victim", "a/../../victim", "/store"} {
			_, err := guarded.List(ctx, prefix)
			assert.ErrorIsf(t, err, storages.ErrUnsafeKey, "List(%q)", prefix)
		}
	})
}

// DeleteAll takes a prefix but is gated as an object key: the prefix that names
// the storage root would take the storage itself down with it.
func TestGuard_DeleteAllCannotRemoveTheStorage(t *testing.T) {
	ctx := context.Background()
	guarded, raw := newGuarded(t)

	for _, prefix := range []string{"", ".", "..", "../victim"} {
		assert.ErrorIsf(t, guarded.DeleteAll(ctx, prefix), storages.ErrUnsafeKey, "DeleteAll(%q)", prefix)
	}

	assertContent(t, raw, "inside.txt", "inside")
	assertContent(t, raw, "../victim/secret.txt", "SECRET")
}

// Delete promises that one bad path leaves the storage untouched; the gate runs
// over every path before any of them is deleted, so an unsafe one does not slip
// through after the safe ones have already gone.
func TestGuard_DeleteRejectsBeforeDeletingAnything(t *testing.T) {
	ctx := context.Background()
	guarded, raw := newGuarded(t)

	err := guarded.Delete(ctx, "inside.txt", "../victim/secret.txt")
	assert.ErrorIs(t, err, storages.ErrUnsafeKey)
	assertContent(t, raw, "inside.txt", "inside")
	assertContent(t, raw, "../victim/secret.txt", "SECRET")
}

// Navigating downwards must not navigate out of the guard: what SubStorage and
// ListDir hand back is guarded too.
func TestGuard_NavigationStaysGuarded(t *testing.T) {
	ctx := context.Background()

	t.Run("SubStorage", func(t *testing.T) {
		guarded, raw := newGuarded(t)
		sub := mustSub(t, guarded, "sub", true)
		assert.ErrorIs(t, sub.PutObject(ctx, "../../victim/planted.txt", strings.NewReader("x")), storages.ErrUnsafeKey)
		assert.ErrorIs(t, sub.DeleteAll(ctx, ""), storages.ErrUnsafeKey)

		// Nested clones stay guarded however deep the navigation goes.
		nested := mustSub(t, mustSub(t, sub, "deeper", true), "deeper-still", true)
		assert.ErrorIs(t, nested.PutObject(ctx, "../x.txt", strings.NewReader("x")), storages.ErrUnsafeKey)

		exists, err := raw.Exists(ctx, "../victim/planted.txt")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	// An absolute re-root is the documented hatch: the caller is naming a new
	// root rather than addressing an object, so the path is not gated — but keys
	// used through the result still are.
	t.Run("SubStorage with an absolute root", func(t *testing.T) {
		guarded, _ := newGuarded(t)
		other := mustSub(t, guarded, "/victim", false)

		content, err := readAll(other, "secret.txt")
		require.NoError(t, err)
		assert.Equal(t, "SECRET", content)
		assert.ErrorIs(t, other.PutObject(ctx, "../store/planted.txt", strings.NewReader("x")), storages.ErrUnsafeKey)
	})

	t.Run("ListDir", func(t *testing.T) {
		guarded, _ := newGuarded(t)
		require.NoError(t, guarded.PutObject(ctx, "dir/x.txt", strings.NewReader("x")))

		_, dirs, err := guarded.ListDir(ctx)
		require.NoError(t, err)
		require.Len(t, dirs, 1)
		assert.ErrorIs(t, dirs[0].PutObject(ctx, "../../victim/planted.txt", strings.NewReader("x")), storages.ErrUnsafeKey)
	})
}

// A sub-path is a key like any other: SubStorage refuses one that escapes
// instead of handing back a storage rooted outside. Nothing is returned with the
// error, so there is no half-usable storage to guard against.
func TestGuard_SubStorageRejectsEscapingPath(t *testing.T) {
	guarded, raw := newGuarded(t)

	for _, subPath := range []string{"..", "../victim", "a/../../victim", "/victim"} {
		sub, err := guarded.SubStorage(subPath, true)
		assert.ErrorIsf(t, err, storages.ErrUnsafeKey, "SubStorage(%q)", subPath)
		assert.Nilf(t, sub, "SubStorage(%q) must return no storage alongside the error", subPath)
	}

	assertContent(t, raw, "../victim/secret.txt", "SECRET")
}

// mustSub re-roots st, failing the test if the storage refuses the path.
func mustSub(t *testing.T, st storages.Storager, subPath string, relative bool) storages.Storager {
	t.Helper()
	sub, err := st.SubStorage(subPath, relative)
	require.NoErrorf(t, err, "SubStorage(%q, %v)", subPath, relative)
	return sub
}

// Guarding twice would be harmless but pointless, so Guard hands back a storage
// that is already guarded unchanged.
func TestGuard_IsIdempotent(t *testing.T) {
	guarded, _ := newGuarded(t)
	assert.Same(t, guarded, storages.Guard(guarded))
}

// The pass-through methods keep answering from the wrapped storage.
func TestGuard_PassesThroughIdentityAndLifecycle(t *testing.T) {
	guarded, raw := newGuarded(t)

	assert.Equal(t, raw.GetCwd(), guarded.GetCwd())
	assert.Equal(t, raw.Dirname(), guarded.Dirname())
	assert.NoError(t, guarded.Ping(context.Background()))
	assert.NoError(t, guarded.Close())
}

func assertContent(t *testing.T, st storages.Storager, key, want string) {
	t.Helper()
	got, err := readAll(st, key)
	require.NoErrorf(t, err, "reading %q", key)
	assert.Equalf(t, want, got, "content of %q", key)
}

func readAll(st storages.Storager, key string) (string, error) {
	r, err := st.GetObject(context.Background(), key)
	if err != nil {
		return "", err
	}
	defer func() { _ = r.Close() }()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func names(objects []storages.ObjectStat) []string {
	res := make([]string, 0, len(objects))
	for _, o := range objects {
		res = append(res, o.Name)
	}
	return res
}

// quoted names a subtest after the key it covers, so the empty key does not
// produce a nameless subtest.
func quoted(key string) string {
	return `"` + key + `"`
}
