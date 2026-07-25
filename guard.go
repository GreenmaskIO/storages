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

package storages

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
)

// Guard wraps store so that no key can address anything outside it, or name its
// root. Every backend constructor in this module applies it, so a storage is
// guarded unless the caller opted out with that backend's WithUnsafe option;
// implementers of Storager outside this module apply it themselves.
//
// A bare backend resolves a key by joining it onto the storage root and going
// straight to the filesystem or object store, which makes "../../etc/passwd" a
// read outside the storage and DeleteAll("") a removal of the storage itself.
// Guard is the single point every key passes through, so a key assembled from
// untrusted input cannot reach past the storage it was handed to. Keys are
// checked after path.Clean, so an interior "a/../b" is fine — only a key that
// still escapes once cleaned is rejected, with ErrUnsafeKey.
//
// The returned Storager re-wraps what SubStorage and ListDir hand back, so
// navigating away from the root cannot navigate out of the guard: a relative
// SubStorage path that escapes is refused with ErrUnsafeKey rather than rooted
// outside. Guarding an already-guarded storage returns it unchanged.
func Guard(store Storager) Storager {
	if _, ok := store.(*guard); ok {
		return store
	}
	return &guard{store: store}
}

// guard is Guard's decorator. Every method is written out rather than relying on
// an embedded Storager: with embedding, a method left unimplemented here would
// be promoted from the wrapped storage and quietly bypass the gate — a hole the
// compiler would never point at. Written out, a new interface method breaks the
// build until it is gated.
type guard struct{ store Storager }

var _ Storager = (*guard)(nil)

func (g *guard) GetCwd() string { return g.store.GetCwd() }

func (g *guard) Dirname() string { return g.store.Dirname() }

func (g *guard) ListDir(ctx context.Context) (files []string, dirs []Storager, err error) {
	files, dirs, err = g.store.ListDir(ctx)
	for i, dir := range dirs {
		dirs[i] = Guard(dir)
	}
	return files, dirs, err
}

func (g *guard) List(ctx context.Context, prefix string) ([]ObjectStat, error) {
	safe, err := listPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return g.store.List(ctx, safe)
}

func (g *guard) GetObject(ctx context.Context, filePath string) (io.ReadCloser, error) {
	safe, err := objectKey(filePath)
	if err != nil {
		return nil, err
	}
	return g.store.GetObject(ctx, safe)
}

func (g *guard) GetObjectRange(ctx context.Context, filePath string, offset, length int64) (io.ReadCloser, error) {
	safe, err := objectKey(filePath)
	if err != nil {
		return nil, err
	}
	return g.store.GetObjectRange(ctx, safe, offset, length)
}

func (g *guard) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	safe, err := objectKey(filePath)
	if err != nil {
		return err
	}
	return g.store.PutObject(ctx, safe, body)
}

func (g *guard) Delete(ctx context.Context, filePaths ...string) error {
	// Gate every path before deleting any of them, matching Delete's own promise
	// that one bad path leaves the storage untouched.
	safe := make([]string, len(filePaths))
	for i, filePath := range filePaths {
		key, err := objectKey(filePath)
		if err != nil {
			return err
		}
		safe[i] = key
	}
	return g.store.Delete(ctx, safe...)
}

func (g *guard) DeleteAll(ctx context.Context, pathPrefix string) error {
	// DeleteAll takes a prefix, but it is gated as an object key: an empty or "."
	// prefix names the storage root, and removing that removes the storage.
	safe, err := objectKey(pathPrefix)
	if err != nil {
		return err
	}
	return g.store.DeleteAll(ctx, safe)
}

func (g *guard) Exists(ctx context.Context, fileName string) (bool, error) {
	safe, err := objectKey(fileName)
	if err != nil {
		return false, err
	}
	return g.store.Exists(ctx, safe)
}

func (g *guard) SubStorage(subPath string, relative bool) (Storager, error) {
	// A relative sub-path is joined onto the cwd, so ".." in it walks out of the
	// storage exactly as it would in an object key. An absolute re-root is the
	// documented hatch out of this storage's tree — the caller is naming a root,
	// not addressing an object inside one — so it is not gated; the result is
	// guarded either way, and keys used through it stay inside the new root.
	if relative {
		safe, err := listPrefix(subPath)
		if err != nil {
			return nil, err
		}
		subPath = safe
	}
	sub, err := g.store.SubStorage(subPath, relative)
	if err != nil {
		return nil, err
	}
	return Guard(sub), nil
}

func (g *guard) Stat(fileName string) (*ObjectStat, error) {
	safe, err := objectKey(fileName)
	if err != nil {
		return nil, err
	}
	return g.store.Stat(safe)
}

func (g *guard) Ping(ctx context.Context) error { return g.store.Ping(ctx) }

func (g *guard) Close() error { return g.store.Close() }

// objectKey gates a key that names an object. Besides escaping the storage, a
// key that names the storage root itself is rejected: it is never a valid object
// path, and DeleteAll("") would take the whole storage down with it.
func objectKey(key string) (string, error) {
	cleaned, err := clean(key)
	if err != nil {
		return "", err
	}
	if cleaned == "" {
		return "", fmt.Errorf("%w: %q names the storage root", ErrUnsafeKey, key)
	}
	return cleaned, nil
}

// listPrefix gates a directory-like prefix. It applies the same checks as
// objectKey minus the root one: listing or re-rooting at the storage root is an
// ordinary request that destroys nothing.
func listPrefix(prefix string) (string, error) {
	return clean(prefix)
}

// clean normalizes key and reports the checks both gates share. The storage root
// comes back as "", which every backend reads as "no prefix" — path.Clean's "."
// does not survive the string concatenation the object stores build keys with.
func clean(key string) (string, error) {
	// Normalize separators first. Backends do the same before resolving a key, so
	// on Windows `..\secret` would otherwise reach the filesystem as `../secret`
	// having passed a gate that only ever looked for forward slashes.
	key = filepath.ToSlash(key)
	if path.IsAbs(key) {
		return "", fmt.Errorf("%w: %q is absolute, keys are relative to the storage root", ErrUnsafeKey, key)
	}
	if key == "" {
		return "", nil
	}
	cleaned := path.Clean(key)
	if escapes(cleaned) {
		return "", fmt.Errorf("%w: %q escapes the storage", ErrUnsafeKey, key)
	}
	if cleaned == "." {
		return "", nil
	}
	return cleaned, nil
}

// escapes reports whether a cleaned path leads out of the storage. path.Clean
// collapses every interior "..", so anything that still walks up shows as a
// leading one.
func escapes(cleaned string) bool {
	return cleaned == ".." || strings.HasPrefix(cleaned, "../")
}
