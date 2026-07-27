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

package directory

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/internal/fsbackend"
	"github.com/greenmaskio/storages/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		// t.TempDir supplies an OS-correct, auto-cleaned root, which also exercises
		// the backend on the host OS's native path conventions.
		st, err := NewStorage(Config{RootPath: t.TempDir()})
		require.NoError(t, err)
		return st
	})
}

// The conformance suite pins that the default storage is guarded. What it
// cannot pin is that the opt-out really opts out — that the guard is a wrapper
// the constructor chooses to apply rather than behavior baked into the backend.
func TestWithUnsafeReachesOutsideTheDirectory(t *testing.T) {
	ctx := context.Background()

	base := t.TempDir()
	root := filepath.Join(base, "store")
	require.NoError(t, os.Mkdir(root, 0o750))
	// A neighbour of the storage root, reachable only by climbing out of it.
	require.NoError(t, os.WriteFile(filepath.Join(base, "victim.txt"), []byte("SECRET"), 0o600))

	guarded, err := NewStorage(Config{RootPath: root})
	require.NoError(t, err)
	_, err = guarded.GetObject(ctx, "../victim.txt")
	assert.ErrorIs(t, err, storages.ErrUnsafeKey, "the default storage must refuse the climb")

	unsafe, err := NewStorage(Config{RootPath: root}, WithUnsafe())
	require.NoError(t, err)
	r, err := unsafe.GetObject(ctx, "../victim.txt")
	require.NoError(t, err, "WithUnsafe hands the key straight to the filesystem")
	defer func() { require.NoError(t, r.Close()) }()
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "SECRET", string(content))
}

// mkdirs creates each slash-separated path under base, as a directory.
func mkdirs(t *testing.T, base string, paths ...string) {
	t.Helper()
	for _, p := range paths {
		require.NoError(t, os.MkdirAll(filepath.Join(base, filepath.FromSlash(p)), 0o750))
	}
}

// mkfile creates a file at the slash-separated path under base.
func mkfile(t *testing.T, base string, p string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(base, filepath.FromSlash(p)), []byte("x"), 0o600))
}

func TestNewStorageRootAndPrefix(t *testing.T) {
	// setup builds a tree under a fresh base dir and returns the config to open.
	// wantRoot and wantAbsent are relative to that base.
	// wantValidateErr marks the cases Validate can see for itself — the shape of
	// the config, not whether the prefix happens to exist.
	tests := []struct {
		name            string
		setup           func(t *testing.T, base string) Config
		opts            []Option
		wantErr         bool
		wantErrIs       error
		wantValidateErr bool
		wantRoot        string
		wantAbsent      []string
	}{
		{
			name: "root path missing",
			setup: func(_ *testing.T, base string) Config {
				return Config{RootPath: filepath.Join(base, "nope")}
			},
			wantErr: true,
			// The option creates the prefix, never the mount point.
			opts:            []Option{WithCreatePrefix()},
			wantValidateErr: true,
			wantAbsent:      []string{"nope"},
		},
		{
			name: "root path is a file",
			setup: func(t *testing.T, base string) Config {
				mkfile(t, base, "root.txt")
				return Config{RootPath: filepath.Join(base, "root.txt")}
			},
			wantErr:         true,
			wantValidateErr: true,
		},
		{
			name: "root path empty",
			setup: func(_ *testing.T, _ string) Config {
				return Config{}
			},
			wantErrIs:       ErrRootPathIsRequired,
			wantValidateErr: true,
		},
		{
			name: "prefix exists",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root/a/b")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "a/b"}
			},
			wantRoot: "root/a/b",
		},
		{
			name: "prefix missing without the option",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "a/b"}
			},
			wantErrIs:  ErrPrefixNotExists,
			wantAbsent: []string{"root/a"},
		},
		{
			name: "prefix partially exists, created",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root/test1")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "test1/test2/test3"}
			},
			opts:     []Option{WithCreatePrefix()},
			wantRoot: "root/test1/test2/test3",
		},
		{
			name: "prefix missing entirely, created",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "a/b/c"}
			},
			opts:     []Option{WithCreatePrefix()},
			wantRoot: "root/a/b/c",
		},
		{
			name: "intermediate prefix segment is a file",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root/test1")
				mkfile(t, base, "root/test1/test2")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "test1/test2/test3"}
			},
			opts:       []Option{WithCreatePrefix()},
			wantErr:    true,
			wantAbsent: []string{"root/test1/test2/test3"},
		},
		{
			name: "prefix names a file",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				mkfile(t, base, "root/a")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "a"}
			},
			opts:    []Option{WithCreatePrefix()},
			wantErr: true,
		},
		{
			name: "prefix is absolute",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: string(filepath.Separator) + "etc"}
			},
			opts:            []Option{WithCreatePrefix()},
			wantErr:         true,
			wantValidateErr: true,
		},
		{
			name: "prefix reaches outside the root",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "../escaped"}
			},
			opts:            []Option{WithCreatePrefix()},
			wantErr:         true,
			wantValidateErr: true,
			wantAbsent:      []string{"escaped"},
		},
		{
			name: "prefix cleans to an escape",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "a/../../escaped"}
			},
			opts:            []Option{WithCreatePrefix()},
			wantErr:         true,
			wantValidateErr: true,
			wantAbsent:      []string{"escaped"},
		},
		{
			name: "empty prefix roots at the root path",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root")}
			},
			// The option is a no-op when there is no prefix to create.
			opts:     []Option{WithCreatePrefix()},
			wantRoot: "root",
		},
		{
			name: "dot prefix roots at the root path",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "."}
			},
			wantRoot: "root",
		},
		{
			name: "interior dot-dot in the prefix is fine",
			setup: func(t *testing.T, base string) Config {
				mkdirs(t, base, "root")
				return Config{RootPath: filepath.Join(base, "root"), Prefix: "a/../b"}
			},
			opts:     []Option{WithCreatePrefix()},
			wantRoot: "root/b",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			cfg := tt.setup(t, base)

			st, err := NewStorage(cfg, tt.opts...)

			if tt.wantErr || tt.wantErrIs != nil {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
				assert.Nil(t, st)
				if tt.wantValidateErr {
					assert.Error(t, cfg.Validate(), "Validate must refuse what NewStorage refuses")
				}
			} else {
				require.NoError(t, err)
				defer func() { require.NoError(t, st.Close()) }()

				wantRoot := filepath.Join(base, filepath.FromSlash(tt.wantRoot))
				assert.Equal(t, filepath.ToSlash(wantRoot), st.GetCwd())
				assert.NoError(t, cfg.Validate())

				// Writes land where the storage says they do, and the guard holds
				// keys inside the prefix rather than the root path.
				require.NoError(t, st.PutObject(ctx, "obj.txt", strings.NewReader("payload")))
				onDisk, err := os.ReadFile(filepath.Join(wantRoot, "obj.txt"))
				require.NoError(t, err)
				assert.Equal(t, "payload", string(onDisk))

				r, err := st.GetObject(ctx, "obj.txt")
				require.NoError(t, err)
				content, err := io.ReadAll(r)
				require.NoError(t, err)
				require.NoError(t, r.Close())
				assert.Equal(t, "payload", string(content))

				_, err = st.GetObject(ctx, "../obj.txt")
				assert.ErrorIs(t, err, storages.ErrUnsafeKey)
			}

			// Not os.ErrNotExist: a path under a file segment stats as ENOTDIR,
			// which is equally "was not created".
			for _, absent := range tt.wantAbsent {
				_, err := os.Stat(filepath.Join(base, filepath.FromSlash(absent)))
				assert.Error(t, err, "%s must not have been created", absent)
			}
		})
	}
}

// A created prefix carries the same mode as the directories the backend makes
// on write. Compared against a reference MkdirAll rather than DirMode itself, so
// the umask applies equally to both.
func TestWithCreatePrefixUsesDirMode(t *testing.T) {
	base := t.TempDir()
	mkdirs(t, base, "root/test1")

	ref := filepath.Join(base, "reference")
	require.NoError(t, os.MkdirAll(ref, fsbackend.DirMode))
	refInfo, err := os.Stat(ref)
	require.NoError(t, err)

	_, err = NewStorage(
		Config{RootPath: filepath.Join(base, "root"), Prefix: "test1/test2/test3"},
		WithCreatePrefix(),
	)
	require.NoError(t, err)

	for _, created := range []string{"root/test1/test2", "root/test1/test2/test3"} {
		info, err := os.Stat(filepath.Join(base, filepath.FromSlash(created)))
		require.NoError(t, err)
		assert.Equal(t, refInfo.Mode().Perm(), info.Mode().Perm(), "mode of %s", created)
	}
}
