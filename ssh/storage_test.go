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

package ssh

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/greenmaskio/storages"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// The end-to-end behavior of this backend against a real OpenSSH server lives in
// the tests/integration module, which keeps testcontainers out of this module's
// dependency graph. What stays here needs no server at all, or only the
// in-process one from testserver_test.go.

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *Config
		wantErr  bool
		wantPort int
	}{
		{
			name:    "missing host",
			cfg:     &Config{User: "u", Password: "p", Port: 22},
			wantErr: true,
		},
		{
			name:    "missing user",
			cfg:     &Config{Host: "h", Password: "p", Port: 22},
			wantErr: true,
		},
		{
			name:    "missing auth",
			cfg:     &Config{Host: "h", User: "u", Port: 22},
			wantErr: true,
		},
		{
			name:     "valid with password",
			cfg:      &Config{Host: "h", User: "u", Password: "p", Port: 22},
			wantErr:  false,
			wantPort: 22,
		},
		{
			name:     "valid with private key path",
			cfg:      &Config{Host: "h", User: "u", PrivateKeyPath: "/key", Port: 22},
			wantErr:  false,
			wantPort: 22,
		},
		{
			name:     "non-positive port clamps to default",
			cfg:      &Config{Host: "h", User: "u", Password: "p", Port: 0},
			wantErr:  false,
			wantPort: defaultPort,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantPort, tt.cfg.Port)
		})
	}
}

func TestDefaultConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, defaultPort, cfg.Port)
}

func TestBuildAuthMethods(t *testing.T) {
	keyPath := writeTestPrivateKey(t)

	t.Run("both: private key precedes password", func(t *testing.T) {
		methods, err := buildAuthMethods(Config{
			User:           "u",
			Password:       "p",
			PrivateKeyPath: keyPath,
		})
		require.NoError(t, err)
		// Two methods, with the public-key method first.
		require.Len(t, methods, 2)
		assert.Equal(t, "publickey", reflectMethodName(methods[0]))
		assert.Equal(t, "password", reflectMethodName(methods[1]))
	})

	t.Run("password only", func(t *testing.T) {
		methods, err := buildAuthMethods(Config{User: "u", Password: "p"})
		require.NoError(t, err)
		require.Len(t, methods, 1)
		assert.Equal(t, "password", reflectMethodName(methods[0]))
	})

	t.Run("key only", func(t *testing.T) {
		methods, err := buildAuthMethods(Config{User: "u", PrivateKeyPath: keyPath})
		require.NoError(t, err)
		require.Len(t, methods, 1)
		assert.Equal(t, "publickey", reflectMethodName(methods[0]))
	})

	t.Run("unreadable key errors", func(t *testing.T) {
		_, err := buildAuthMethods(Config{User: "u", PrivateKeyPath: path.Join(t.TempDir(), "nope")})
		assert.Error(t, err)
	})

	t.Run("malformed key errors", func(t *testing.T) {
		bad := path.Join(t.TempDir(), "bad_key")
		require.NoError(t, os.WriteFile(bad, []byte("not a key"), 0o600))
		_, err := buildAuthMethods(Config{User: "u", PrivateKeyPath: bad})
		assert.Error(t, err)
	})
}

// reflectMethodName reports the SSH auth method name ("publickey" / "password")
// of an ssh.AuthMethod. The concrete types are unexported, so we identify them
// by their method() string, which ssh.AuthMethod exposes via the wire name.
func reflectMethodName(m ssh.AuthMethod) string {
	switch fmt.Sprintf("%T", m) {
	case "ssh.publicKeyCallback":
		return "publickey"
	case "ssh.passwordCallback":
		return "password"
	default:
		return fmt.Sprintf("%T", m)
	}
}

// writeTestPrivateKey generates an ed25519 OpenSSH private key, writes it to a
// temp file and returns the path.
func writeTestPrivateKey(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(priv, "")
	require.NoError(t, err)
	keyPath := path.Join(t.TempDir(), "id_ed25519")
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(block), 0o600))
	return keyPath
}

func putObject(t *testing.T, st storages.Storager, key string, content []byte) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), key, bytes.NewReader(content)))
}

// Ranged reads and flat listings are SFTP protocol work — offsets travel in the
// read requests and the recursion is client-side — so they are exercised against
// the in-process server rather than only in the integration module.
func TestStorage_GetObjectRange(t *testing.T) {
	ctx := context.Background()
	const content = "0123456789abcdefghij"

	read := func(t *testing.T, st storages.Storager, offset, length int64) string {
		t.Helper()
		r, err := st.GetObjectRange(ctx, "ranged.txt", offset, length)
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		return string(got)
	}

	t.Run("reads the requested slice", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "ranged.txt", []byte(content))

		assert.Equal(t, "5678", read(t, st, 5, 4))
		assert.Equal(t, "fghij", read(t, st, 15, -1), "a negative length reads to the end")
		assert.Equal(t, "ghij", read(t, st, 16, 100), "a range past the end is clamped")
	})

	t.Run("impossible ranges are rejected", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "ranged.txt", []byte(content))
		putObject(t, st, "empty.txt", nil)

		for _, tt := range []struct {
			name           string
			file           string
			offset, length int64
		}{
			{name: "offset at size", file: "ranged.txt", offset: int64(len(content)), length: 1},
			{name: "offset past size", file: "ranged.txt", offset: int64(len(content)) + 100, length: 1},
			{name: "zero length", file: "ranged.txt", offset: 0, length: 0},
			{name: "negative offset", file: "ranged.txt", offset: -1, length: 4},
			{name: "any range on an empty object", file: "empty.txt", offset: 0, length: 1},
		} {
			t.Run(tt.name, func(t *testing.T) {
				_, err := st.GetObjectRange(ctx, tt.file, tt.offset, tt.length)
				assert.ErrorIs(t, err, storages.ErrInvalidRange)
			})
		}
	})

	t.Run("missing object is not found", func(t *testing.T) {
		st := newLocalStorage(t)
		_, err := st.GetObjectRange(ctx, "missing.txt", 0, 4)
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})
}

// TestStorage_Copy covers Copy against the in-process server, which lacks
// posix-rename, so the plain-rename fallback is what runs.
func TestStorage_Copy(t *testing.T) {
	ctx := context.Background()

	// names lists every file in st, leftover .copy- temporaries included.
	names := func(t *testing.T, st storages.Storager) []string {
		t.Helper()
		objects, err := st.List(ctx, "")
		require.NoError(t, err)
		var res []string
		for _, o := range objects {
			res = append(res, o.Name)
		}
		return res
	}

	t.Run("copies and overwrites, leaving no temporary behind", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "src.txt", []byte("new"))
		putObject(t, st, "dir/dst.txt", []byte("old and longer"))

		stat, err := st.Copy(ctx, "src.txt", "dir/dst.txt")
		require.NoError(t, err)

		assert.Equal(t, storages.CopyStat{Bytes: int64(len("new")), Streamed: true}, stat,
			"ssh has no server-side copy, so every copy is streamed")

		assert.Equal(t, "new", readObject(t, st, "dir/dst.txt"))
		assert.Equal(t, "new", readObject(t, st, "src.txt"))
		assert.Equal(t, []string{"dir/dst.txt", "src.txt"}, names(t, st))
	})

	t.Run("a symlink to the source does not empty it", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlinks need extra privileges on Windows")
		}
		st := newLocalStorage(t)
		putObject(t, st, "src.txt", []byte("keep me"))
		require.NoError(t, os.Symlink(path.Join(st.GetCwd(), "src.txt"), path.Join(st.GetCwd(), "link.txt")))

		_, err := st.Copy(ctx, "src.txt", "link.txt")
		require.NoError(t, err)

		assert.Equal(t, "keep me", readObject(t, st, "src.txt"))
		assert.Equal(t, "keep me", readObject(t, st, "link.txt"))
	})

	// An unguarded storage passes keys uncleaned; the temporary must still land
	// next to the cleaned destination.
	t.Run("an uncleaned destination key", func(t *testing.T) {
		for _, dst := range []string{"dir/dst.txt/", "dir/sub/../dst.txt", "dir//dst.txt"} {
			t.Run(dst, func(t *testing.T) {
				st := newLocalRawStorage(t)
				putObject(t, st, "src.txt", []byte("new"))
				putObject(t, st, "dir/other.txt", []byte("o"))

				_, err := st.Copy(ctx, "src.txt", dst)
				require.NoError(t, err)

				assert.Equal(t, "new", readObject(t, st, "dir/dst.txt"))
				assert.Equal(t, []string{"dir/dst.txt", "dir/other.txt", "src.txt"}, names(t, st))
				_, dirs, err := mustSubRaw(t, st, "dir").ListDir(ctx)
				require.NoError(t, err)
				assert.Empty(t, dirs, "no stray directory is left behind")
			})
		}
	})

	t.Run("a cancelled copy stops and leaves no temporary behind", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "src.txt", bytes.Repeat([]byte("x"), 1<<20))
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		_, err := st.Copy(cancelled, "src.txt", "dir/dst.txt")

		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, []string{"src.txt"}, names(t, st))
	})

	t.Run("a failed copy leaves no temporary behind", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "dir/a.txt", []byte("a"))

		_, err := st.Copy(ctx, "dir", "copy.txt")
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
		_, err = st.Copy(ctx, "missing.txt", "copy.txt")
		assert.ErrorIs(t, err, storages.ErrFileNotFound)

		assert.Equal(t, []string{"dir/a.txt"}, names(t, st))
	})

	t.Run("a directory destination is an error and stays in place", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "src.txt", []byte("src"))
		putObject(t, st, "dir/a.txt", []byte("a"))

		_, err := st.Copy(ctx, "src.txt", "dir")
		require.ErrorContains(t, err, "is a directory")

		assert.Equal(t, []string{"dir/a.txt", "src.txt"}, names(t, st))
	})
}

func TestStorage_PutObject(t *testing.T) {
	ctx := context.Background()

	names := func(t *testing.T, st storages.Storager) []string {
		t.Helper()
		objects, err := st.List(ctx, "")
		require.NoError(t, err)
		var res []string
		for _, o := range objects {
			res = append(res, o.Name)
		}
		return res
	}

	t.Run("a failed read keeps the previous object", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "obj.txt", []byte("old"))

		boom := errors.New("boom")
		body := io.MultiReader(strings.NewReader("new-partial"), iotest.ErrReader(boom))
		require.ErrorIs(t, st.PutObject(ctx, "obj.txt", body), boom)

		assert.Equal(t, "old", readObject(t, st, "obj.txt"))
		assert.Equal(t, []string{"obj.txt"}, names(t, st))
	})

	t.Run("a cancelled put keeps the previous object", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "obj.txt", []byte("old"))

		cctx, cancel := context.WithCancel(ctx)
		cancel()
		pr, pw := io.Pipe() // never written: a read would block forever
		defer func() { require.NoError(t, pw.Close()) }()
		require.ErrorIs(t, st.PutObject(cctx, "obj.txt", pr), context.Canceled)

		assert.Equal(t, []string{"obj.txt"}, names(t, st), "the temporary file is removed")
		assert.Equal(t, "old", readObject(t, st, "obj.txt"))
	})
}

func readObject(t *testing.T, st storages.Storager, key string) string {
	t.Helper()
	r, err := st.GetObject(context.Background(), key)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(got)
}

func TestStorage_List(t *testing.T) {
	ctx := context.Background()

	names := func(objects []storages.ObjectStat) []string {
		res := make([]string, 0, len(objects))
		for _, o := range objects {
			res = append(res, o.Name)
		}
		return res
	}

	t.Run("flattens the tree, sorted, with sizes", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "a.txt", []byte("1"))
		putObject(t, st, "dir/b.txt", []byte("22"))
		putObject(t, st, "dir/sub/c.txt", []byte("333"))

		objects, err := st.List(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"a.txt", "dir/b.txt", "dir/sub/c.txt"}, names(objects))
		assert.Equal(t, []int64{1, 2, 3}, []int64{objects[0].Size, objects[1].Size, objects[2].Size})
		for _, o := range objects {
			assert.True(t, o.Exist)
			assert.False(t, o.LastModified.IsZero())
		}

		// A prefix roots the listing: names come back relative to it.
		nested, err := st.List(ctx, "dir")
		require.NoError(t, err)
		assert.Equal(t, []string{"b.txt", "sub/c.txt"}, names(nested))
	})

	t.Run("a prefix holding nothing is an empty listing, not an error", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "kept/a.txt", []byte("a"))

		objects, err := st.List(ctx, "never_existed")
		require.NoError(t, err)
		assert.Empty(t, objects)

		// The prefix is directory-like, so one naming a file lists nothing rather
		// than failing on a ReadDir of a non-directory.
		objects, err = st.List(ctx, "kept/a.txt")
		require.NoError(t, err)
		assert.Empty(t, objects)
	})
}

// Close is about this backend's own lifecycle bookkeeping rather than any
// server-side behavior, so it runs against the in-process server.
func TestStorage_Close(t *testing.T) {
	ctx := context.Background()

	t.Run("close releases the connection and blocks further use", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "a.txt", []byte("a")) // forces the connection

		require.NoError(t, st.Close())

		// After Close, operations fail instead of using a dead connection.
		_, err := st.Exists(ctx, "a.txt")
		require.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("close is idempotent", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "a.txt", []byte("a"))
		require.NoError(t, st.Close())
		require.NoError(t, st.Close())
	})

	t.Run("close before connecting is a no-op", func(t *testing.T) {
		st := newLocalStorage(t) // never triggers a connection
		require.NoError(t, st.Close())
	})

	t.Run("closing through a sub storage releases the shared connection", func(t *testing.T) {
		st := newLocalStorage(t)
		putObject(t, st, "a.txt", []byte("a"))

		sub, err := st.SubStorage("sub", true)
		require.NoError(t, err)
		require.NoError(t, sub.Close())

		// The parent shares the same connection, so it is closed too.
		_, err = st.Exists(ctx, "a.txt")
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}

// The guard itself is tested once, in the root package, over a backend-agnostic
// decorator. What this backend has to pin is only that its constructor applies
// it — that a caller who does nothing special gets a storage no key can climb
// out of. The connection is lazy and the key is refused before it is needed, so
// no host is contacted.
func TestNewStorage_GuardsKeysByDefault(t *testing.T) {
	ctx := context.Background()

	cfg := Config{
		Host:     "127.0.0.1",
		Port:     1, // never dialled
		User:     "irrelevant",
		Password: "irrelevant",
		Prefix:   "/srv/dumps",
	}

	st, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	_, err = st.GetObject(ctx, "../../escape.txt")
	assert.ErrorIs(t, err, storages.ErrUnsafeKey)
	assert.ErrorIs(t, st.DeleteAll(ctx, ""), storages.ErrUnsafeKey)

	// WithUnsafe hands back the bare backend instead.
	unsafe, err := New(cfg, WithUnsafe())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, unsafe.Close()) })
	_, ok := unsafe.(*Storage)
	assert.True(t, ok, "WithUnsafe must yield the bare backend")
}

// renameFS is an SFTPClient double for driving replaceFile through failures;
// only the methods replaceFile uses are implemented.
type renameFS struct {
	SFTPClient
	files map[string]string
	// posixErr is what PosixRename answers; nil performs it.
	posixErr error
	// failRename makes Rename(from, to) fail for the listed from→to pairs.
	failRename map[[2]string]bool
}

func (f *renameFS) PosixRename(from, to string) error {
	if f.posixErr != nil {
		return f.posixErr
	}
	return f.move(from, to)
}

func (f *renameFS) Rename(from, to string) error {
	if f.failRename[[2]string{from, to}] {
		return errors.New("rename refused")
	}
	if _, ok := f.files[to]; ok {
		return errors.New("target exists")
	}
	return f.move(from, to)
}

func (f *renameFS) Remove(p string) error {
	if _, ok := f.files[p]; !ok {
		return os.ErrNotExist
	}
	delete(f.files, p)
	return nil
}

func (f *renameFS) move(from, to string) error {
	content, ok := f.files[from]
	if !ok {
		return os.ErrNotExist
	}
	delete(f.files, from)
	f.files[to] = content
	return nil
}

// asideFile returns the one file replaceFile parked dst under, if any.
func (f *renameFS) asideFile(t *testing.T) (string, bool) {
	t.Helper()
	for p := range f.files {
		if strings.HasPrefix(path.Base(p), ".copy-") {
			return p, true
		}
	}
	return "", false
}

func TestStorage_replaceFile(t *testing.T) {
	st := &Storage{logger: slog.New(slog.DiscardHandler)}
	unsupported := &sftp.StatusError{Code: uint32(sftp.ErrSSHFxOpUnsupported)}

	t.Run("posix-rename replaces the destination", func(t *testing.T) {
		fs := &renameFS{files: map[string]string{"/d/tmp": "new", "/d/dst": "old"}}
		require.NoError(t, st.replaceFile(fs, "/d/tmp", "/d/dst"))
		assert.Equal(t, map[string]string{"/d/dst": "new"}, fs.files)
	})

	// Servers answer posix-rename as unsupported or with a plain failure.
	for name, posixErr := range map[string]error{"unsupported": unsupported, "failure": errors.New("failure")} {
		t.Run("fallback after posix-rename "+name, func(t *testing.T) {
			fs := &renameFS{files: map[string]string{"/d/tmp": "new", "/d/dst": "old"}, posixErr: posixErr}
			require.NoError(t, st.replaceFile(fs, "/d/tmp", "/d/dst"))
			assert.Equal(t, map[string]string{"/d/dst": "new"}, fs.files, "the parked destination is removed")
		})
	}

	t.Run("fallback with no destination yet", func(t *testing.T) {
		fs := &renameFS{files: map[string]string{"/d/tmp": "new"}, posixErr: unsupported}
		require.NoError(t, st.replaceFile(fs, "/d/tmp", "/d/dst"))
		assert.Equal(t, map[string]string{"/d/dst": "new"}, fs.files)
	})

	t.Run("a failed swap puts the destination back", func(t *testing.T) {
		fs := &renameFS{
			files:      map[string]string{"/d/tmp": "new", "/d/dst": "old"},
			posixErr:   unsupported,
			failRename: map[[2]string]bool{{"/d/tmp", "/d/dst"}: true},
		}
		require.Error(t, st.replaceFile(fs, "/d/tmp", "/d/dst"))
		assert.Equal(t, map[string]string{"/d/tmp": "new", "/d/dst": "old"}, fs.files)
	})

	t.Run("a failed restore says where the destination went", func(t *testing.T) {
		fs := &renameFS{
			files:    map[string]string{"/d/tmp": "new", "/d/dst": "old"},
			posixErr: unsupported,
		}
		// Fail the swap, and the restore from whatever the parked name is.
		fs.failRename = map[[2]string]bool{{"/d/tmp", "/d/dst"}: true}
		restore := &restoreFailer{renameFS: fs}

		err := st.replaceFile(restore, "/d/tmp", "/d/dst")

		require.Error(t, err)
		aside, ok := fs.asideFile(t)
		require.True(t, ok, "the previous destination must still exist somewhere")
		assert.Equal(t, "old", fs.files[aside])
		assert.Contains(t, err.Error(), aside)
	})
}

// restoreFailer fails any rename back onto /d/dst from a parked .copy- file.
type restoreFailer struct{ *renameFS }

func (r *restoreFailer) Rename(from, to string) error {
	if to == "/d/dst" && strings.HasPrefix(path.Base(from), ".copy-") {
		return errors.New("restore refused")
	}
	return r.renameFS.Rename(from, to)
}

// mustSubRaw re-roots st at subPath, failing the test if the storage refuses.
func mustSubRaw(t *testing.T, st storages.Storager, subPath string) storages.Storager {
	t.Helper()
	sub, err := st.SubStorage(subPath, true)
	require.NoError(t, err)
	return sub
}
