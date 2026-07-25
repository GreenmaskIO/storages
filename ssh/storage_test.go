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
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/greenmaskio/storages"
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
