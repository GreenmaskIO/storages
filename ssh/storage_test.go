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
	"os"
	"path"
	"testing"

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

func putObject(t *testing.T, st *Storage, key string, content []byte) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), key, bytes.NewReader(content)))
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

		sub, ok := st.SubStorage("sub", true).(*Storage)
		require.True(t, ok)
		require.NoError(t, sub.Close())

		// The parent shares the same connection, so it is closed too.
		_, err := st.Exists(ctx, "a.txt")
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}
