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

// Package ssh exercises the ssh backend end to end against a real OpenSSH
// server (atmoz/sftp) in a container.
package ssh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	cryptossh "golang.org/x/crypto/ssh"

	"github.com/greenmaskio/storages"
	sshstorage "github.com/greenmaskio/storages/ssh"
)

// --- atmoz/sftp testcontainer harness --------------------------------------
//
// A single SFTP container is shared across all round-trip tests (started lazily
// on first use, terminated in TestMain). Each test gets its own unique prefix
// via newTestStorage so cases stay isolated.

const (
	sftpImage    = "atmoz/sftp:alpine"
	sftpUser     = "testuser"
	sftpPassword = "testpass"

	// sftpReadyTimeout bounds both the container wait strategy and the SSH
	// handshake probe. Host-key generation dominates the startup time and is
	// slow on shared CI runners, so keep this generous.
	sftpReadyTimeout = 2 * time.Minute
)

var (
	sftpOnce sync.Once
	sftpCtr  testcontainers.Container
	sftpHost string
	sftpPort int
	sftpErr  error

	prefixCounter atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sftpCtr != nil {
		_ = sftpCtr.Terminate(context.Background())
	}
	os.Exit(code)
}

// sftpEndpoint lazily starts the shared atmoz/sftp container and returns its
// host and mapped port. The command provisions user testuser/testpass with a
// writable /upload directory.
//
// The image generates its SSH host keys on first boot, which takes seconds on a
// loaded CI runner. Docker's userland proxy binds the mapped host port as soon
// as the container starts and resets connections until sshd is actually
// listening, so "the port accepts a TCP connection" is not a usable readiness
// signal here — it is satisfied long before the handshake can succeed. We wait
// for sshd's own startup log line and then confirm with a real SSH handshake.
func sftpEndpoint(t *testing.T) (string, int) {
	t.Helper()
	sftpOnce.Do(func() {
		ctx := context.Background()
		req := testcontainers.ContainerRequest{
			Image:        sftpImage,
			ExposedPorts: []string{"22/tcp"},
			Cmd:          []string{fmt.Sprintf("%s:%s:::upload", sftpUser, sftpPassword)},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("22/tcp"),
				wait.ForLog("Server listening on 0.0.0.0 port 22"),
			).WithDeadline(sftpReadyTimeout),
		}
		sftpCtr, sftpErr = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if sftpErr != nil {
			return
		}
		sftpHost, sftpErr = sftpCtr.Host(ctx)
		if sftpErr != nil {
			return
		}
		mapped, err := sftpCtr.MappedPort(ctx, "22/tcp")
		if err != nil {
			sftpErr = err
			return
		}
		sftpPort = int(mapped.Num())
		sftpErr = waitForSSHHandshake(ctx, sftpHost, sftpPort)
	})
	require.NoError(t, sftpErr)
	return sftpHost, sftpPort
}

// waitForSSHHandshake polls the mapped port with full SSH handshakes until one
// succeeds. This is the readiness check that actually matters: it only passes
// once sshd has its host keys and is serving on the mapped port.
func waitForSSHHandshake(ctx context.Context, host string, port int) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	ctx, cancel := context.WithTimeout(ctx, sftpReadyTimeout)
	defer cancel()

	var lastErr error
	for {
		client, err := cryptossh.Dial("tcp", addr, clientConfig())
		if err == nil {
			return client.Close()
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("SFTP container never accepted an SSH connection on %s: %w", addr, lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func clientConfig() *cryptossh.ClientConfig {
	return &cryptossh.ClientConfig{
		User:            sftpUser,
		Auth:            []cryptossh.AuthMethod{cryptossh.Password(sftpPassword)},
		HostKeyCallback: cryptossh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}
}

// newTestStorage returns a Storage pointed at a unique prefix under /upload in
// the shared SFTP container.
func newTestStorage(t *testing.T) *sshstorage.Storage {
	t.Helper()
	host, port := sftpEndpoint(t)
	st, err := sshstorage.NewStorage(sshstorage.Config{
		Host:     host,
		Port:     port,
		User:     sftpUser,
		Password: sftpPassword,
		Prefix:   fmt.Sprintf("/upload/test-%d", prefixCounter.Add(1)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// rawSFTPClient opens an SFTP session to the container that is independent of
// the storage backend, for assertions about the protocol itself.
func rawSFTPClient(t *testing.T) *sftp.Client {
	t.Helper()
	host, port := sftpEndpoint(t)

	conn, err := cryptossh.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(port)), clientConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client, err := sftp.NewClient(conn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func putObject(t *testing.T, st *sshstorage.Storage, key string, content []byte) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), key, bytes.NewReader(content)))
}

func mustGet(t *testing.T, st *sshstorage.Storage, key string) []byte {
	t.Helper()
	r, err := st.GetObject(context.Background(), key)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

func dirNames(dirs []storages.Storager) []string {
	names := make([]string, 0, len(dirs))
	for _, d := range dirs {
		names = append(names, d.Dirname())
	}
	return names
}

// --- Tests -----------------------------------------------------------------

func TestSSHOps(t *testing.T) {
	ctx := context.Background()

	t.Run("new storage", func(t *testing.T) {
		st := newTestStorage(t)
		// Lazy-connect succeeds (Exists forces the connection).
		_, err := st.Exists(ctx, "anything")
		require.NoError(t, err)
	})

	t.Run("put object", func(t *testing.T) {
		st := newTestStorage(t)
		putObject(t, st, "test.txt", []byte("root"))
		putObject(t, st, "testdb/test.txt", []byte("nested"))
		assert.Equal(t, []byte("root"), mustGet(t, st, "test.txt"))
		assert.Equal(t, []byte("nested"), mustGet(t, st, "testdb/test.txt"))
	})

	t.Run("get object", func(t *testing.T) {
		st := newTestStorage(t)
		payload := []byte("hello sftp world")
		putObject(t, st, "payload.bin", payload)
		assert.Equal(t, payload, mustGet(t, st, "payload.bin"))
	})

	t.Run("walking", func(t *testing.T) {
		st := newTestStorage(t)
		putObject(t, st, "root.txt", []byte("r"))
		putObject(t, st, "sub/nested.txt", []byte("n"))

		files, dirs, err := st.ListDir(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"root.txt"}, files)
		assert.ElementsMatch(t, []string{"sub"}, dirNames(dirs))

		require.Len(t, dirs, 1)
		child := dirs[0]
		assert.Equal(t, path.Join(st.GetCwd(), "sub"), child.GetCwd())
		childFiles, _, err := child.ListDir(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"nested.txt"}, childFiles)
	})

	t.Run("exists", func(t *testing.T) {
		st := newTestStorage(t)
		putObject(t, st, "a.txt", []byte("a"))
		putObject(t, st, "d/b.txt", []byte("b"))

		ok, err := st.Exists(ctx, "a.txt")
		require.NoError(t, err)
		assert.True(t, ok)

		ok, err = st.Exists(ctx, "d/b.txt")
		require.NoError(t, err)
		assert.True(t, ok)

		ok, err = st.Exists(ctx, "missing.txt")
		require.NoError(t, err)
		assert.False(t, ok)

		require.NoError(t, st.Delete(ctx, "a.txt"))
		ok, err = st.Exists(ctx, "a.txt")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("sub storage", func(t *testing.T) {
		st := newTestStorage(t)
		sub := st.SubStorage("sub", true)
		content := []byte("sub-storage-payload")
		require.NoError(t, sub.PutObject(ctx, "test.txt", bytes.NewReader(content)))

		reader, err := sub.GetObject(ctx, "test.txt")
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, data)

		assert.Equal(t, content, mustGet(t, st, "sub/test.txt"))
	})

	t.Run("stat", func(t *testing.T) {
		st := newTestStorage(t)
		putObject(t, st, "s.txt", []byte("data"))

		stat, err := st.Stat("s.txt")
		require.NoError(t, err)
		assert.True(t, stat.Exist)
		assert.Equal(t, path.Join(st.GetCwd(), "s.txt"), stat.Name)
		assert.False(t, stat.LastModified.IsZero())

		missing, err := st.Stat("nope.txt")
		require.NoError(t, err)
		assert.False(t, missing.Exist)
	})

	t.Run("not found", func(t *testing.T) {
		st := newTestStorage(t)
		_, err := st.GetObject(ctx, "ghost.txt")
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})

	t.Run("delete", func(t *testing.T) {
		st := newTestStorage(t)
		putObject(t, st, "a.txt", []byte("a"))
		putObject(t, st, "b.txt", []byte("b"))

		require.NoError(t, st.Delete(ctx, "a.txt"))
		// Deleting a missing file reports it against a real SFTP server.
		assert.ErrorIs(t, st.Delete(ctx, "ghost.txt"), storages.ErrFileNotFound)

		ok, err := st.Exists(ctx, "a.txt")
		require.NoError(t, err)
		assert.False(t, ok)
		ok, err = st.Exists(ctx, "b.txt")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("delete_all", func(t *testing.T) {
		st := newTestStorage(t)
		putObject(t, st, "books/a.txt", []byte("a"))
		putObject(t, st, "books/sci/b.txt", []byte("b"))
		putObject(t, st, "users/u.txt", []byte("u"))

		require.NoError(t, st.DeleteAll(ctx, "books"))

		remaining, err := storages.Walk(ctx, st)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"users/u.txt"}, remaining)

		// DeleteAll must remove the now-empty directory tree, not just the
		// files — otherwise a deleted dump still surfaces in list-dumps as an
		// empty, status-less directory.
		_, dirs, err := st.ListDir(ctx)
		require.NoError(t, err)
		assert.NotContains(t, dirNames(dirs), "books", "empty dump directory must be removed")

		exists, err := st.Exists(ctx, "books")
		require.NoError(t, err)
		assert.False(t, exists, "the prefix directory itself must be gone")
	})

	t.Run("delete_all nested subdirs", func(t *testing.T) {
		st := newTestStorage(t)
		// A multi-level tree under dump/, with branches and files at several
		// depths, plus a sibling tree (keep/) that must survive untouched.
		putObject(t, st, "dump/meta.json", []byte("m"))
		putObject(t, st, "dump/data/a.dat", []byte("a"))
		putObject(t, st, "dump/data/blobs/b.dat", []byte("b"))
		putObject(t, st, "dump/data/blobs/deep/c.dat", []byte("c"))
		putObject(t, st, "dump/schema/tables/t.sql", []byte("t"))
		putObject(t, st, "keep/k.txt", []byte("k"))

		require.NoError(t, st.DeleteAll(ctx, "dump"))

		// Every file under dump/ is gone; the sibling tree is untouched.
		remaining, err := storages.Walk(ctx, st)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"keep/k.txt"}, remaining)

		// No part of the nested directory tree may linger — not the prefix dir
		// nor any intermediate subdirectory.
		_, dirs, err := st.ListDir(ctx)
		require.NoError(t, err)
		assert.NotContains(t, dirNames(dirs), "dump", "the prefix directory must be removed")

		for _, dir := range []string{"dump", "dump/data", "dump/data/blobs", "dump/data/blobs/deep", "dump/schema", "dump/schema/tables"} {
			exists, err := st.Exists(ctx, dir)
			require.NoError(t, err)
			assert.Falsef(t, exists, "nested directory %q must be removed", dir)
		}
	})

	t.Run("remove on a directory errors", func(t *testing.T) {
		// The backend's removeAll relies on SFTP Remove being a file-only op
		// (RemoveDirectory handles dirs). This guards that invariant against a
		// real server: calling Remove on a directory must fail rather than
		// silently no-op or delete recursively. If this ever stops failing,
		// removeAll's file/dir branching can rot.
		st := newTestStorage(t)
		putObject(t, st, "adir/child.txt", []byte("c"))

		client := rawSFTPClient(t)
		dirPath := path.Join(st.GetCwd(), "adir")

		require.Error(t, client.Remove(dirPath), "SFTP Remove must reject a directory")

		// RemoveDirectory is also strict: it refuses a non-empty directory.
		require.Error(t, client.RemoveDirectory(dirPath), "RemoveDirectory must reject a non-empty directory")

		// The directory and its contents survive the failed removals.
		exists, err := st.Exists(ctx, "adir/child.txt")
		require.NoError(t, err)
		assert.True(t, exists, "failed directory removal must leave contents intact")
	})
}
