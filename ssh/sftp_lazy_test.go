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
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// sftpLazy is the one piece of shared mutable state in this backend: a single
// instance is handed to every SubStorage clone, so its methods can be called
// from several goroutines at once. These tests drive that concurrency
// deliberately — they are the reason ./ssh/... is worth running under -race.

// --- Concurrency harness ---------------------------------------------------

// concurrency is the number of goroutines each test fans out to. It only has to
// be large enough that unsynchronized access is likely to interleave; the race
// detector does the actual detecting.
const concurrency = 16

// fanOut runs fn from `concurrency` goroutines released at the same moment, so
// they contend rather than trickle through one at a time. fn runs off the test
// goroutine, so it must assert (never require).
func fanOut(fn func(i int)) {
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := range concurrency {
		done.Go(func() {
			start.Wait()
			fn(i)
		})
	}
	start.Done()
	done.Wait()
}

// rejectingListener starts a TCP listener that accepts connections and closes
// them immediately, failing the SSH handshake. It returns the address and a
// counter of accepted connections, which is what lets these tests assert that
// the lazy connection is established exactly once — no container required.
func rejectingListener(t *testing.T) (addr string, accepted *atomic.Int64) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	accepted = &atomic.Int64{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed by the cleanup below
			}
			accepted.Add(1)
			_ = conn.Close()
		}
	}()

	t.Cleanup(func() {
		require.NoError(t, ln.Close())
		<-done // let the accept loop exit before the test ends
	})

	return ln.Addr().String(), accepted
}

func newRejectingLazy(t *testing.T, addr string) *sftpLazy {
	t.Helper()
	return newSFTPLazy(addr, &ssh.ClientConfig{
		User:            "irrelevant",
		Auth:            []ssh.AuthMethod{ssh.Password("irrelevant")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}, slog.New(slog.DiscardHandler))
}

// newRejectingStorage builds a Storage aimed at a rejecting listener, so every
// connection attempt fails without needing a real SFTP server.
func newRejectingStorage(t *testing.T, addr string) *Storage {
	t.Helper()

	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := net.LookupPort("tcp", portStr)
	require.NoError(t, err)

	st, err := NewStorage(Config{
		Host:     host,
		Port:     port,
		User:     "irrelevant",
		Password: "irrelevant",
		Prefix:   "/upload",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// --- Tests -----------------------------------------------------------------

// However the storage is fanned out — reused directly or cloned via SubStorage
// — the clones share one sftpLazy, so concurrent use must dial the remote host
// exactly once rather than once per caller.
func TestSFTPLazy_ConcurrentUseDialsOnce(t *testing.T) {
	tests := []struct {
		name string
		// handles returns one storage per goroutine; each entry is expected to
		// be backed by the parent's sftpLazy.
		handles func(t *testing.T, parent *Storage) []*Storage
	}{
		{
			name: "the same storage from every goroutine",
			handles: func(_ *testing.T, parent *Storage) []*Storage {
				handles := make([]*Storage, concurrency)
				for i := range handles {
					handles[i] = parent
				}
				return handles
			},
		},
		{
			name: "one SubStorage clone per goroutine",
			handles: func(t *testing.T, parent *Storage) []*Storage {
				handles := make([]*Storage, concurrency)
				for i := range handles {
					sub, ok := parent.SubStorage("sub", true).(*Storage)
					require.True(t, ok)
					handles[i] = sub
				}
				return handles
			},
		},
		{
			name: "clones of clones",
			handles: func(t *testing.T, parent *Storage) []*Storage {
				handles := make([]*Storage, concurrency)
				current := parent
				for i := range handles {
					sub, ok := current.SubStorage("nested", true).(*Storage)
					require.True(t, ok)
					handles[i] = sub
					current = sub
				}
				return handles
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			addr, accepted := rejectingListener(t)
			parent := newRejectingStorage(t, addr)
			handles := tt.handles(t, parent)

			// Act: every goroutine forces the lazy connection at once.
			fanOut(func(i int) {
				assert.Errorf(t, handles[i].Ping(context.Background()), "caller %d", i)
			})

			// Assert
			assert.Equal(t, int64(1), accepted.Load(), "the remote host must be dialled exactly once")
			for i, handle := range handles {
				assert.Samef(t, parent.sftpLazy, handle.sftpLazy, "handle %d must share the parent's connection", i)
			}
		})
	}
}

// The outcome of the single dial — success or failure — is cached and handed to
// every concurrent caller.
func TestSFTPLazy_ConcurrentClient(t *testing.T) {
	t.Run("failure is cached and shared by every caller", func(t *testing.T) {
		// Arrange
		addr, accepted := rejectingListener(t)
		lazy := newRejectingLazy(t, addr)

		// Act
		errs := make([]error, concurrency)
		fanOut(func(i int) {
			client, err := lazy.Client(context.Background())
			assert.Nilf(t, client, "caller %d", i)
			errs[i] = err
		})

		// Assert
		assert.Equal(t, int64(1), accepted.Load(), "the remote host must be dialled exactly once")
		require.Error(t, errs[0])
		for i, err := range errs {
			// The identical error value, not a fresh one from a second dial.
			assert.Samef(t, errs[0], err, "caller %d must observe the cached error", i)
		}
	})

	t.Run("established client is shared by every caller", func(t *testing.T) {
		// Arrange: the happy path needs a real server, so use the shared container.
		st := newTestStorage(t)
		t.Cleanup(func() { _ = st.Close() })

		// Act
		clients := make([]SFTPClient, concurrency)
		fanOut(func(i int) {
			client, err := st.sftpLazy.Client(context.Background())
			assert.NoErrorf(t, err, "caller %d", i)
			clients[i] = client
		})

		// Assert
		require.NotNil(t, clients[0])
		for i, client := range clients {
			assert.Samef(t, clients[0], client, "caller %d must reuse the established client", i)
		}
	})
}

// Close races with in-flight Client calls when one goroutine shuts the storage
// down while others are still using it.
func TestSFTPLazy_ConcurrentClientAndClose(t *testing.T) {
	t.Run("close during in-flight calls", func(t *testing.T) {
		// Arrange
		addr, accepted := rejectingListener(t)
		lazy := newRejectingLazy(t, addr)

		// Act: close concurrently with the fan-out, so the shutdown lands at an
		// arbitrary point among the callers.
		var closeErr error
		var closing sync.WaitGroup
		closing.Go(func() { closeErr = lazy.Close() })
		fanOut(func(i int) {
			client, err := lazy.Client(context.Background())
			// The dial always fails against the rejecting listener, so whichever
			// way the race lands there is no usable client — either the dial
			// error or errStorageClosed.
			assert.Nilf(t, client, "caller %d", i)
			assert.Errorf(t, err, "caller %d", i)
		})
		closing.Wait()

		// Assert
		require.NoError(t, closeErr)
		assert.LessOrEqual(t, accepted.Load(), int64(1), "at most one dial regardless of how the race lands")
	})

	t.Run("client after close is rejected", func(t *testing.T) {
		// Arrange
		addr, accepted := rejectingListener(t)
		lazy := newRejectingLazy(t, addr)
		require.NoError(t, lazy.Close())

		// Act
		client, err := lazy.Client(context.Background())

		// Assert
		assert.Nil(t, client)
		assert.ErrorIs(t, err, errStorageClosed)
		assert.Zero(t, accepted.Load(), "a closed storage must not dial")
	})
}
