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

	// WithUnsafe yields the bare backend: these tests assert on its own fields
	// (the shared sftpLazy), which the guard wrapper does not expose.
	st, err := New(Config{
		Host:     host,
		Port:     port,
		User:     "irrelevant",
		Password: "irrelevant",
		Prefix:   "/upload",
	}, WithUnsafe())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	raw, ok := st.(*Storage)
	require.True(t, ok, "WithUnsafe must yield the bare backend")
	return raw
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
					subStorager, err := parent.SubStorage("sub", true)
					require.NoError(t, err)
					sub, ok := subStorager.(*Storage)
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
					subStorager, err := current.SubStorage("nested", true)
					require.NoError(t, err)
					sub, ok := subStorager.(*Storage)
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
		// Arrange: the happy path needs a server that actually completes the
		// handshake, so use the in-process one.
		st := newLocalRawStorage(t)

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
			// error or ErrStorageClosed.
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
		assert.ErrorIs(t, err, ErrStorageClosed)
		assert.Zero(t, accepted.Load(), "a closed storage must not dial")
	})
}

// stallingListener accepts connections and holds them open without a word, the
// way a host behind a stalled proxy does: an SSH handshake against it never ends.
// Every accept is also sent on accepts, so a test can act once a dial is in its handshake.
func stallingListener(t *testing.T) (addr string, accepted *atomic.Int64, accepts <-chan struct{}) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	accepted = &atomic.Int64{}
	acceptCh := make(chan struct{}, concurrency)
	var conns []net.Conn
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			acceptCh <- struct{}{}
		}
	}()

	t.Cleanup(func() {
		require.NoError(t, ln.Close())
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return ln.Addr().String(), accepted, acceptCh
}

func newStallingLazy(addr string, timeout time.Duration) *sftpLazy {
	return newSFTPLazy(addr, &ssh.ClientConfig{
		User:            "irrelevant",
		Auth:            []ssh.AuthMethod{ssh.Password("irrelevant")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}, slog.New(slog.DiscardHandler))
}

// stallingSFTPServer completes the SSH handshake and accepts a session channel,
// then never answers the sftp subsystem request: the SFTP init against it never ends.
func stallingSFTPServer(t *testing.T) (addr string) {
	t.Helper()

	cfg := &ssh.ServerConfig{NoClientAuth: true}
	cfg.AddHostKey(generateHostKey(t))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var conns []net.Conn
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go func() {
				_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
				for newChannel := range chans {
					_, requests, err := newChannel.Accept()
					if err != nil {
						return
					}
					go func() {
						for range requests { // never replied to
						}
					}()
				}
			}()
		}
	}()

	t.Cleanup(func() {
		require.NoError(t, ln.Close())
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return ln.Addr().String()
}

// ssh.Dial bounds only the TCP connect; the timeout must bound the rest too.
func TestSFTPLazy_Client_TimeoutBoundsTheConnection(t *testing.T) {
	tests := []struct {
		name string
		addr func(t *testing.T) string
	}{
		{
			name: "stalled SSH handshake",
			addr: func(t *testing.T) string {
				addr, _, _ := stallingListener(t)
				return addr
			},
		},
		{
			name: "stalled SFTP init",
			addr: stallingSFTPServer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lazy := newStallingLazy(tt.addr(t), 200*time.Millisecond)

			_, err := lazy.Client(t.Context())

			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}

// A cancelled caller ends the handshake, and its failure is not cached: the host
// was never judged, so the next caller dials again.
func TestSFTPLazy_Client_CancelledAttemptIsNotCached(t *testing.T) {
	addr, accepted, accepts := stallingListener(t)
	lazy := newStallingLazy(addr, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-accepts
		cancel()
	}()
	_, err := lazy.Client(ctx)
	require.ErrorIs(t, err, context.Canceled)
	retryCtx, retryCancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer retryCancel()

	_, err = lazy.Client(retryCtx)

	require.Error(t, err)
	assert.Equal(t, int64(2), accepted.Load(), "the second call dials again")
}

// A caller waiting on another's attempt gives up on its own ctx, without dialling.
func TestSFTPLazy_Client_WaiterHonoursItsCtx(t *testing.T) {
	addr, accepted, accepts := stallingListener(t)
	lazy := newStallingLazy(addr, time.Minute)
	firstCtx, firstCancel := context.WithCancel(t.Context())
	firstErr := make(chan error, 1)
	go func() {
		_, err := lazy.Client(firstCtx)
		firstErr <- err
	}()
	<-accepts
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer waitCancel()

	_, waitErr := lazy.Client(waitCtx)
	firstCancel()

	assert.ErrorIs(t, waitErr, context.DeadlineExceeded)
	assert.ErrorIs(t, <-firstErr, context.Canceled)
	assert.Equal(t, int64(1), accepted.Load(), "the waiter must not dial")
}
