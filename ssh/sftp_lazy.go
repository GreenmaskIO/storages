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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// ErrStorageClosed is returned by any operation on a storage whose Close has
// already been called. Callers can test for it with errors.Is to tell a
// closed-storage misuse apart from a transport failure.
var ErrStorageClosed = errors.New("ssh storage is closed")

// SFTPClient is the subset of *sftp.Client used by the storage. It is kept as
// an interface so the dependency surface stays small and explicit.
type SFTPClient interface {
	ReadDir(path string) ([]os.FileInfo, error)
	Join(elem ...string) string
	Remove(path string) error
	RemoveDirectory(path string) error
	Stat(p string) (os.FileInfo, error)
	Open(path string) (*sftp.File, error)
	Create(path string) (*sftp.File, error)
	MkdirAll(path string) error
	Rename(oldname, newname string) error
	PosixRename(oldname, newname string) error
}

// sftpLazy establishes the SSH/SFTP connection on the first Client() call and
// reuses it in all subsequent calls (shared across every SubStorage clone).
// Close() releases the connection; it must be called by the lifecycle owner
// (e.g. a long-running server) — otherwise the connection, its receive
// goroutine and its socket leak until the process exits.
type sftpLazy struct {
	address string
	config  *ssh.ClientConfig
	logger  *slog.Logger

	// lock is a mutex a caller can stop waiting for when its ctx is done. It
	// guards the fields below and is held while connecting.
	lock      chan struct{}
	sftp      SFTPClient
	ssh       *ssh.Client
	connErr   error
	attempted bool
	closed    bool
}

func newSFTPLazy(addr string, config *ssh.ClientConfig, logger *slog.Logger) *sftpLazy {
	return &sftpLazy{
		address: addr,
		config:  config,
		logger:  logger,
		lock:    make(chan struct{}, 1),
	}
}

// Client returns the shared SFTP client, connecting on the first call. The
// outcome is cached for the lifetime of the storage, unless the attempt was cut
// short by the caller's ctx: then the next call connects again. A caller waiting
// for another's attempt gives up when its own ctx is done.
func (l *sftpLazy) Client(ctx context.Context) (SFTPClient, error) {
	select {
	case l.lock <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for SSH connection to %s: %w", l.address, context.Cause(ctx))
	}
	defer func() { <-l.lock }()

	if l.closed {
		return nil, ErrStorageClosed
	}
	if l.attempted {
		return l.sftp, l.connErr
	}
	sftpClient, sshClient, err := connect(ctx, l.address, l.config, l.logger)
	if err != nil {
		// A cancelled caller says nothing about the host. A ConnectTimeout
		// does, and is cached like any other failure.
		if ctx.Err() == nil {
			l.attempted = true
			l.connErr = err
		}
		return nil, err
	}
	l.attempted = true
	l.sftp = sftpClient
	l.ssh = sshClient
	return l.sftp, nil
}

// Close releases the underlying connection. It is idempotent and safe to call
// before the connection has been established; a connection attempt in flight,
// bounded by ConnectTimeout, is waited for. Closing the SSH client tears down
// the TCP connection, which also unblocks and stops the sftp receive goroutine;
// we deliberately close the SSH client rather than sftp.Client.Close(), which
// can hang on a wedged connection because it waits on that receive loop.
func (l *sftpLazy) Close() error {
	l.lock <- struct{}{}
	defer func() { <-l.lock }()

	l.closed = true
	if l.ssh == nil {
		return nil // never connected (or already torn down)
	}
	err := l.ssh.Close()
	l.ssh = nil
	l.sftp = nil
	return err
}

// connect dials addr and opens an SFTP session on it. ctx and config.Timeout
// bound all of it: ssh.Dial applies the timeout to the TCP connect alone, so a
// host that stalls the handshake or the SFTP init would hang it.
func connect(ctx context.Context, addr string, config *ssh.ClientConfig, logger *slog.Logger) (*sftp.Client, *ssh.Client, error) {
	if config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	// Everything below blocks on conn; closing it is what unblocks it once ctx is done.
	stop := context.AfterFunc(ctx, func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Warn("close SSH connection cut short by ctx", "address", addr, "error", err)
		}
	})
	sftpClient, sshClient, err := openSFTP(conn, addr, config, logger)
	if !stop() {
		// conn is closed, which ends sshClient and sftpClient too.
		return nil, nil, fmt.Errorf("connect to %s via SSH: %w", addr, context.Cause(ctx))
	}
	return sftpClient, sshClient, err
}

// openSFTP runs the SSH handshake on conn and starts an SFTP session. On failure
// conn is closed.
func openSFTP(conn net.Conn, addr string, config *ssh.ClientConfig, logger *slog.Logger) (*sftp.Client, *ssh.Client, error) {
	clientConn, channels, requests, err := ssh.NewClientConn(conn, addr, config)
	if err != nil {
		// NewClientConn closes conn on failure.
		return nil, nil, fmt.Errorf("connect to %s via SSH: %w", addr, err)
	}
	sshClient := ssh.NewClient(clientConn, channels, requests)

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		// The SSH client owns the TCP connection; close it so a failed SFTP
		// handshake does not leak the socket.
		if cerr := sshClient.Close(); cerr != nil {
			logger.Warn("error closing SSH client after failed SFTP handshake", "address", addr, "error", cerr)
		}
		return nil, nil, fmt.Errorf("start SFTP session with %s: %w", addr, err)
	}
	return sftpClient, sshClient, nil
}
