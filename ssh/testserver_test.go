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
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"testing"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// An in-process SSH server exposing the SFTP subsystem. The tests in this
// package that need a *working* connection (rather than a failing one) use it
// instead of a container: they only care that a real handshake and SFTP session
// can be established, which does not justify Docker. End-to-end behavior
// against a genuine OpenSSH server is covered by the tests/integration module.

const (
	testServerUser     = "testuser"
	testServerPassword = "testpass"
)

// startSFTPServer starts an in-process SSH server serving the SFTP subsystem
// rooted at rootDir, and returns the host and port it listens on. The server is
// shut down when the test ends.
func startSFTPServer(t *testing.T, rootDir string) (string, int) {
	t.Helper()

	signer := generateHostKey(t)
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() != testServerUser || string(pass) != testServerPassword {
				return nil, errBadCredentials
			}
			return nil, nil
		},
	}
	cfg.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed by the cleanup below
			}
			go serveSSHConn(conn, cfg, rootDir)
		}
	}()

	t.Cleanup(func() {
		require.NoError(t, ln.Close())
		<-done
	})

	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)
	return addr.IP.String(), addr.Port
}

// errBadCredentials is the rejection returned by the test server's password
// callback. It never reaches production code.
var errBadCredentials = io.ErrUnexpectedEOF

// serveSSHConn completes the SSH handshake and serves an SFTP subsystem on any
// session channel. Errors are dropped: the connection simply goes away, which
// is what the client-side assertions observe.
func serveSSHConn(conn net.Conn, cfg *ssh.ServerConfig, rootDir string) {
	sshConn, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer func() { _ = sshConn.Close() }()

	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "only session channels are supported")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			return
		}
		go serveSFTPChannel(channel, requests, rootDir)
	}
}

// serveSFTPChannel waits for the "sftp" subsystem request on an accepted
// session channel and then hands the channel to an sftp.Server.
func serveSFTPChannel(channel ssh.Channel, requests <-chan *ssh.Request, rootDir string) {
	for req := range requests {
		if req.Type != "subsystem" || len(req.Payload) < 4 || string(req.Payload[4:]) != "sftp" {
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
			continue
		}
		if req.WantReply {
			_ = req.Reply(true, nil)
		}

		server, err := sftp.NewServer(channel, sftp.WithServerWorkingDirectory(rootDir))
		if err != nil {
			_ = channel.Close()
			return
		}
		_ = server.Serve()
		_ = server.Close()
		return
	}
}

// generateHostKey returns a throwaway ed25519 signer for the test server.
func generateHostKey(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	return signer
}

// newLocalStorage returns a Storage connected to an in-process SFTP server
// rooted at a fresh temp directory.
func newLocalStorage(t *testing.T) *Storage {
	t.Helper()

	root := t.TempDir()
	host, port := startSFTPServer(t, root)

	st, err := NewStorage(Config{
		Host:     host,
		Port:     port,
		User:     testServerUser,
		Password: testServerPassword,
		Prefix:   root,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}
