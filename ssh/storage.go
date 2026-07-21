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

// Package ssh implements the Storager interface on top of SSH/SFTP.
// The implementation is inspired by wal-g's pkg/storages/sh.
package ssh

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path"
	"strconv"

	"golang.org/x/crypto/ssh"

	"github.com/greenmaskio/storages"
)

// defaultBufferSize is the 64 MiB read buffer used by GetObject.
const defaultBufferSize = 64 << 20

// *Storage owns a persistent SSH connection and must be closed by its owner.
var _ io.Closer = (*Storage)(nil)

// Compile-time check that Storage implements the Storager interface.
var _ storages.Storager = (*Storage)(nil)

type Storage struct {
	cfg      Config
	sftpLazy *sftpLazy
	cwd      string // current remote dir (root = cfg.Prefix)
	logger   *slog.Logger
}

// NewStorage builds an SSH/SFTP backend from cfg. The connection is established
// lazily on first use (or via Ping). Pass WithLogger to enable diagnostic
// output; without it the backend does not log at all. Call Close on the returned
// storage when done to release the connection.
func NewStorage(cfg Config, opts ...Option) (*Storage, error) {
	cfg.applyDefaults()
	s := &Storage{
		cfg: cfg,
		cwd: cfg.Prefix,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		// Default to a no-op logger so the rest of the code can call s.logger
		// unconditionally without nil checks; it discards everything until the
		// caller supplies one via WithLogger.
		s.logger = slog.New(slog.DiscardHandler)
	}

	authMethods, err := buildAuthMethods(cfg)
	if err != nil {
		return nil, err
	}

	sshConfig := &ssh.ClientConfig{
		User: cfg.User,
		Auth: authMethods,
		// TODO: verify host keys when a known_hosts option is added
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	address := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))

	s.sftpLazy = newSFTPLazy(address, sshConfig, s.logger)
	return s, nil
}

// Option configures a Storage.
type Option func(*Storage)

// WithLogger sets the logger for the backend's diagnostic output. Without this
// option the backend does not log at all.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Storage) {
		s.logger = logger
	}
}

// buildAuthMethods assembles the SSH auth methods so the private key (if
// configured) precedes the password. It is factored out so the auth ordering
// and key parsing are unit-testable without opening a connection.
func buildAuthMethods(cfg Config) ([]ssh.AuthMethod, error) {
	var authMethods []ssh.AuthMethod
	if cfg.PrivateKeyPath != "" {
		pkey, err := os.ReadFile(cfg.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("read SSH private key: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(pkey)
		if err != nil {
			return nil, fmt.Errorf("parse SSH private key: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}
	if cfg.Password != "" {
		authMethods = append(authMethods, ssh.Password(cfg.Password))
	}
	return authMethods, nil
}

// Close releases the shared SSH/SFTP connection. It must be called once by the
// lifecycle owner when the storage and all of its sub-storages are no longer
// needed; otherwise the connection, its receive goroutine and its socket leak
// until the process exits. SubStorage clones share the same connection, so only
// the connection is closed (not the clone) — Close is idempotent and safe to
// call more than once and before the connection has been established.
func (s *Storage) Close() error {
	return s.sftpLazy.Close()
}

func (s *Storage) GetCwd() string {
	return s.cwd
}

func (s *Storage) Dirname() string {
	return path.Base(s.cwd)
}

func (s *Storage) ListDir(ctx context.Context) (files []string, dirs []storages.Storager, err error) {
	client, err := s.sftpLazy.Client(ctx)
	if err != nil {
		return nil, nil, err
	}

	filesInfo, err := client.ReadDir(s.cwd)
	if os.IsNotExist(err) {
		// A nonexistent dir is treated as an empty one.
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read SFTP dir %q: %w", s.cwd, err)
	}

	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			dirs = append(dirs, &Storage{
				cfg:      s.cfg,
				sftpLazy: s.sftpLazy,
				cwd:      client.Join(s.cwd, fileInfo.Name()),
				logger:   s.logger,
			})
			continue
		}
		files = append(files, fileInfo.Name())
	}
	return files, dirs, nil
}

func (s *Storage) GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	client, err := s.sftpLazy.Client(ctx)
	if err != nil {
		return nil, err
	}

	objPath := path.Join(s.cwd, filePath)
	file, err := client.Open(objPath)
	if err != nil {
		// Any open failure (including a missing file) maps to not found.
		return nil, storages.ErrFileNotFound
	}

	return struct {
		io.Reader
		io.Closer
	}{bufio.NewReaderSize(file, defaultBufferSize), file}, nil
}

func (s *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	client, err := s.sftpLazy.Client(ctx)
	if err != nil {
		return err
	}

	absolutePath := path.Join(s.cwd, filePath)
	dirPath := path.Dir(absolutePath)
	if err = client.MkdirAll(dirPath); err != nil {
		return fmt.Errorf("create directory %q via SFTP: %w", dirPath, err)
	}

	file, err := client.Create(absolutePath)
	if err != nil {
		return fmt.Errorf("create file %q via SFTP: %w", absolutePath, err)
	}

	done := make(chan struct{})
	var copyErr error
	go func() {
		_, copyErr = io.Copy(file, body)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if cerr := file.Close(); cerr != nil {
			s.logger.Warn("error closing file after context cancellation", "path", absolutePath, "error", cerr)
		}
		return ctx.Err()
	case <-done:
	}

	if copyErr != nil {
		if cerr := file.Close(); cerr != nil {
			s.logger.Warn("error closing file after failed write", "path", absolutePath, "error", cerr)
		}
		return fmt.Errorf("write data to file %q via SFTP: %w", absolutePath, copyErr)
	}

	if err = file.Close(); err != nil {
		return fmt.Errorf("close file %q opened via SFTP: %w", absolutePath, err)
	}
	return nil
}

func (s *Storage) Delete(ctx context.Context, filePaths ...string) error {
	client, err := s.sftpLazy.Client(ctx)
	if err != nil {
		return err
	}

	// Verify every path before removing anything, so a request naming one bad
	// path leaves the storage untouched rather than partly deleted.
	objPaths := make([]string, len(filePaths))
	var missing []string
	for i, fp := range filePaths {
		objPaths[i] = path.Join(s.cwd, fp)

		stat, err := client.Stat(objPaths[i])
		switch {
		case errors.Is(err, os.ErrNotExist):
			missing = append(missing, fp)
		case err != nil:
			return fmt.Errorf("get stats of object %q via SFTP: %w", objPaths[i], err)
		case stat.IsDir():
			// Delete is object-level; a directory is not an object. DeleteAll
			// removes sub-trees.
			missing = append(missing, fp)
		}
	}
	if len(missing) > 0 {
		return &storages.MissingObjectsError{Paths: missing}
	}

	for i, objPath := range objPaths {
		// A path that vanished between the check above and here means someone
		// else removed it; the caller's intent is satisfied either way.
		if err := client.Remove(objPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete object %q via SFTP: %w", filePaths[i], err)
		}
	}
	return nil
}

func (s *Storage) DeleteAll(ctx context.Context, pathPrefix string) error {
	client, err := s.sftpLazy.Client(ctx)
	if err != nil {
		return err
	}
	fullPath := path.Join(s.cwd, pathPrefix)

	// The prefix itself must exist; removeAll tolerates a missing path so that
	// concurrent removal deeper in the tree is not an error, but the top-level
	// target being absent is what the caller needs to hear about.
	if _, err := client.Stat(fullPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &storages.MissingObjectsError{Paths: []string{pathPrefix}}
		}
		return fmt.Errorf("get stats of %q via SFTP: %w", fullPath, err)
	}

	// Mirror os.RemoveAll: remove everything under the prefix, including the now
	// empty directories and the prefix directory itself. SFTP has no recursive
	// remove, and leaving the emptied directories behind would make a deleted
	// dump still surface in list-dumps as an empty, status-less directory.
	if err := removeAll(client, fullPath); err != nil {
		return fmt.Errorf("error deleting %q: %w", pathPrefix, err)
	}
	return nil
}

// removeAll recursively deletes p and everything below it: files are removed,
// directories are emptied then removed, and p itself is removed. A missing p is
// not an error (idempotent), matching os.RemoveAll.
func removeAll(client SFTPClient, p string) error {
	info, err := client.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get stats of %q via SFTP: %w", p, err)
	}

	if !info.IsDir() {
		if err := client.Remove(p); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return fmt.Errorf("delete object %q via SFTP: %w", p, err)
		}
		return nil
	}

	entries, err := client.ReadDir(p)
	if err != nil {
		return fmt.Errorf("read SFTP dir %q: %w", p, err)
	}
	for _, e := range entries {
		if err := removeAll(client, client.Join(p, e.Name())); err != nil {
			return err
		}
	}
	if err := client.RemoveDirectory(p); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("remove SFTP dir %q: %w", p, err)
	}
	return nil
}

func (s *Storage) Exists(ctx context.Context, fileName string) (bool, error) {
	client, err := s.sftpLazy.Client(ctx)
	if err != nil {
		return false, err
	}

	objPath := path.Join(s.cwd, fileName)
	_, err = client.Stat(objPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check file %q existence via SFTP: %w", objPath, err)
	}
	return true, nil
}

func (s *Storage) SubStorage(subPath string, relative bool) storages.Storager {
	cwd := subPath
	if relative {
		cwd = path.Join(s.cwd, subPath)
	}
	return &Storage{
		cfg:      s.cfg,
		sftpLazy: s.sftpLazy,
		cwd:      cwd,
		logger:   s.logger,
	}
}

// Ping forces the lazy SSH/SFTP connection to be established, which serves as a
// real reachability check for the remote host.
func (s *Storage) Ping(ctx context.Context) error {
	if _, err := s.sftpLazy.Client(ctx); err != nil {
		return fmt.Errorf("error pinging ssh storage: %w", err)
	}
	return nil
}

func (s *Storage) Stat(fileName string) (*storages.ObjectStat, error) {
	// Stat has no ctx in the Storager interface; use the background context if a
	// lazy connect is triggered here.
	client, err := s.sftpLazy.Client(context.Background())
	if err != nil {
		return nil, err
	}

	fullPath := path.Join(s.cwd, fileName)
	fileInfo, err := client.Stat(fullPath)
	if os.IsNotExist(err) {
		return &storages.ObjectStat{
			Name:  fullPath,
			Exist: false,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error getting file stat: %w", err)
	}

	return &storages.ObjectStat{
		Name:         fullPath,
		LastModified: fileInfo.ModTime(),
		Exist:        true,
	}, nil
}
