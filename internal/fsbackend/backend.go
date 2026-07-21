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

// Package fsbackend provides a Storager implementation on top of an afero.Fs.
// It is the shared foundation for the filesystem-like backends: the directory
// backend wires it to afero.NewOsFs(), while the inmemory backend wires it to
// afero.NewMemMapFs(). Keeping a single implementation guarantees the in-memory
// test double behaves exactly like the real on-disk backend.
package fsbackend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/greenmaskio/storages"
)

const (
	// DirMode is the permission mode used when creating intermediate directories.
	DirMode os.FileMode = 0750
	// FileMode is the permission mode used when creating files.
	FileMode os.FileMode = 0640
)

// Compile-time check that Storage implements the Storager interface.
var _ storages.Storager = (*Storage)(nil)

// Storage is a Storager rooted at cwd on top of an afero.Fs.
type Storage struct {
	fs       afero.Fs
	cwd      string
	dirMode  os.FileMode
	fileMode os.FileMode
	logger   *slog.Logger
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

// toSlash converts a caller-supplied path to the internal forward-slash
// convention used everywhere in this backend. afero is OS-native — OsFs passes
// paths straight to the os package and MemMapFs normalizes with
// filepath.Separator — so without this the same key would resolve differently on
// Windows. Normalizing to "/" keeps OsFs and MemMapFs identical and matches the
// S3-style keys used by the other backends. os/afero.OsFs accept forward slashes
// on Windows, and MemMapFs re-normalizes symmetrically, so round-trips stay
// consistent.
func toSlash(p string) string {
	return filepath.ToSlash(p)
}

// New builds a Storage rooted at cwd on top of fsys.
func New(fsys afero.Fs, cwd string, opts ...Option) *Storage {
	s := &Storage{
		fs:       fsys,
		cwd:      toSlash(cwd),
		dirMode:  DirMode,
		fileMode: FileMode,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Storage) GetCwd() string {
	return s.cwd
}

func (s *Storage) Dirname() string {
	return path.Base(s.cwd)
}

func (s *Storage) ListDir(_ context.Context) (files []string, dirs []storages.Storager, err error) {
	entries, err := afero.ReadDir(s.fs, s.cwd)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, s.sub(path.Join(s.cwd, entry.Name())))
		} else {
			files = append(files, entry.Name())
		}
	}
	return files, dirs, nil
}

func (s *Storage) GetObject(_ context.Context, filePath string) (io.ReadCloser, error) {
	f, err := s.fs.Open(path.Join(s.cwd, toSlash(filePath)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, storages.ErrFileNotFound
		}
		return nil, err
	}
	return f, nil
}

func (s *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	fullPath := path.Join(s.cwd, toSlash(filePath))
	if dir := path.Dir(fullPath); dir != "" {
		if err := s.fs.MkdirAll(dir, s.dirMode); err != nil {
			return fmt.Errorf("error creating directory: %w", err)
		}
	}

	f, err := s.fs.Create(fullPath)
	if err != nil {
		return fmt.Errorf("unable to create file: %w", err)
	}

	// Copy in a goroutine so a cancelled context returns promptly. The goroutine
	// owns the file handle and closes it itself, so returning on ctx.Done() never
	// races with the in-flight io.Copy (write-after-close). The channel is
	// buffered so the goroutine can always finish and never leaks.
	done := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(f, body)
		if cerr := f.Close(); cerr != nil {
			if copyErr == nil {
				copyErr = cerr
			} else if s.logger != nil {
				s.logger.Warn("error closing file", "path", fullPath, "error", cerr)
			}
		}
		done <- copyErr
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return fmt.Errorf("error writing data: %w", err)
		}
	}

	// Apply the configured file mode explicitly: afero.Fs.Create uses a fixed
	// default, so without this s.fileMode would be dead configuration. On Windows
	// Chmod only honors the write bit, which is acceptable.
	if err := s.fs.Chmod(fullPath, s.fileMode); err != nil {
		return fmt.Errorf("error setting file mode: %w", err)
	}
	return nil
}

func (s *Storage) Delete(_ context.Context, filePaths ...string) error {
	for _, fp := range filePaths {
		fullPath := path.Join(s.cwd, toSlash(fp))
		info, err := s.fs.Stat(fullPath)
		if err != nil {
			// Deleting a missing object is not an error.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		if info.IsDir() {
			err = s.fs.RemoveAll(fullPath)
		} else {
			err = s.fs.Remove(fullPath)
		}
		if err != nil {
			return fmt.Errorf("error deleting %s: %w", fp, err)
		}
	}
	return nil
}

func (s *Storage) DeleteAll(_ context.Context, pathPrefix string) error {
	if err := s.fs.RemoveAll(path.Join(s.cwd, toSlash(pathPrefix))); err != nil {
		return fmt.Errorf("error deleting %s: %w", pathPrefix, err)
	}
	return nil
}

func (s *Storage) Exists(_ context.Context, fileName string) (bool, error) {
	_, err := s.fs.Stat(path.Join(s.cwd, toSlash(fileName)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Storage) SubStorage(subPath string, relative bool) storages.Storager {
	newCwd := toSlash(subPath)
	if relative {
		newCwd = path.Join(s.cwd, toSlash(subPath))
	}
	return s.sub(newCwd)
}

func (s *Storage) sub(cwd string) *Storage {
	return &Storage{
		fs:       s.fs,
		cwd:      cwd,
		dirMode:  s.dirMode,
		fileMode: s.fileMode,
		logger:   s.logger,
	}
}

func (s *Storage) Stat(fileName string) (*storages.ObjectStat, error) {
	fullPath := path.Join(s.cwd, toSlash(fileName))
	info, err := s.fs.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &storages.ObjectStat{Name: fullPath, Exist: false}, nil
		}
		return nil, fmt.Errorf("error getting file stat: %w", err)
	}
	return &storages.ObjectStat{
		Name:         fullPath,
		LastModified: info.ModTime(),
		Exist:        true,
	}, nil
}

// Ping checks that the backend's current directory is readable.
func (s *Storage) Ping(_ context.Context) error {
	if _, err := afero.ReadDir(s.fs, s.cwd); err != nil {
		return fmt.Errorf("error pinging storage: %w", err)
	}
	return nil
}

// Close is a no-op: the filesystem backends hold no resources to release.
func (s *Storage) Close() error {
	return nil
}
