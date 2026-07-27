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

// Package directory provides an on-disk Storager backed by the local
// filesystem. It is a thin wrapper over the shared fsbackend implementation
// wired to afero.NewOsFs().
package directory

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/internal/fsbackend"
)

// Compile-time check that Storage implements the Storager interface.
var _ storages.Storager = (*Storage)(nil)

// Storage is the on-disk directory backend.
type Storage = fsbackend.Storage

// Option configures a Storage.
type Option = fsbackend.Option

// WithLogger sets the logger for the backend's diagnostic output. Without this
// option the backend does not log at all.
func WithLogger(logger *slog.Logger) Option {
	return fsbackend.WithLogger(logger)
}

// WithUnsafe turns the key guard off. By default the storage returned by
// NewStorage is guarded: a key that reaches outside the configured directory —
// "../../etc/passwd" — or that names the directory itself is refused with
// storages.ErrUnsafeKey before it reaches the filesystem. Pass this only when
// every key is known to be trusted and a path with legitimate ".." segments has
// to get through.
func WithUnsafe() Option {
	return fsbackend.WithUnsafe()
}

// WithCreatePrefix creates a missing cfg.Prefix, intermediate directories
// included, with mode 0750; without it NewStorage returns ErrPrefixNotExists.
// cfg.RootPath is never created, so a typo in the root cannot quietly produce a
// fresh empty tree.
func WithCreatePrefix() Option {
	return fsbackend.WithCreatePrefix()
}

// NewStorage opens the directory backend rooted at cfg.RootPath/cfg.Prefix.
// RootPath must exist and be a directory. The prefix must exist too, unless
// WithCreatePrefix is passed, in which case it is created. Pass WithLogger to
// enable diagnostic output; without it the backend does not log at all.
//
// The returned storage is guarded: keys cannot escape cfg.RootPath/cfg.Prefix.
// See WithUnsafe to opt out.
func NewStorage(cfg Config, opts ...Option) (storages.Storager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	prefix, err := cleanPrefix(cfg.Prefix)
	if err != nil {
		return nil, err
	}
	// filepath.Join, not path.Join: this is the OS-native path the os calls below
	// act on. fsbackend converts it to its own slash convention.
	root := filepath.Join(cfg.RootPath, filepath.FromSlash(prefix))

	s := fsbackend.New(afero.NewOsFs(), root, opts...)
	if err := ensurePrefix(root, fsbackend.CreatePrefixEnabled(s)); err != nil {
		return nil, err
	}
	return fsbackend.Guarded(s), nil
}

// ensurePrefix settles the storage root before any object reaches it. With an
// empty prefix root is RootPath, already validated, so this is just a stat.
func ensurePrefix(root string, create bool) error {
	info, err := os.Stat(root)
	switch {
	case err == nil:
		if !info.IsDir() {
			return fmt.Errorf("prefix path %q is a file", root)
		}
		return nil
	case errors.Is(err, fs.ErrNotExist):
		if !create {
			return fmt.Errorf("%q: %w", root, ErrPrefixNotExists)
		}
		// MkdirAll creates only the missing segments of a partial prefix.
		if err := os.MkdirAll(root, fsbackend.DirMode); err != nil {
			return fmt.Errorf("error creating prefix directory: %w", err)
		}
		return nil
	default:
		// An intermediate segment that is a file, a permission error: the
		// filesystem's answer to give, not ours to reinterpret.
		return err
	}
}
