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
	"log/slog"
	"os"

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

// NewStorage opens the directory backend rooted at cfg.Path. The path must exist
// and be a directory. Pass WithLogger to enable diagnostic output; without it the
// backend does not log at all.
//
// The returned storage is guarded: keys cannot escape cfg.Path. See WithUnsafe
// to opt out.
func NewStorage(cfg Config, opts ...Option) (storages.Storager, error) {
	fileInfo, err := os.Stat(cfg.Path)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, errors.New("received directory path is file")
	}
	return fsbackend.Guarded(fsbackend.New(afero.NewOsFs(), cfg.Path, opts...)), nil
}
