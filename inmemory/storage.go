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

// Package inmemory provides an in-memory Storager implementation, useful for
// tests and for validating code that consumes the Storager interface without
// touching a real backend. It is a thin wrapper over the shared fsbackend
// implementation wired to afero.NewMemMapFs(), so it behaves exactly like the
// on-disk directory backend.
package inmemory

import (
	"fmt"
	"log/slog"

	"github.com/spf13/afero"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/internal/fsbackend"
)

// Compile-time check that Storage implements the Storager interface.
var _ storages.Storager = (*Storage)(nil)

// Storage is the in-memory backend.
type Storage = fsbackend.Storage

// Option configures a Storage.
type Option = fsbackend.Option

// WithLogger sets the logger for the backend's diagnostic output. Without this
// option the backend does not log at all.
func WithLogger(logger *slog.Logger) Option {
	return fsbackend.WithLogger(logger)
}

// WithUnsafe turns the key guard off. By default the storage returned by New is
// guarded: a key that reaches outside basePath or that names it is refused with
// storages.ErrUnsafeKey. Pass this only when every key is known to be trusted
// and a path with legitimate ".." segments has to get through.
func WithUnsafe() Option {
	return fsbackend.WithUnsafe()
}

// New initializes a root in-memory storage rooted at basePath. An empty basePath
// is treated as "/".
//
// The returned storage is guarded: keys cannot escape basePath. See WithUnsafe
// to opt out.
func New(basePath string, opts ...Option) storages.Storager {
	if basePath == "" {
		basePath = "/"
	}
	memFs := afero.NewMemMapFs()
	// Ensure the root exists so ListDir/Ping on a fresh storage succeed. MkdirAll
	// on a fresh in-memory filesystem cannot fail for a valid path; a failure here
	// means the process is in a broken state, so surface it loudly.
	if err := memFs.MkdirAll(basePath, fsbackend.DirMode); err != nil {
		panic(fmt.Sprintf("inmemory: failed to create root %q: %v", basePath, err))
	}
	return fsbackend.Guarded(fsbackend.New(memFs, basePath, opts...))
}
