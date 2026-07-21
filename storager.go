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

// Package storages defines Storager, the backend-agnostic interface implemented
// by every storage backend (directory, s3, azure, ssh, and the in-memory
// inmemory backend), together with the shared ErrFileNotFound sentinel and the
// Walk helper. It is the foundation package: backends import it, never the
// reverse, so the dependency graph stays acyclic.
package storages

import (
	"context"
	"io"
	"time"
)

// ObjectStat is the metadata returned by Storager.Stat. A missing object is
// reported via Exist being false, not via an error.
type ObjectStat struct {
	Name         string
	LastModified time.Time
	Exist        bool
}

// Storager is the common interface implemented by every storage backend. It
// models a directory-like namespace: a backend instance is rooted at a "current
// working directory" (GetCwd) and exposes object CRUD plus hierarchical
// navigation via SubStorage and ListDir.
//
// Paths passed to the object methods are interpreted relative to the backend's
// current working directory and use forward slashes on every OS. A missing
// object surfaces differently per method: GetObject, Delete and DeleteAll
// return an error wrapping ErrFileNotFound, while Exists and Stat report it as
// a value with a nil error. ListDir returns []Storager and SubStorage returns
// Storager, which makes the interface self-referential — this is why the
// interface lives in a foundation package that no backend may import back into.
type Storager interface {
	// GetCwd returns the current working directory (root path/prefix) of this
	// storage instance.
	GetCwd() string
	// Dirname returns the base name of the current working directory.
	Dirname() string
	// ListDir lists the immediate contents of the current directory, returning
	// the file names and a Storager for each sub-directory.
	ListDir(ctx context.Context) (files []string, dirs []Storager, err error)
	// GetObject opens the object at filePath for reading. It returns
	// ErrFileNotFound if the object does not exist. The caller must Close the
	// returned reader.
	GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error)
	// PutObject writes body to the object at filePath, overwriting an existing
	// object and creating any intermediate directories as needed.
	PutObject(ctx context.Context, filePath string, body io.Reader) error
	// Delete removes the named objects. It is object-level and never recursive:
	// a path naming a directory rather than an object is not deleted. Use
	// DeleteAll to remove a sub-tree.
	//
	// Every path is verified before anything is removed. If any path is not an
	// existing object — absent, or a directory — nothing is deleted and the
	// error is a *MissingObjectsError listing them, which wraps ErrFileNotFound.
	// Delete is therefore not idempotent: deleting the same object twice fails
	// the second time.
	Delete(ctx context.Context, filePaths ...string) error
	// DeleteAll recursively removes everything under pathPrefix, including the
	// prefix "directory" itself on backends that have real directories.
	//
	// A prefix holding nothing is a *MissingObjectsError wrapping
	// ErrFileNotFound, so DeleteAll is likewise not idempotent: re-running a
	// deletion that already succeeded fails.
	DeleteAll(ctx context.Context, pathPrefix string) error
	// Exists reports whether fileName exists in the current directory. A missing
	// object is (false, nil), not an error; an error means the lookup itself
	// failed.
	Exists(ctx context.Context, fileName string) (bool, error)
	// SubStorage returns a Storager rooted at subPath. When relative is true the
	// path is joined onto the current working directory; otherwise it is used as
	// an absolute root. The returned storage shares the parent's connection and
	// configuration.
	SubStorage(subPath string, relative bool) Storager
	// Stat returns metadata about fileName. A non-existent object is reported via
	// ObjectStat.Exist being false with a nil error; an error means the lookup
	// itself failed.
	Stat(fileName string) (*ObjectStat, error)
	// Ping checks connectivity/reachability of the underlying storage. It forces
	// a real connection where the backend connects lazily.
	Ping(ctx context.Context) error
	// Close releases any resources held by the storage (e.g. the ssh backend's
	// connection). It is safe to call on backends that hold none, and callers
	// should always call it when done. SubStorage clones share the parent's
	// resources, so Close should be called on the owning root instance.
	Close() error
}
