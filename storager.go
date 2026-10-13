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

// Package storages defines Storager, the interface every storage backend
// implements, plus the helpers and errors shared by all of them.
package storages

import (
	"context"
	"io"
	"time"
)

// ObjectStat is an object's metadata. Stat names it by its full path, List by
// its path relative to the listed prefix.
type ObjectStat struct {
	Name         string
	LastModified time.Time
	// Exist is false when Stat finds no object; that is not an error.
	Exist bool
	// Size is the object's size in bytes, 0 when Exist is false.
	Size int64
	// ETag is the object's quoted HTTP entity tag, changing on every overwrite.
	// Only s3 and azure fill it.
	ETag string
}

// Storager is a directory-like object store; keys are slash-separated and
// relative to its working directory. See Guard for which keys are refused.
type Storager interface {
	Reader
	Writer
	Deleter
	Inspector
	Navigator
	Copier
	// Ping checks that the storage is reachable, connecting if it has not yet.
	Ping(ctx context.Context) error
	// Close releases the storage's resources. Call it on the root storage, not
	// on one returned by SubStorage or ListDir, which share them.
	io.Closer
}

// Reader reads objects.
type Reader interface {
	// GetObject opens filePath for reading; a missing object is ErrFileNotFound.
	GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error)
	// GetObjectRange opens length bytes at offset, or to the end if length < 0.
	// A range yielding nothing is ErrInvalidRange; one past the end is clamped.
	GetObjectRange(ctx context.Context, filePath string, offset, length int64) (reader io.ReadCloser, err error)
}

// Writer writes objects.
type Writer interface {
	// PutObject writes body to filePath, overwriting it and creating parent
	// directories.
	PutObject(ctx context.Context, filePath string, body io.Reader) error
}

// Deleter removes objects.
type Deleter interface {
	// Delete removes the named objects, or none of them if any is missing or a
	// directory: that is a *MissingObjectsError wrapping ErrFileNotFound.
	Delete(ctx context.Context, filePaths ...string) error
	// DeleteAll removes everything under pathPrefix. An empty prefix is a
	// *MissingObjectsError wrapping ErrFileNotFound.
	DeleteAll(ctx context.Context, pathPrefix string) error
}

// Inspector reports which objects exist without reading them. A missing object
// is a value, not an error.
type Inspector interface {
	// List returns every object under prefix, recursively, sorted by name. An
	// empty or absent prefix yields an empty slice.
	List(ctx context.Context, prefix string) ([]ObjectStat, error)
	// Stat returns fileName's metadata, with Exist false if it is missing.
	Stat(fileName string) (*ObjectStat, error)
	// Exists reports whether fileName is an object.
	Exists(ctx context.Context, fileName string) (bool, error)
}

// Navigator moves through the storage's directory tree.
type Navigator interface {
	// GetCwd returns the storage's working directory.
	GetCwd() string
	// Dirname returns the base name of the working directory.
	Dirname() string
	// ListDir returns the files in the working directory and a Storager for
	// each sub-directory.
	ListDir(ctx context.Context) (files []string, dirs []Storager, err error)
	// SubStorage returns a Storager rooted at subPath, joined onto the working
	// directory if relative. A guarded storage refuses an escaping path.
	SubStorage(subPath string, relative bool) (Storager, error)
}

// Copier copies objects within one storage.
type Copier interface {
	// Copy copies srcPath to dstPath, overwriting it. A missing or directory
	// source is ErrFileNotFound; on s3, a source changed mid-copy is ErrSourceChanged.
	Copy(ctx context.Context, srcPath, dstPath string, opts ...CopyOption) (CopyStat, error)
	// CopyAll copies every object under srcPrefix to dstPrefix. It is not
	// atomic: the first error cancels the rest, and finished copies stay.
	CopyAll(ctx context.Context, srcPrefix, dstPrefix string, opts ...CopyAllOption) (CopyStat, error)
}
