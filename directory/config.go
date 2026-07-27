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

package directory

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var ErrRootPathIsRequired = errors.New("root path is required")

// ErrPrefixNotExists reports that Config.Prefix is missing under RootPath and
// WithCreatePrefix was not passed. Test for it with errors.Is.
var ErrPrefixNotExists = errors.New("prefix does not exist")

// Config addresses a directory on the local filesystem, splitting the mount
// point from the sub-tree the way the s3 and ssh configs do.
type Config struct {
	// RootPath is required and must be an existing directory.
	RootPath string
	// Prefix is an optional slash-separated path inside RootPath; the storage is
	// rooted at RootPath/Prefix. It cannot be absolute or escape with "..", and a
	// missing prefix is an error unless WithCreatePrefix is passed.
	Prefix string
}

// DefaultConfig returns an empty Config. Set the required RootPath field on the
// result before passing it to NewStorage.
func DefaultConfig() Config {
	return Config{}
}

// Validate checks that RootPath is an existing directory and that Prefix has a
// usable shape. Whether Prefix exists is not checked here: that depends on
// WithCreatePrefix, which is an option to NewStorage rather than config.
func (d *Config) Validate() error {
	if d.RootPath == "" {
		return ErrRootPathIsRequired
	}
	info, err := os.Stat(d.RootPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("root path %q is not a directory", d.RootPath)
	}
	if _, err := cleanPrefix(d.Prefix); err != nil {
		return err
	}
	return nil
}

// cleanPrefix normalizes a prefix to the slash-separated form joined onto
// RootPath and rejects the shapes that would not stay inside it. A prefix that
// cleans to "." roots the storage at RootPath itself.
func cleanPrefix(prefix string) (string, error) {
	if prefix == "" {
		return "", nil
	}
	if filepath.IsAbs(prefix) {
		return "", fmt.Errorf("prefix %q must be relative to the root path", prefix)
	}
	cleaned := path.Clean(filepath.ToSlash(prefix))
	if cleaned == "." {
		return "", nil
	}
	if path.IsAbs(cleaned) {
		return "", fmt.Errorf("prefix %q must be relative to the root path", prefix)
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("prefix %q reaches outside the root path", prefix)
	}
	return cleaned, nil
}
