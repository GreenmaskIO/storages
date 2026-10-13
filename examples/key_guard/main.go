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

// Command key_guard shows the key guard every constructor applies by default.
// It lays out a temporary directory with a storage root next to a "secret"
// file, then reads that file through the same "../secret.txt" key twice:
//
//  1. through a default (guarded) storage — refused with storages.ErrUnsafeKey
//  2. through a WithUnsafe() storage — the key reaches the filesystem
//
// No external services are needed:
//
//	go run ./examples/key_guard
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/directory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	// <tmp>/secret.txt sits outside the storage root <tmp>/root.
	tmp, err := os.MkdirTemp("", "key_guard")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmp); err != nil {
			fmt.Fprintf(os.Stderr, "remove temp dir: %v\n", err)
		}
	}()

	root := filepath.Join(tmp, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmp, "secret.txt"), []byte("top secret"), 0o644); err != nil {
		return err
	}

	const key = "../secret.txt"

	// 1. Guarded (the default): the key never reaches the filesystem.
	st, err := directory.NewStorage(directory.Config{RootPath: root})
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close guarded storage: %v\n", err)
		}
	}()

	_, err = st.GetObject(ctx, key)
	fmt.Printf("guarded  GetObject(%q): %v (ErrUnsafeKey: %t)\n",
		key, err, errors.Is(err, storages.ErrUnsafeKey))

	// 2. Unsafe: the bare backend joins the key onto the root and reads outside it.
	unsafe, err := directory.NewStorage(directory.Config{RootPath: root}, directory.WithUnsafe())
	if err != nil {
		return err
	}
	defer func() {
		if err := unsafe.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close unsafe storage: %v\n", err)
		}
	}()

	r, err := unsafe.GetObject(ctx, key)
	if err != nil {
		return fmt.Errorf("unsafe GetObject: %w", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close reader: %v\n", err)
		}
	}()
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	fmt.Printf("unsafe   GetObject(%q): %q\n", key, data)
	return nil
}
