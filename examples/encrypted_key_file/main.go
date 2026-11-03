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

// Command encrypted_key_file shows the key lifecycle of an encrypted storage:
// how an application creates an age identity once, keeps it in a file in the
// format age-keygen writes, loads it on every start, and wraps a storage with it.
//
// The key file is the same one `age-keygen -o storage.key` produces, so the
// objects stay readable with the age CLI (`age -d -i storage.key < object`)
// without this program.
//
// No external services are needed:
//
//	go run ./examples/encrypted_key_file
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"filippo.io/age"

	"github.com/greenmaskio/storages/directory"
	"github.com/greenmaskio/storages/encrypted"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	tmp, err := os.MkdirTemp("", "encrypted_key_file")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmp); err != nil {
			fmt.Fprintf(os.Stderr, "remove temp dir: %v\n", err)
		}
	}()
	keyPath := filepath.Join(tmp, "storage.key")

	// 1. First start: there is no key yet, so create one and keep it. Losing this
	//    file loses every object written with it; back it up like a password.
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		return err
	}
	if err := writeKeyFile(keyPath, identity); err != nil {
		return err
	}
	keyFile, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	fmt.Printf("key file %s:\n%s\n", keyPath, keyFile)

	// 2. Every start: load the identity from the file. The application never
	//    holds the key as anything but this parsed value.
	loaded, err := loadIdentity(keyPath)
	if err != nil {
		return err
	}

	// 3. Wrap any storage with it. The plaintext never reaches the backend.
	root := filepath.Join(tmp, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		return err
	}
	inner, err := directory.NewStorage(directory.Config{RootPath: root})
	if err != nil {
		return err
	}
	st, err := encrypted.New(inner, loaded)
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close storage: %v\n", err)
		}
	}()
	// The public key is safe to log: compare it with the one in the key file's
	// comment, or with the one the storage was first written with.
	fmt.Printf("storage encrypts to %s\n\n", st.Recipient())

	const key = "reports/2026.txt"
	plaintext := []byte("quarterly numbers, not for the storage provider")
	if err := st.PutObject(ctx, key, bytes.NewReader(plaintext)); err != nil {
		return err
	}
	got, err := readObject(ctx, st, key)
	if err != nil {
		return err
	}
	raw, err := readObject(ctx, inner, key)
	if err != nil {
		return err
	}
	fmt.Printf("through encrypted: %q\n", got)
	fmt.Printf("on disk:           %q... (%d bytes)\n", raw[:len("age-encryption.org/v1")], len(raw))

	// 4. Another identity cannot read the objects: ErrNoKey, not a corrupt object.
	other, err := age.GenerateX25519Identity()
	if err != nil {
		return err
	}
	wrong, err := encrypted.New(inner, other)
	if err != nil {
		return err
	}
	_, err = readObject(ctx, wrong, key)
	fmt.Printf("with another key:  %v (ErrNoKey: %t)\n", err, errors.Is(err, encrypted.ErrNoKey))
	return nil
}

// writeKeyFile writes identity to path in the format age-keygen uses: the
// creation time and the public key as comments, then the secret key. Only the
// owner can read it.
func writeKeyFile(path string, identity *age.X25519Identity) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "# created: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(&buf, "# public key: %s\n", identity.Recipient())
	fmt.Fprintf(&buf, "%s\n", identity)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

// loadIdentity reads the one X25519 identity in an age-keygen key file.
// encrypted.New needs exactly that type: the plaintext sizes it reports are
// computed for one X25519 recipient, so a post-quantum or a second identity is
// refused here with a message naming the file.
func loadIdentity(path string) (*age.X25519Identity, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close key file: %v\n", err)
		}
	}()
	identities, err := age.ParseIdentities(f)
	if err != nil {
		return nil, fmt.Errorf("parse key file %s: %w", path, err)
	}
	if len(identities) != 1 {
		return nil, fmt.Errorf("key file %s holds %d identities, need exactly one", path, len(identities))
	}
	identity, ok := identities[0].(*age.X25519Identity)
	if !ok {
		return nil, fmt.Errorf("key file %s holds a %T, need an X25519 identity (AGE-SECRET-KEY-1...)", path, identities[0])
	}
	return identity, nil
}

func readObject(ctx context.Context, st interface {
	GetObject(context.Context, string) (io.ReadCloser, error)
}, key string) ([]byte, error) {
	r, err := st.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := r.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close object: %v\n", err)
		}
	}()
	return io.ReadAll(r)
}
