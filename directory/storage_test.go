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
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		// t.TempDir supplies an OS-correct, auto-cleaned root, which also exercises
		// the backend on the host OS's native path conventions.
		st, err := NewStorage(Config{Path: t.TempDir()})
		require.NoError(t, err)
		return st
	})
}

// The conformance suite pins that the default storage is guarded. What it
// cannot pin is that the opt-out really opts out — that the guard is a wrapper
// the constructor chooses to apply rather than behavior baked into the backend.
func TestWithUnsafeReachesOutsideTheDirectory(t *testing.T) {
	ctx := context.Background()

	base := t.TempDir()
	root := filepath.Join(base, "store")
	require.NoError(t, os.Mkdir(root, 0o750))
	// A neighbour of the storage root, reachable only by climbing out of it.
	require.NoError(t, os.WriteFile(filepath.Join(base, "victim.txt"), []byte("SECRET"), 0o600))

	guarded, err := NewStorage(Config{Path: root})
	require.NoError(t, err)
	_, err = guarded.GetObject(ctx, "../victim.txt")
	assert.ErrorIs(t, err, storages.ErrUnsafeKey, "the default storage must refuse the climb")

	unsafe, err := NewStorage(Config{Path: root}, WithUnsafe())
	require.NoError(t, err)
	r, err := unsafe.GetObject(ctx, "../victim.txt")
	require.NoError(t, err, "WithUnsafe hands the key straight to the filesystem")
	defer func() { require.NoError(t, r.Close()) }()
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "SECRET", string(content))
}
