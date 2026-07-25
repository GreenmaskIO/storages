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

package inmemory

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		return New("")
	})
}

// The conformance suite pins that the default storage is guarded. What it
// cannot pin is that the opt-out really opts out — that the guard is a wrapper
// New chooses to apply rather than behavior baked into the backend.
func TestWithUnsafeReachesOutsideTheRoot(t *testing.T) {
	ctx := context.Background()

	guarded := New("/store")
	if _, err := guarded.GetObject(ctx, "../victim.txt"); !errors.Is(err, storages.ErrUnsafeKey) {
		t.Errorf("GetObject(../victim.txt) error = %v, want ErrUnsafeKey", err)
	}

	unsafe := New("/store", WithUnsafe())
	// The key is handed straight to the filesystem, which resolves it above the
	// storage root and stores it there.
	if err := unsafe.PutObject(ctx, "../victim.txt", strings.NewReader("SECRET")); err != nil {
		t.Fatalf("PutObject(../victim.txt): %v", err)
	}
	r, err := unsafe.GetObject(ctx, "../victim.txt")
	if err != nil {
		t.Fatalf("GetObject(../victim.txt): %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(content) != "SECRET" {
		t.Errorf("content = %q, want SECRET", content)
	}
}
