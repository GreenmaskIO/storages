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

package encrypted

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"filippo.io/age"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/inmemory"
	"github.com/greenmaskio/storages/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		return newTestStorage(t, inmemory.New(""))
	})
}

func TestStorage_Stat_plaintextSizeAtChunkBoundaries(t *testing.T) {
	tests := []int{0, 1, chunkSize - 1, chunkSize, chunkSize + 1, 2 * chunkSize, 2*chunkSize + 7}
	for _, size := range tests {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			inner := inmemory.New("")
			st := newTestStorage(t, inner)
			content := newContent(size)
			require.NoError(t, st.PutObject(t.Context(), "obj", bytes.NewReader(content)))

			stat, err := st.Stat("obj")
			require.NoError(t, err)
			objects, err := st.List(t.Context(), "")
			require.NoError(t, err)
			raw, err := inner.Stat("obj")
			require.NoError(t, err)

			assert.Equal(t, int64(size), stat.Size)
			require.Len(t, objects, 1)
			assert.Equal(t, int64(size), objects[0].Size)
			assert.Equal(t, encryptedSize(st.layout, int64(size)), raw.Size)
			assert.Equal(t, content, readAll(t, st, "obj"))
		})
	}
}

func TestStorage_GetObjectRange_acrossChunks(t *testing.T) {
	const size = 3*wholeObjectSize + 123
	tests := []struct {
		name           string
		offset, length int64
	}{
		{name: "first byte", offset: 0, length: 1},
		{name: "inside the first chunk", offset: 10, length: 100},
		{name: "across a boundary", offset: chunkSize - 5, length: 10},
		{name: "a whole chunk on its boundaries", offset: 4 * chunkSize, length: chunkSize},
		{name: "across many chunks", offset: chunkSize + 3, length: 5 * chunkSize},
		{name: "far into the object", offset: 2 * wholeObjectSize, length: 3000},
		{name: "the final chunk", offset: size - 10, length: 10},
		{name: "to the end", offset: size - chunkSize - 10, length: -1},
		{name: "clamped past the end", offset: size - 3, length: 100},
		{name: "everything", offset: 0, length: -1},
	}
	inner := inmemory.New("")
	st := newTestStorage(t, inner)
	content := newContent(size)
	require.NoError(t, st.PutObject(t.Context(), "big", bytes.NewReader(content)))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			end := int64(size)
			if tt.length > 0 {
				end = min(end, tt.offset+tt.length)
			}

			got := readRange(t, st, "big", tt.offset, tt.length)

			assert.Equal(t, content[tt.offset:end], got)
		})
	}
}

func TestStorage_GetObjectRange_requests(t *testing.T) {
	const size = 4 * wholeObjectSize
	tests := []struct {
		name           string
		size           int
		offset, length int64
		want           int
	}{
		{name: "a small object is read whole", size: wholeObjectSize / 2, offset: 1000, length: 10, want: 2},
		{name: "chunks near the header share its request", size: size, offset: 10, length: 2 * chunkSize, want: 3},
		{name: "chunks far from the header", size: size, offset: 2 * wholeObjectSize, length: 2 * chunkSize, want: 4},
		{name: "the final chunk", size: size, offset: size - 10, length: -1, want: 3},
		{name: "to the end", size: size, offset: 5, length: -1, want: 3},
		{name: "a length past any end", size: size, offset: 5, length: math.MaxInt64, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := &countingStorage{Storager: inmemory.New("")}
			st := newTestStorage(t, inner)
			require.NoError(t, st.PutObject(t.Context(), "obj", bytes.NewReader(newContent(tt.size))))
			inner.reset()

			readRange(t, st, "obj", tt.offset, tt.length)

			assert.Equal(t, tt.want, inner.reads())
		})
	}
}

func TestStorage_GetObject_OtherIdentity(t *testing.T) {
	inner := inmemory.New("")
	writer := newTestStorage(t, inner)
	reader := newTestStorage(t, inner)
	require.NoError(t, writer.PutObject(t.Context(), "obj", bytes.NewReader(newContent(10))))

	_, getErr := reader.GetObject(t.Context(), "obj")
	_, rangeErr := reader.GetObjectRange(t.Context(), "obj", 0, 1)

	assert.ErrorIs(t, getErr, ErrNoKey)
	assert.ErrorIs(t, rangeErr, ErrNoKey)
}

func TestStorage_GetObject_Corrupt(t *testing.T) {
	tests := []struct {
		name   string
		size   int
		damage func(ciphertext []byte) []byte
	}{
		{name: "truncated", size: 2 * chunkSize, damage: func(c []byte) []byte { return c[:len(c)-1] }},
		{name: "a flipped payload byte", size: 2 * chunkSize, damage: func(c []byte) []byte {
			c[len(c)-20] ^= 1
			return c
		}},
		{name: "never encrypted", size: 2 * chunkSize, damage: func([]byte) []byte { return []byte("plain text") }},
		{name: "a large object with a flipped body byte", size: 3 * wholeObjectSize, damage: func(c []byte) []byte {
			c[len(c)/2] ^= 1
			return c
		}},
		{name: "a large object with a flipped final chunk", size: 3 * wholeObjectSize, damage: func(c []byte) []byte {
			c[len(c)-1] ^= 1
			return c
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			inner := inmemory.New("")
			st := newTestStorage(t, inner)
			require.NoError(t, st.PutObject(t.Context(), "obj", bytes.NewReader(newContent(tt.size))))
			ciphertext := readAll(t, inner, "obj")
			require.NoError(t, inner.PutObject(t.Context(), "obj", bytes.NewReader(tt.damage(ciphertext))))

			// Act
			getErr := readErr(st.GetObject(t.Context(), "obj"))
			rangeErr := readErr(st.GetObjectRange(t.Context(), "obj", 0, -1))

			// Assert
			assert.ErrorIs(t, getErr, ErrCorruptObject)
			assert.ErrorIs(t, rangeErr, ErrCorruptObject)
		})
	}
}

func TestStorage_Stat_TornObject(t *testing.T) {
	inner := inmemory.New("")
	st := newTestStorage(t, inner)
	require.NoError(t, inner.PutObject(t.Context(), "torn", bytes.NewReader([]byte("abc"))))

	stat, err := st.Stat("torn")

	require.NoError(t, err)
	assert.Equal(t, int64(3), stat.Size)
}

func TestStorage_PutObject_StoresAnAgeFile(t *testing.T) {
	inner := inmemory.New("")
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	st, err := New(inner, identity)
	require.NoError(t, err)
	content := []byte("a secret worth hiding")

	require.NoError(t, st.PutObject(t.Context(), "obj", bytes.NewReader(content)))

	ciphertext := readAll(t, inner, "obj")
	assert.NotContains(t, string(ciphertext), string(content))
	plain, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	require.NoError(t, err)
	got, err := io.ReadAll(plain)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestStorage_CopyAll(t *testing.T) {
	// Arrange
	st := newTestStorage(t, inmemory.New(""))
	content := newContent(chunkSize + 1)
	require.NoError(t, st.PutObject(t.Context(), "a/obj", bytes.NewReader(content)))

	// Act
	copied, err := st.CopyAll(t.Context(), "a", "b")

	// Assert
	require.NoError(t, err)
	sub, err := st.SubStorage("b", true)
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), copied.Bytes)
	assert.Equal(t, 1, copied.Objects)
	assert.Equal(t, content, readAll(t, sub, "obj"))
}

func TestStorage_CopyAll_ListedCopy(t *testing.T) {
	// Arrange
	inner := &listedStorage{Storager: inmemory.New("")}
	st := newTestStorage(t, inner)
	contents := map[string][]byte{"a/x": newContent(chunkSize + 1), "a/y": newContent(10)}
	for key, content := range contents {
		require.NoError(t, st.PutObject(t.Context(), key, bytes.NewReader(content)))
	}

	// Act
	copied, err := st.CopyAll(t.Context(), "a", "b")

	// Assert
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"b/x", "b/y"}, inner.copied())
	assert.Equal(t, int64(chunkSize+11), copied.Bytes)
	assert.Equal(t, 2, copied.Objects)
	assert.Equal(t, contents["a/x"], readAll(t, st, "b/x"))
}

func TestStorage_GetObjectRange_CloseEndsABlockedRead(t *testing.T) {
	// Arrange
	const size = 3 * wholeObjectSize
	inner := &blockingStorage{Storager: inmemory.New(""), entered: make(chan struct{})}
	st := newTestStorage(t, inner)
	require.NoError(t, st.PutObject(t.Context(), "obj", bytes.NewReader(newContent(size))))
	stat, err := inner.Stat("obj")
	require.NoError(t, err)
	inner.blockAt, _ = st.layout.chunkSpan(wholeObjectSize, wholeObjectSize+10, stat.Size)
	body, err := st.GetObjectRange(t.Context(), "obj", wholeObjectSize, 10)
	require.NoError(t, err)
	read := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(body)
		read <- err
	}()
	<-inner.entered

	// Act
	closeErr := body.Close()

	// Assert
	require.NoError(t, closeErr)
	select {
	case err := <-read:
		assert.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not end the blocked read")
	}
}

func TestStorage_Copy(t *testing.T) {
	tests := []struct {
		name      string
		dst       string
		wantBytes int64
	}{
		{name: "to another key", dst: "other/obj", wantBytes: 2*chunkSize + 3},
		{name: "onto itself", dst: "obj", wantBytes: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			st := newTestStorage(t, inmemory.New(""))
			content := newContent(2*chunkSize + 3)
			require.NoError(t, st.PutObject(t.Context(), "obj", bytes.NewReader(content)))

			// Act
			copied, err := st.Copy(t.Context(), "obj", tt.dst)

			// Assert
			require.NoError(t, err)
			assert.Equal(t, tt.wantBytes, copied.Bytes)
			assert.Equal(t, content, readAll(t, st, tt.dst))
		})
	}
}

// What ListDir hands back decrypts as the storage itself does.
func TestStorage_ListDir(t *testing.T) {
	// Arrange
	st := newTestStorage(t, inmemory.New(""))
	content := newContent(10)
	require.NoError(t, st.PutObject(t.Context(), "a/obj", bytes.NewReader(content)))

	// Act
	_, dirs, err := st.ListDir(t.Context())

	// Assert
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	assert.Equal(t, content, readAll(t, dirs[0], "obj"))
}

// A store that fails mid-read reports its own error, never a corrupt object.
func TestStorage_GetObject_StoreFailure(t *testing.T) {
	errStore := errors.New("connection reset")
	const large = 3 * wholeObjectSize
	tests := []struct {
		name      string
		size      int
		failAfter int64
		err       error
		read      func(st storages.Storager) error
	}{
		{name: "GetObject in the header", size: 4 * chunkSize, failAfter: 10, err: errStore, read: func(st storages.Storager) error {
			return readErr(st.GetObject(t.Context(), "obj"))
		}},
		{name: "GetObject in the payload", size: 4 * chunkSize, failAfter: chunkSize, err: errStore, read: func(st storages.Storager) error {
			return readErr(st.GetObject(t.Context(), "obj"))
		}},
		{name: "GetObjectRange of a small object", size: 4 * chunkSize, failAfter: chunkSize, err: errStore, read: func(st storages.Storager) error {
			return readErr(st.GetObjectRange(t.Context(), "obj", 0, -1))
		}},
		{name: "GetObjectRange of a large object", size: large, failAfter: 1000, err: errStore, read: func(st storages.Storager) error {
			return readErr(st.GetObjectRange(t.Context(), "obj", wholeObjectSize, 10))
		}},
		{
			name: "a connection dropped mid-range", size: large, failAfter: 1000, err: io.ErrUnexpectedEOF,
			read: func(st storages.Storager) error {
				return readErr(st.GetObjectRange(t.Context(), "obj", wholeObjectSize, 10))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			inner := &failingStorage{Storager: inmemory.New(""), failAfter: tt.failAfter, err: tt.err}
			st := newTestStorage(t, inner)
			require.NoError(t, st.PutObject(t.Context(), "obj", bytes.NewReader(newContent(tt.size))))

			// Act
			err := tt.read(st)

			// Assert
			assert.ErrorIs(t, err, tt.err)
			assert.NotErrorIs(t, err, ErrCorruptObject)
		})
	}
}

func TestStorage_PutObject_FailedReadFailsThePut(t *testing.T) {
	// Arrange
	st := newTestStorage(t, inmemory.New(""))
	errBody := errors.New("boom")

	// Act
	err := st.PutObject(t.Context(), "obj", iotest.ErrReader(errBody))

	// Assert
	assert.ErrorIs(t, err, errBody)
	exists, existsErr := st.Exists(t.Context(), "obj")
	require.NoError(t, existsErr)
	assert.False(t, exists)
}

func TestNew(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	tests := []struct {
		name     string
		store    storages.Storager
		identity *age.X25519Identity
		wantErr  string
	}{
		{name: "no store", identity: identity, wantErr: "needs a store"},
		{name: "no identity", store: inmemory.New(""), wantErr: "needs an identity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Act
			_, err := New(tt.store, tt.identity)

			// Assert
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func newTestStorage(t *testing.T, inner storages.Storager) *Storage {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	st, err := New(inner, identity)
	require.NoError(t, err)
	return st
}

// newContent returns size bytes that differ at every offset a test reads.
func newContent(size int) []byte {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i*7 + i/251)
	}
	return content
}

func readAll(t *testing.T, st storages.Storager, key string) []byte {
	t.Helper()
	body, err := st.GetObject(t.Context(), key)
	require.NoError(t, err)
	defer func() { require.NoError(t, body.Close()) }()
	data, err := io.ReadAll(body)
	require.NoError(t, err)
	return data
}

func readRange(t *testing.T, st storages.Storager, key string, offset, length int64) []byte {
	t.Helper()
	body, err := st.GetObjectRange(t.Context(), key, offset, length)
	require.NoError(t, err)
	defer func() { require.NoError(t, body.Close()) }()
	data, err := io.ReadAll(body)
	require.NoError(t, err)
	return data
}

// readErr reads body to its end and returns the first error met.
func readErr(body io.ReadCloser, err error) error {
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()
	_, err = io.ReadAll(body)
	return err
}

// failingStorage fails every read of an object after failAfter bytes.
type failingStorage struct {
	storages.Storager
	failAfter int64
	err       error
}

func (f *failingStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	body, err := f.Storager.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	return failingBody{
		Reader: io.MultiReader(io.LimitReader(body, f.failAfter), iotest.ErrReader(f.err)),
		Closer: body,
	}, nil
}

// GetObjectRange fails every range body after failAfter bytes.
func (f *failingStorage) GetObjectRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, err := f.Storager.GetObjectRange(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	return failingBody{
		Reader: io.MultiReader(io.LimitReader(body, f.failAfter), iotest.ErrReader(f.err)),
		Closer: body,
	}, nil
}

type failingBody struct {
	io.Reader
	io.Closer
}

// countingStorage counts the reads that reach the store, Stat included.
type countingStorage struct {
	storages.Storager
	mu sync.Mutex
	n  int
}

func (c *countingStorage) Stat(key string) (*storages.ObjectStat, error) {
	c.count()
	return c.Storager.Stat(key)
}

func (c *countingStorage) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	c.count()
	return c.Storager.GetObject(ctx, key)
}

func (c *countingStorage) GetObjectRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	c.count()
	return c.Storager.GetObjectRange(ctx, key, offset, length)
}

func (c *countingStorage) count() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

func (c *countingStorage) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n = 0
}

func (c *countingStorage) reads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// encryptedSize is what an object of plain bytes takes in the store.
func encryptedSize(l layout, plain int64) int64 {
	chunks := max((plain+chunkSize-1)/chunkSize, 1)
	return l.payloadOffset + plain + chunks*tagSize
}

// listedStorage records the listed copies a CopyAll reaches it with.
type listedStorage struct {
	storages.Storager
	mu   sync.Mutex
	dsts []string
}

func (l *listedStorage) CopyListedObject(
	ctx context.Context, _ storages.ObjectStat, srcPath, dstPath string, opts ...storages.CopyOption,
) (storages.CopyStat, error) {
	l.mu.Lock()
	l.dsts = append(l.dsts, dstPath)
	l.mu.Unlock()
	return l.Copy(ctx, srcPath, dstPath, opts...)
}

func (l *listedStorage) copied() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dsts
}

// blockingStorage hands out a range body at blockAt that blocks until it is closed.
type blockingStorage struct {
	storages.Storager
	blockAt int64
	entered chan struct{}
}

func (b *blockingStorage) GetObjectRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset != b.blockAt {
		return b.Storager.GetObjectRange(ctx, key, offset, length)
	}
	return &blockedBody{entered: b.entered, closed: make(chan struct{})}, nil
}

type blockedBody struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (b *blockedBody) Read([]byte) (int, error) {
	close(b.entered)
	<-b.closed
	return 0, errors.New("read on a closed body")
}

func (b *blockedBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}
