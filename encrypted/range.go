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

// Range reads.
//
// An age file is a text header (the file key wrapped to the recipient), a 16-byte
// nonce, then the payload: 64 KiB chunks of ChaCha20-Poly1305, each followed by a
// 16-byte tag. A chunk authenticates only as a whole, so a plaintext range is served
// by decrypting every chunk it touches.
//
// age.DecryptReaderAt touches three regions of the ciphertext, in this order:
//
//  1. the header, read through a 4 KiB bufio buffer at offset 0; the nonce comes
//     out of the same buffer;
//  2. the final chunk, decrypted at open: its nonce carries the last-chunk flag,
//     which is how a truncated object is refused before any byte is served;
//  3. the chunks the range covers, one full chunk per ReadAt, in increasing order,
//     never backwards (the reader keeps a one-chunk cache for a read that ends
//     mid-chunk).
//
// chunkSource fetches 1 and 2 into memory when it opens, and opens 3 as one forward
// stream that it reads as age asks. That is one request per region, decided up
// front, with at most 4 KiB + one chunk held in memory however long the range is.
// An object of up to wholeObjectSize is cheaper to fetch whole, in one request.
//
// The forward stream relies on age reading chunk offsets in non-decreasing order; a
// read behind the stream's position is a bug, not a case to serve, and fails loudly.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/greenmaskio/storages"
)

const (
	// wholeObjectSize is the largest object a range read fetches whole, in one
	// request, rather than as its header, final chunk and the chunks it needs.
	wholeObjectSize = 1 << 20
	// headerReadSize is what age reads at offset 0 to parse a header.
	headerReadSize = 4096
)

// rangeSource is the ciphertext age decrypts a range from.
type rangeSource interface {
	io.ReaderAt
	io.Closer
	// sourceErr returns a failure that is not the object's: the store's own, or a
	// read age should never have made.
	sourceErr() error
}

// openRangeSource returns the ciphertext source for plaintext [off, end). A store
// failure is returned as is; an object shorter than its Stat said is ErrCorruptObject.
func (s *Storage) openRangeSource(
	ctx context.Context, key string, encrypted, off, end int64,
) (rangeSource, error) {
	if encrypted <= wholeObjectSize {
		return s.readWholeSource(ctx, key)
	}
	return s.openChunkSource(ctx, key, encrypted, off, end)
}

func (s *Storage) readWholeSource(ctx context.Context, key string) (rangeSource, error) {
	body, err := s.store.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); closeErr != nil {
		s.logger.Warn("close object", "key", key, "error", closeErr)
	}
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", key, err)
	}
	return wholeSource{Reader: bytes.NewReader(data)}, nil
}

// wholeSource is an object read whole into memory.
type wholeSource struct{ *bytes.Reader }

func (wholeSource) Close() error { return nil }

func (wholeSource) sourceErr() error { return nil }

// openChunkSource fetches the header and the final chunk, and opens the chunks
// of plaintext [off, end) as one stream, cut where the final chunk begins.
func (s *Storage) openChunkSource(
	ctx context.Context, key string, encrypted, off, end int64,
) (rangeSource, error) {
	bodyStart, bodyEnd := s.layout.chunkSpan(off, end, encrypted)
	tailStart := s.layout.finalChunkOffset(encrypted)
	head, err := s.fetch(ctx, key, 0, headerReadSize)
	if err != nil {
		return nil, err
	}
	tail, err := s.fetch(ctx, key, tailStart, encrypted-tailStart)
	if err != nil {
		return nil, err
	}
	c := &chunkSource{
		key:           key,
		size:          encrypted,
		payloadOffset: s.layout.payloadOffset,
		tailStart:     tailStart,
		streamEnd:     min(bodyEnd, tailStart),
		head:          head,
		tail:          tail,
		pos:           bodyStart,
		logger:        s.logger,
	}
	if bodyStart < c.streamEnd {
		stream, err := s.store.GetObjectRange(ctx, key, bodyStart, c.streamEnd-bodyStart)
		if err != nil {
			return nil, err
		}
		c.stream = &rangeBody{ReadCloser: stream}
	}
	return c, nil
}

// fetch reads ciphertext [off, off+length) into memory. A short body, or a range the
// store no longer has, is ErrCorruptObject; a store failure is returned as is.
func (s *Storage) fetch(ctx context.Context, key string, off, length int64) ([]byte, error) {
	body, err := s.store.GetObjectRange(ctx, key, off, length)
	if err != nil {
		if errors.Is(err, storages.ErrInvalidRange) {
			return nil, fmt.Errorf("read %d bytes at %d of %q: %w: %w", length, off, key, ErrCorruptObject, err)
		}
		return nil, fmt.Errorf("read %q: %w", key, err)
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); closeErr != nil {
		s.logger.Warn("close object range", "key", key, "error", closeErr)
	}
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", key, err)
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("read %d bytes at %d of %q, got %d: %w", length, off, key, len(data), ErrCorruptObject)
	}
	return data, nil
}

// chunkSource serves age's reads of one large object: the header and the final
// chunk from memory, the chunks in between from one forward stream.
type chunkSource struct {
	key           string
	size          int64
	payloadOffset int64
	// tailStart is where the final chunk, and tail, begin.
	tailStart int64
	// streamEnd is where stream ends: the range's last chunk or tailStart, whichever is first.
	streamEnd int64
	head      []byte
	tail      []byte
	// stream is nil when the range lies within the final chunk.
	stream *rangeBody
	// pos is the ciphertext offset stream is at.
	pos    int64
	err    error
	logger *slog.Logger
}

// ReadAt serves ciphertext [off, off+len(p)) from the head, the stream or the tail,
// crossing between them as needed. A read behind the stream's position is an error.
func (c *chunkSource) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	for n < len(p) {
		at := off + int64(n)
		if at >= c.size {
			return n, io.EOF
		}
		var (
			read int
			err  error
		)
		switch {
		case at >= c.tailStart:
			read = copy(p[n:], c.tail[at-c.tailStart:])
		case at < c.payloadOffset:
			read = copy(p[n:], c.head[at:])
		default:
			read, err = c.readStream(p[n:], at)
		}
		n += read
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// readStream reads the stream at at, skipping forward to it, up to streamEnd. A store
// failure is remembered as the source's; a body that ended early is io.ErrUnexpectedEOF.
func (c *chunkSource) readStream(p []byte, at int64) (int, error) {
	if c.stream == nil || at < c.pos || at >= c.streamEnd {
		return 0, c.fail(fmt.Errorf("read %q at %d, outside the stream at [%d, %d): %w",
			c.key, at, c.pos, c.streamEnd, errBackwardRead))
	}
	if skip := at - c.pos; skip > 0 {
		if _, err := io.CopyN(io.Discard, c.stream, skip); err != nil {
			return 0, c.failRead()
		}
		c.pos = at
	}
	p = p[:min(int64(len(p)), c.streamEnd-at)]
	n, err := io.ReadFull(c.stream, p)
	c.pos += int64(n)
	if err != nil {
		return n, c.failRead()
	}
	return n, nil
}

// errBackwardRead is a read age's order rules out: behind the stream's position or
// outside the chunks the range covers.
var errBackwardRead = errors.New("read outside the forward stream")

// failRead classifies a failed stream read: a failure the body returned itself
// is the store's; a body that ended cleanly short of the range is the object's.
func (c *chunkSource) failRead() error {
	if c.stream.err != nil {
		return c.fail(c.stream.err)
	}
	return fmt.Errorf("read %q: %w", c.key, io.ErrUnexpectedEOF)
}

// fail remembers err as the source's own failure, not the object's.
func (c *chunkSource) fail(err error) error {
	c.err = err
	return err
}

func (c *chunkSource) sourceErr() error { return c.err }

// Close closes the stream, ending a read blocked on it.
func (c *chunkSource) Close() error {
	if c.stream == nil {
		return nil
	}
	return c.stream.Close()
}

// rangeBody is a range response's body, remembering the failure it returned itself.
type rangeBody struct {
	io.ReadCloser
	err error
}

func (b *rangeBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		b.err = err
	}
	return n, err
}
