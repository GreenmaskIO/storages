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
	"log/slog"
	"os"
	"sync"

	"github.com/greenmaskio/storages"
)

const (
	// wholeObjectSize is the largest object a range read fetches whole, in one
	// request, rather than as its header, final chunk and the chunks it needs.
	wholeObjectSize = 1 << 20
	// headerReadSize is what age reads at offset 0 to parse a header.
	headerReadSize = 4096
	// maxGap is the most a range read skips by reading through rather than by
	// opening another request.
	maxGap = 256 << 10
)

// rangeSource is the ciphertext age decrypts a range from.
type rangeSource interface {
	io.ReaderAt
	io.Closer
	// sourceErr is the store's own failure, if a read met one.
	sourceErr() error
}

// openRangeSource returns the ciphertext source for plaintext [off, end): the
// whole object when it is small, else one that fetches only what age reads.
func (s *Storage) openRangeSource(
	ctx context.Context, key string, encrypted, off, end int64,
) (rangeSource, error) {
	if encrypted <= wholeObjectSize {
		return s.readWholeSource(ctx, key)
	}
	bodyStart, bodyEnd := s.layout.chunkSpan(off, end, encrypted)
	return &chunkSource{
		ctx:       ctx,
		store:     s.store,
		key:       key,
		size:      encrypted,
		tailStart: s.layout.finalChunkOffset(encrypted),
		bodyStart: bodyStart,
		bodyEnd:   bodyEnd,
		logger:    s.logger,
	}, nil
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

// chunkSource serves age's reads of one object with as few requests as it can:
// the final chunk and the header's read kept in memory, everything else through one
// forward-only stream that reads through a small gap instead of reopening. A
// header close to the chunks the range needs comes in the same request.
type chunkSource struct {
	ctx       context.Context
	store     storages.Storager
	key       string
	size      int64
	tailStart int64
	bodyStart int64
	bodyEnd   int64
	logger    *slog.Logger

	// mu serialises age's reads.
	mu     sync.Mutex
	head   []byte
	tail   []byte
	stream *rangeBody
	pos    int64
	end    int64
	err    error

	// streamMu guards replacing the stream against Close, which does not wait for a read.
	streamMu sync.Mutex
	closed   bool
}

func (c *chunkSource) ReadAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed() {
		return 0, fmt.Errorf("read %q: %w", c.key, os.ErrClosed)
	}

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
			read, err = c.readTail(p[n:], at)
		case at < int64(len(c.head)):
			read = copy(p[n:], c.head[at:])
		default:
			read, err = c.readStream(p[n:int(min(int64(len(p)), c.tailStart-off))], at)
		}
		n += read
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (c *chunkSource) readTail(p []byte, at int64) (int, error) {
	if c.tail == nil {
		tail, err := c.fetch(c.tailStart, c.size-c.tailStart)
		if err != nil {
			return 0, err
		}
		c.tail = tail
	}
	return copy(p, c.tail[at-c.tailStart:]), nil
}

func (c *chunkSource) fetch(off, length int64) ([]byte, error) {
	stream, err := c.store.GetObjectRange(c.ctx, c.key, off, length)
	if err != nil {
		return nil, c.fail(err)
	}
	body := &rangeBody{ReadCloser: stream}
	data, err := io.ReadAll(body)
	c.closeBody(body)
	if err != nil {
		return nil, c.failRead(body, err)
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("read %d bytes at %d of %q, got %d: %w", length, off, c.key, len(data), io.ErrUnexpectedEOF)
	}
	return data, nil
}

func (c *chunkSource) readStream(p []byte, at int64) (int, error) {
	if c.stream == nil || at < c.pos || at-c.pos > maxGap || at >= c.end {
		if err := c.openStream(at, len(p)); err != nil {
			return 0, err
		}
	}
	if skip := at - c.pos; skip > 0 {
		if _, err := io.CopyN(io.Discard, c.stream, skip); err != nil {
			return 0, c.failRead(c.stream, err)
		}
		c.pos = at
	}
	p = p[:min(int64(len(p)), c.end-at)]
	n, err := io.ReadFull(c.stream, p)
	c.pos += int64(n)
	if err != nil {
		return n, c.failRead(c.stream, err)
	}
	if at == 0 {
		// age parses the header through a buffer that reads past it, then
		// reads the start of the payload again.
		c.head = bytes.Clone(p[:min(n, headerReadSize)])
	}
	return n, nil
}

// openStream opens the stream at at, to the end of the chunks the range needs
// when it reads them or lies close enough before them, else for want bytes.
func (c *chunkSource) openStream(at int64, want int) error {
	c.dropStream()
	end := min(at+int64(max(want, headerReadSize)), c.tailStart)
	if at < c.bodyEnd && c.bodyStart < c.tailStart && c.bodyStart-(at+int64(want)) <= maxGap {
		end = max(end, min(c.bodyEnd, c.tailStart))
	}
	stream, err := c.store.GetObjectRange(c.ctx, c.key, at, end-at)
	if err != nil {
		return c.fail(err)
	}
	body := &rangeBody{ReadCloser: stream}
	if !c.setStream(body) {
		c.closeBody(body)
		return fmt.Errorf("read %q: %w", c.key, os.ErrClosed)
	}
	c.pos, c.end = at, end
	return nil
}

func (c *chunkSource) setStream(body *rangeBody) bool {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	if c.closed {
		return false
	}
	c.stream = body
	return true
}

func (c *chunkSource) dropStream() {
	c.streamMu.Lock()
	stream := c.stream
	c.stream = nil
	c.streamMu.Unlock()
	if stream != nil {
		c.closeBody(stream)
	}
}

func (c *chunkSource) isClosed() bool {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	return c.closed
}

// failRead is fail for a read of body: a failure the body itself returned is the store's,
// however it is spelled; one that ended cleanly short of the range is the object's.
func (c *chunkSource) failRead(body *rangeBody, err error) error {
	if body.err != nil {
		c.err = body.err
		return body.err
	}
	return c.fail(err)
}

// fail records err as the store's own failure; a read cut short, or past an end that moved, is
// the object's, as age reports a truncated chunk.
func (c *chunkSource) fail(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, storages.ErrInvalidRange) {
		return fmt.Errorf("read %q: %w", c.key, io.ErrUnexpectedEOF)
	}
	c.err = err
	return err
}

func (c *chunkSource) sourceErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *chunkSource) closeBody(body io.Closer) {
	if err := body.Close(); err != nil {
		c.logger.Warn("close object range", "key", c.key, "error", err)
	}
}

// Close closes the stream without waiting for a read on it, which it ends.
func (c *chunkSource) Close() error {
	c.streamMu.Lock()
	c.closed = true
	stream := c.stream
	c.streamMu.Unlock()
	if stream == nil {
		return nil
	}
	return stream.Close()
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
