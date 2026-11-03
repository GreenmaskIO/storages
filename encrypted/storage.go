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

// Package encrypted wraps a Storager so that every object it writes is an age
// file encrypted to one X25519 identity, and every object it reads is
// decrypted. Keys, listings and the directory tree pass through unchanged;
// sizes are reported in plaintext bytes. How a range read is served is
// explained at the top of range.go.
package encrypted

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"filippo.io/age"

	"github.com/greenmaskio/storages"
)

// Storage is the decorator New returns.
type Storage struct {
	store     storages.Storager
	identity  *age.X25519Identity
	recipient *age.X25519Recipient
	layout    layout
	logger    *slog.Logger
}

var _ storages.Storager = (*Storage)(nil)

// Option configures a Storage.
type Option func(*Storage)

// WithLogger sets the logger for failures met while another is handled. Without
// it the storage logs nothing.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Storage) {
		s.logger = logger
	}
}

// New wraps store so that its objects are encrypted to identity. Pass a guarded
// store: the keys are handed to it unchanged.
func New(store storages.Storager, identity *age.X25519Identity, opts ...Option) (*Storage, error) {
	if store == nil {
		return nil, errors.New("encrypted storage needs a store")
	}
	if identity == nil {
		return nil, errors.New("encrypted storage needs an identity")
	}
	recipient := identity.Recipient()
	payloadOffset, err := measurePayloadOffset(recipient)
	if err != nil {
		return nil, err
	}
	// A range read holds the first headerReadSize bytes and expects the header and
	// nonce in them; one X25519 stanza makes a header of about 200 bytes.
	if payloadOffset > headerReadSize {
		return nil, fmt.Errorf("age header of %d bytes exceeds the %d a range read holds", payloadOffset, headerReadSize)
	}
	s := &Storage{
		store:     store,
		identity:  identity,
		recipient: recipient,
		layout:    layout{payloadOffset: payloadOffset},
		logger:    slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// measurePayloadOffset measures the header one recipient's objects carry by
// encrypting an empty plaintext: header, nonce, and one empty chunk's tag.
func measurePayloadOffset(recipient age.Recipient) (int64, error) {
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipient)
	if err != nil {
		return 0, fmt.Errorf("measure the age header: %w", err)
	}
	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("measure the age header: %w", err)
	}
	return int64(buf.Len()) - tagSize, nil
}

// rewrap returns a Storage over sub with the same identity.
func (s *Storage) rewrap(sub storages.Storager) *Storage {
	return &Storage{
		store:     sub,
		identity:  s.identity,
		recipient: s.recipient,
		layout:    s.layout,
		logger:    s.logger,
	}
}

func (s *Storage) GetCwd() string { return s.store.GetCwd() }

func (s *Storage) Dirname() string { return s.store.Dirname() }

func (s *Storage) ListDir(ctx context.Context) (files []string, dirs []storages.Storager, err error) {
	files, dirs, err = s.store.ListDir(ctx)
	for i, dir := range dirs {
		dirs[i] = s.rewrap(dir)
	}
	return files, dirs, err
}

func (s *Storage) SubStorage(subPath string, relative bool) (storages.Storager, error) {
	sub, err := s.store.SubStorage(subPath, relative)
	if err != nil {
		return nil, err
	}
	return s.rewrap(sub), nil
}

// List reports each object's plaintext size.
func (s *Storage) List(ctx context.Context, prefix string) ([]storages.ObjectStat, error) {
	objects, err := s.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for i := range objects {
		objects[i].Size = s.layout.reportedSize(objects[i].Size)
	}
	return objects, nil
}

// Stat reports the object's plaintext size; a stored size no age file can have
// is reported as is.
func (s *Storage) Stat(fileName string) (*storages.ObjectStat, error) {
	stat, err := s.store.Stat(fileName)
	if err != nil || !stat.Exist {
		return stat, err
	}
	plain := *stat
	plain.Size = s.layout.reportedSize(stat.Size)
	return &plain, nil
}

func (s *Storage) Exists(ctx context.Context, fileName string) (bool, error) {
	return s.store.Exists(ctx, fileName)
}

// PutObject encrypts body on its way to the store; a failed read fails the put.
func (s *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	ciphertext, err := age.EncryptReader(body, s.recipient)
	if err != nil {
		return fmt.Errorf("encrypt %q: %w", filePath, err)
	}
	return s.store.PutObject(ctx, filePath, ciphertext)
}

// GetObject decrypts the object as it is read. An object of another identity
// is ErrNoKey; one that does not authenticate is ErrCorruptObject, possibly
// only once the read reaches the damage.
func (s *Storage) GetObject(ctx context.Context, filePath string) (io.ReadCloser, error) {
	body, err := s.store.GetObject(ctx, filePath)
	if err != nil {
		return nil, err
	}
	src := &sourceReader{reader: body}
	plain, err := age.Decrypt(src, s.identity)
	if err != nil {
		s.closeAfterFailure(filePath, body)
		return nil, newDecryptError(filePath, src.err, err)
	}
	return &plainReader{
		reader: plain,
		src:    src,
		closer: body,
		key:    filePath,
	}, nil
}

// GetObjectRange decrypts length plaintext bytes at offset, or to the end if
// length < 0. It reads the object's header and final chunk besides the chunks
// the range covers.
func (s *Storage) GetObjectRange(ctx context.Context, filePath string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length == 0 {
		return nil, fmt.Errorf("range %d+%d of %q: %w", offset, length, filePath, storages.ErrInvalidRange)
	}
	stat, err := s.store.Stat(filePath)
	if err != nil {
		return nil, err
	}
	if !stat.Exist {
		return nil, fmt.Errorf("object %q: %w", filePath, storages.ErrFileNotFound)
	}
	plainSize, ok := s.layout.plaintextSize(stat.Size)
	if !ok {
		return nil, fmt.Errorf("object %q of %d bytes: %w", filePath, stat.Size, ErrCorruptObject)
	}
	if offset >= plainSize {
		return nil, fmt.Errorf("range %d+%d of %q, %d bytes: %w",
			offset, length, filePath, plainSize, storages.ErrInvalidRange)
	}
	end := plainSize
	if length > 0 && length < plainSize-offset {
		end = offset + length
	}

	src, err := s.openRangeSource(ctx, filePath, stat.Size, offset, end)
	if err != nil {
		return nil, err
	}
	plain, _, err := age.DecryptReaderAt(src, stat.Size, s.identity)
	if err != nil {
		s.closeAfterFailure(filePath, src)
		return nil, newDecryptError(filePath, src.sourceErr(), err)
	}
	return &rangeReader{
		reader: io.NewSectionReader(plain, offset, end-offset),
		src:    src,
		key:    filePath,
	}, nil
}

func (s *Storage) Delete(ctx context.Context, filePaths ...string) error {
	return s.store.Delete(ctx, filePaths...)
}

func (s *Storage) DeleteAll(ctx context.Context, pathPrefix string) error {
	return s.store.DeleteAll(ctx, pathPrefix)
}

// Copy copies the ciphertext, server-side where the store can: an object does
// not depend on its key. CopyStat.Bytes counts plaintext.
func (s *Storage) Copy(
	ctx context.Context, srcPath, dstPath string, opts ...storages.CopyOption,
) (storages.CopyStat, error) {
	stat, err := s.store.Copy(ctx, srcPath, dstPath, opts...)
	if err != nil {
		return storages.CopyStat{}, err
	}
	return s.plainCopyStat(stat), nil
}

// CopyAll copies every object under srcPrefix as Copy does.
func (s *Storage) CopyAll(
	ctx context.Context, srcPrefix, dstPrefix string, opts ...storages.CopyAllOption,
) (storages.CopyStat, error) {
	return storages.CopyEach(ctx, s, srcPrefix, dstPrefix, opts...)
}

func (s *Storage) Ping(ctx context.Context) error { return s.store.Ping(ctx) }

func (s *Storage) Close() error { return s.store.Close() }

// Recipient returns the public key every object is encrypted to.
func (s *Storage) Recipient() *age.X25519Recipient { return s.recipient }

// plainCopyStat reports a copy's bytes in plaintext; a copy onto itself stays 0.
func (s *Storage) plainCopyStat(stat storages.CopyStat) storages.CopyStat {
	if stat.Bytes > 0 {
		stat.Bytes = s.layout.reportedSize(stat.Bytes)
	}
	return stat
}

func (s *Storage) closeAfterFailure(key string, closer io.Closer) {
	if err := closer.Close(); err != nil {
		s.logger.Warn("close object after a failed decrypt", "key", key, "error", err)
	}
}

// newDecryptError tells apart a store that failed, an identity that does not
// match, and an object that does not decrypt.
func newDecryptError(key string, sourceErr, err error) error {
	if sourceErr != nil {
		return fmt.Errorf("read %q: %w", key, sourceErr)
	}
	var noMatch *age.NoIdentityMatchError
	if errors.As(err, &noMatch) {
		return fmt.Errorf("decrypt %q: %w", key, ErrNoKey)
	}
	return fmt.Errorf("decrypt %q: %w: %w", key, ErrCorruptObject, err)
}

// sourceReader remembers the store's own read failure, so that it is not
// mistaken for a corrupt object.
type sourceReader struct {
	reader io.Reader
	err    error
}

func (r *sourceReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = err
	}
	return n, err
}

// plainReader is GetObject's result: the plaintext over the store's body.
type plainReader struct {
	reader io.Reader
	src    *sourceReader
	closer io.Closer
	key    string
}

func (r *plainReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, newDecryptError(r.key, r.src.err, err)
	}
	return n, err
}

func (r *plainReader) Close() error { return r.closer.Close() }

// rangeReader is GetObjectRange's result: a section of the plaintext.
type rangeReader struct {
	reader io.Reader
	src    rangeSource
	key    string
}

func (r *rangeReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, newDecryptError(r.key, r.src.sourceErr(), err)
	}
	return n, err
}

func (r *rangeReader) Close() error { return r.src.Close() }
