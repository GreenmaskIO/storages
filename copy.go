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

package storages

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// CopyStat reports what a successful Copy or CopyAll did.
type CopyStat struct {
	// Bytes is the number of bytes written; a copy onto itself reports 0.
	Bytes int64
	// Streamed reports that the data passed through the client rather than
	// being copied server-side. CopyAll sets it when any of its copies did.
	Streamed bool
	// Objects is the number of objects CopyAll copied; Copier.Copy leaves it 0.
	Objects int
}

// CopyOptions is the resolved set of CopyOption values; a zero field means the
// backend default. Backends build it with NewCopyOptions.
type CopyOptions struct {
	// Concurrency caps how many part copies a backend has in flight at once.
	Concurrency int
	// PartSize is the number of bytes each part copy covers.
	PartSize int64
	// ClientSideFallback lets a backend whose server-side copy is refused
	// stream the object through the client instead of failing.
	ClientSideFallback bool
	// PollInterval and PollMaxInterval bound polling of an asynchronous copy.
	PollInterval    time.Duration
	PollMaxInterval time.Duration
}

// CopyOption tunes a single Copier.Copy call.
type CopyOption func(*CopyOptions)

// WithCopyConcurrency caps concurrent part copies of an s3 multipart copy,
// overriding s3.Config.Concurrency. A value below 1 keeps the default.
func WithCopyConcurrency(n int) CopyOption {
	return func(o *CopyOptions) {
		o.Concurrency = max(n, 0)
	}
}

// WithCopyPartSize sets the s3 multipart copy part size, raised as needed to fit
// S3's 5 MiB minimum and 10000-part limit. A value below 1 keeps the default.
func WithCopyPartSize(n int64) CopyOption {
	return func(o *CopyOptions) {
		o.PartSize = max(n, 0)
	}
}

// WithClientSideCopyFallback makes an azure copy refused server-side stream
// through the client with StreamCopy instead of returning the refusal.
func WithClientSideCopyFallback() CopyOption {
	return func(o *CopyOptions) {
		o.ClientSideFallback = true
	}
}

// WithCopyPollInterval sets azure copy polling, doubling from interval up to
// maxInterval, overriding azure.Config. A value below 1 keeps the default.
func WithCopyPollInterval(interval, maxInterval time.Duration) CopyOption {
	return func(o *CopyOptions) {
		o.PollInterval = max(interval, 0)
		o.PollMaxInterval = max(maxInterval, 0)
	}
}

// NewCopyOptions applies opts in order, skipping nil, for Copier implementations.
func NewCopyOptions(opts ...CopyOption) CopyOptions {
	var o CopyOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// StreamCopy copies via GetObject and PutObject, for backends without a native
// copy. Only a same-key copy is detected; aliases and directories are not.
func StreamCopy(ctx context.Context, st Storager, srcPath, dstPath string) (CopyStat, error) {
	if sameKey(srcPath, dstPath) {
		ok, err := st.Exists(ctx, srcPath)
		if err != nil {
			return CopyStat{}, fmt.Errorf("error checking %q: %w", srcPath, err)
		}
		if !ok {
			return CopyStat{}, ErrFileNotFound
		}
		return CopyStat{}, nil
	}

	src, err := st.GetObject(ctx, srcPath)
	if err != nil {
		return CopyStat{}, err
	}
	counted := &countingReader{r: src}
	werr := st.PutObject(ctx, dstPath, counted)
	cerr := src.Close()
	if werr != nil {
		return CopyStat{}, fmt.Errorf("error writing copy: %w", werr)
	}
	if cerr != nil {
		return CopyStat{}, fmt.Errorf("error closing copy source: %w", cerr)
	}
	// PutObject has returned, so nothing reads through counted any more.
	return CopyStat{Bytes: counted.n, Streamed: true}, nil
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// sameKey reports whether two keys name the same object once normalized the way
// every backend resolves them.
func sameKey(a, b string) bool {
	return path.Clean(filepath.ToSlash(a)) == path.Clean(filepath.ToSlash(b))
}

// defaultCopyAllObjectConcurrency is CopyEach's object concurrency when none is
// given.
const defaultCopyAllObjectConcurrency = 8

// CopyAllOptions is the resolved set of CopyAllOption values; a zero field means
// the default. Fields other than ObjectConcurrency are passed to every Copy.
type CopyAllOptions struct {
	// ObjectConcurrency caps how many objects are copied at once.
	ObjectConcurrency int
	// PartConcurrency is passed as CopyOptions.Concurrency.
	PartConcurrency int
	// PartSize is passed as CopyOptions.PartSize.
	PartSize int64
	// ClientSideFallback is passed as CopyOptions.ClientSideFallback.
	ClientSideFallback bool
	// PollInterval and PollMaxInterval are passed as the same CopyOptions fields.
	PollInterval    time.Duration
	PollMaxInterval time.Duration
}

// CopyAllOption tunes a single CopyAll call.
type CopyAllOption func(*CopyAllOptions)

// WithCopyAllObjectConcurrency caps concurrent object copies (default 8). On s3,
// up to its product with WithCopyAllPartConcurrency part copies run at once.
func WithCopyAllObjectConcurrency(n int) CopyAllOption {
	return func(o *CopyAllOptions) {
		o.ObjectConcurrency = max(n, 0)
	}
}

// WithCopyAllPartConcurrency is WithCopyConcurrency for every object copied.
func WithCopyAllPartConcurrency(n int) CopyAllOption {
	return func(o *CopyAllOptions) {
		o.PartConcurrency = max(n, 0)
	}
}

// WithCopyAllPartSize is WithCopyPartSize for every object copied.
func WithCopyAllPartSize(n int64) CopyAllOption {
	return func(o *CopyAllOptions) {
		o.PartSize = max(n, 0)
	}
}

// WithCopyAllClientSideFallback is WithClientSideCopyFallback for every object
// copied.
func WithCopyAllClientSideFallback() CopyAllOption {
	return func(o *CopyAllOptions) {
		o.ClientSideFallback = true
	}
}

// WithCopyAllPollInterval is WithCopyPollInterval for every object copied.
func WithCopyAllPollInterval(interval, maxInterval time.Duration) CopyAllOption {
	return func(o *CopyAllOptions) {
		o.PollInterval = max(interval, 0)
		o.PollMaxInterval = max(maxInterval, 0)
	}
}

// NewCopyAllOptions applies opts in order, skipping nil.
func NewCopyAllOptions(opts ...CopyAllOption) CopyAllOptions {
	var o CopyAllOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// copyOptions converts the per-object fields into Copy options.
func (o CopyAllOptions) copyOptions() []CopyOption {
	opts := []CopyOption{
		WithCopyConcurrency(o.PartConcurrency),
		WithCopyPartSize(o.PartSize),
		WithCopyPollInterval(o.PollInterval, o.PollMaxInterval),
	}
	if o.ClientSideFallback {
		opts = append(opts, WithClientSideCopyFallback())
	}
	return opts
}

// ObjectCopyFunc copies one listed object; obj is what the listing reported.
type ObjectCopyFunc func(ctx context.Context, obj ObjectStat, srcPath, dstPath string, opts ...CopyOption) (CopyStat, error)

// CopyEach implements Copier.CopyAll with st's List and Copy.
func CopyEach(ctx context.Context, st Storager, srcPrefix, dstPrefix string, opts ...CopyAllOption) (CopyStat, error) {
	copyObject := func(ctx context.Context, _ ObjectStat, srcPath, dstPath string, o ...CopyOption) (CopyStat, error) {
		return st.Copy(ctx, srcPath, dstPath, o...)
	}
	return CopyListed(ctx, st, srcPrefix, dstPrefix, copyObject, opts...)
}

// CopyListed implements Copier.CopyAll for a backend whose copy can use what
// the listing reported: it lists srcPrefix once, then calls copyObject per object.
func CopyListed(
	ctx context.Context, st Inspector, srcPrefix, dstPrefix string, copyObject ObjectCopyFunc, opts ...CopyAllOption,
) (CopyStat, error) {
	o := NewCopyAllOptions(opts...)

	objects, err := st.List(ctx, srcPrefix)
	if err != nil {
		return CopyStat{}, fmt.Errorf("error listing %q: %w", srcPrefix, err)
	}
	// A key ending in "/" is an S3 folder marker that no backend can Copy.
	objects = slices.DeleteFunc(objects, func(o ObjectStat) bool {
		return strings.HasSuffix(o.Name, "/")
	})
	if len(objects) == 0 {
		return CopyStat{}, fmt.Errorf("nothing to copy under %q: %w", srcPrefix, ErrFileNotFound)
	}
	// st may be an unguarded backend, so a listed key is checked here: joined
	// onto a prefix, "a/../../x" would land outside it, and "a//b" on another key.
	for _, obj := range objects {
		if !cleanRelative(obj.Name) {
			return CopyStat{}, fmt.Errorf("%w: listed key %q under %q", ErrUnsafeKey, obj.Name, srcPrefix)
		}
	}
	if nested, ok := nestedPrefix(srcPrefix, dstPrefix); ok {
		objects = slices.DeleteFunc(objects, func(o ObjectStat) bool {
			// An object named exactly like the destination is not under it.
			return strings.HasPrefix(o.Name, nested+"/")
		})
		if len(objects) == 0 {
			return CopyStat{}, nil
		}
	}

	concurrency := o.ObjectConcurrency
	if concurrency < 1 {
		concurrency = defaultCopyAllObjectConcurrency
	}
	copyOpts := o.copyOptions()

	var (
		mu    sync.Mutex
		total CopyStat
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for _, obj := range objects {
		g.Go(func() error {
			// Go may unblock after a failure or cancel: skip, do not start.
			if err := gctx.Err(); err != nil {
				return err
			}
			src, dst := path.Join(srcPrefix, obj.Name), path.Join(dstPrefix, obj.Name)
			stat, err := copyObject(gctx, obj, src, dst, copyOpts...)
			if err != nil {
				return fmt.Errorf("error copying %q to %q: %w", src, dst, err)
			}
			mu.Lock()
			total.Bytes += stat.Bytes
			total.Streamed = total.Streamed || stat.Streamed
			total.Objects++
			mu.Unlock()
			return nil
		})
	}
	err = g.Wait()
	return total, err
}

// nestedPrefix returns dstPrefix relative to srcPrefix when it lies strictly
// inside it.
func nestedPrefix(srcPrefix, dstPrefix string) (string, bool) {
	src, dst := normPrefix(srcPrefix), normPrefix(dstPrefix)
	switch {
	case dst == src:
		return "", false
	case src == "":
		return dst, dst != ".." && !strings.HasPrefix(dst, "../")
	case strings.HasPrefix(dst, src+"/"):
		return strings.TrimPrefix(dst, src+"/"), true
	}
	return "", false
}

// normPrefix cleans a prefix the way the backends resolve it: a leading "/" is
// dropped, as path.Join does, and the root is "" rather than ".".
func normPrefix(p string) string {
	return strings.TrimPrefix(path.Clean("/"+filepath.ToSlash(p)), "/")
}

// cleanRelative reports whether name is a relative path that path.Join keeps
// as it is and that stays inside the prefix it is joined onto.
func cleanRelative(name string) bool {
	return name != "" && !path.IsAbs(name) && path.Clean(name) == name &&
		name != ".." && !strings.HasPrefix(name, "../")
}
