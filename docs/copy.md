# Copy and CopyAll

Backend behaviour, limitations and tuning options for `Storager.Copy` and
`Storager.CopyAll`. The short contract is in the [README](../README.md#usage).

## Copy

`Copy(ctx, srcPath, dstPath, opts...)` copies an object to another key in the
same storage, overwriting the destination.

- A missing source (or a directory) is `storages.ErrFileNotFound`.
- Copying an object onto itself is a no-op. A second name for the same file (a
  symlink, or a different case on a case-insensitive filesystem) never empties
  it either: directory and in-memory treat it as a no-op, while SSH replaces the
  link with a copy of the file.
- The returned `CopyStat` holds the bytes written (`Bytes`, 0 for a copy onto
  itself) and whether they went through the client (`Streamed`).

### Where the bytes go

| Backend   | How `Copy` works                                                                  |
|-----------|-----------------------------------------------------------------------------------|
| S3        | Server-side `CopyObject`; a multipart `UploadPartCopy` above 5 GiB                |
| Azure     | Server-side blob copy, waited on until it completes                               |
| Directory | Local copy (`copy_file_range` on Linux) into a temp file, then renamed into place |
| In-memory | Local copy into a temp file, then renamed into place                              |
| SSH/SFTP  | Streamed through the client into a temp file, then renamed into place             |

### Limitations

- **Azure:** a copy that fails or is aborted while pending leaves an empty
  destination. That includes copies CopyAll cancels after another one failed.
- **Directory, in-memory, SSH:** the destination file is replaced, so its mode
  and owner are not kept and a symlink is replaced rather than written through.
  A directory destination is refused. A hidden `.copy-*` file is left behind if
  the process is killed mid-copy.
- **S3:** the copy is written with the storage's configured storage class and
  SSE, as `PutObject` would write it. Above 5 GiB it keeps the source's content
  headers, metadata and tags, as `CopyObject` does below it. The copy is pinned
  to the source version Copy inspected, so a source overwritten mid-copy fails
  it with `storages.ErrSourceChanged`.

### Options

Options tune a single call:

```go
stat, err := st.Copy(ctx, "big.tar", "copy.tar",
	storages.WithCopyConcurrency(2),       // S3 multipart: default s3.Config.Concurrency, else 5
	storages.WithCopyPartSize(256<<20),    // S3 multipart: default 512 MiB
	storages.WithClientSideCopyFallback(), // Azure: stream the blob if the server-side copy is refused
	storages.WithCopyPollInterval(time.Second, 10*time.Second)) // Azure: default azure.Config.CopyPollInterval, else 50ms..2s
```

## CopyAll

`CopyAll(ctx, srcPrefix, dstPrefix, opts...)` copies every object under a
directory-like prefix to the same relative name under `dstPrefix`.

No storage copies a whole prefix in one request, so CopyAll lists the prefix
once with `List` and then calls the backend's own `Copy` for each object, 8 at a
time by default. It stays server-side on S3 and Azure, one copy request per
object (on S3 the listing's ETag stands in for a HEAD); over SSH every object is
streamed through the client.

- `""` as the source means the whole storage.
- A destination inside the source is fine: the listing is taken first, and
  objects already under that destination are left out.
- Zero-byte S3 folder markers (`dir/` keys) are skipped.
- A prefix holding nothing is `storages.ErrFileNotFound`.
- A listed key that is not a clean relative path (`a//b`, `../x`), legal on S3,
  fails the call with `storages.ErrUnsafeKey` before anything is copied:
  every backend's `Copy` cleans keys, so it could not be copied as named.
- CopyAll is not atomic: the first failed copy cancels the ones still running
  and starts no more, finished copies stay, and the returned `CopyStat` counts
  them (`Objects`, `Bytes`).

### Options

Its options mirror `Copy`'s and are applied to every object:

```go
stat, err := st.CopyAll(ctx, "dumps/2026-10-01", "archive/2026-10-01",
	storages.WithCopyAllObjectConcurrency(16), // objects copied at once: default 8
	storages.WithCopyAllPartConcurrency(2))    // per object, as WithCopyConcurrency
```

On S3 the two limits multiply: up to 16 × 2 part copies can be in flight at once
while large objects are copied.
