# storages

[![Go Reference](https://pkg.go.dev/badge/github.com/greenmaskio/storages.svg)](https://pkg.go.dev/github.com/greenmaskio/storages)
[![CI](https://github.com/GreenmaskIO/storages/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GreenmaskIO/storages/actions/workflows/ci.yml)
[![GitHub Release](https://img.shields.io/github/v/release/greenmaskio/storages)](https://github.com/greenmaskio/storages/releases/latest)
[![Go Version](https://img.shields.io/github/go-mod/go-version/greenmaskio/storages)](https://github.com/greenmaskio/storages/blob/main/go.mod)
[![License](https://img.shields.io/github/license/greenmaskio/storages)](https://github.com/greenmaskio/storages/blob/main/LICENSE)
[![Discord](https://img.shields.io/discord/1179422525294399488?label=Discord&logo=discord)](https://discord.com/invite/rKBKvDECfd)

Pluggable, backend-agnostic object storage for Go. One `Storager` interface,
interchangeable backends. Object CRUD, byte-range reads and recursive listing
with sizes — no presigned URLs or other provider-specific features. Keys are
guarded by default, so none of them can address anything outside the storage.

Extracted from [greenmask](https://github.com/greenmaskio/greenmask).

## Supported backends

| Backend | Package | Use for |
|---|---|---|
| [Directory](#directory) | [`directory`](directory) | Local filesystem |
| [S3](#s3) | [`s3`](s3) | Amazon S3 and compatibles: MinIO, Ceph/RGW, Backblaze B2 |
| [Azure Blob](#azure-blob) | [`azure`](azure) | Azure Blob Storage |
| [SSH/SFTP](#sshsftp) | [`ssh`](ssh) | Remote host over SFTP |
| [In-memory](#in-memory) | [`inmemory`](inmemory) | Tests — no I/O, no services |

Need something else? See [Writing your own backend](#writing-your-own-backend).

## Install

```sh
go get github.com/greenmaskio/storages
```

Requires Go 1.25+.

## Usage

Write your code against `storages.Storager`; pick the backend at construction:

```go
st, err := directory.NewStorage(directory.Config{RootPath: "/var/dumps"})
if err != nil {
	return err
}
defer st.Close()

err = st.PutObject(ctx, "reports/2023.txt", strings.NewReader("annual report"))
files, err := storages.Walk(ctx, st)     // -> ["reports/2023.txt"]
objects, err := st.List(ctx, "reports")  // -> [{Name: "2023.txt", Size: 13, ...}]
chunk, err := st.GetObjectRange(ctx, "reports/2023.txt", 7, 6) // -> "report"
```

```go
type Storager interface {
	GetCwd() string
	Dirname() string
	ListDir(ctx context.Context) (files []string, dirs []Storager, err error)
	List(ctx context.Context, prefix string) ([]ObjectStat, error)
	GetObject(ctx context.Context, filePath string) (io.ReadCloser, error)
	GetObjectRange(ctx context.Context, filePath string, offset, length int64) (io.ReadCloser, error)
	PutObject(ctx context.Context, filePath string, body io.Reader) error
	Delete(ctx context.Context, filePaths ...string) error
	DeleteAll(ctx context.Context, pathPrefix string) error
	Exists(ctx context.Context, fileName string) (bool, error)
	SubStorage(subPath string, relative bool) (Storager, error)
	Stat(fileName string) (*ObjectStat, error)
	Ping(ctx context.Context) error
	Close() error
}
```

Semantics, uniform across all backends:

- Object paths are relative to the storage root and use forward slashes on
  every OS. `SubStorage` returns a `Storager` rooted at a sub-path; it fails
  only when the storage refuses the path (see the key guard below).
- `Delete` is object-level and never recursive; `DeleteAll` is the recursive one.
- **Deleting a missing path is an error, not a no-op** — and nothing gets
  deleted. The error is a `*storages.MissingObjectsError` (with the offending
  paths in `.Paths`) wrapping `storages.ErrFileNotFound`. Code that retries
  deletions should treat `errors.Is(err, storages.ErrFileNotFound)` as success.
- `Stat` reports a missing object as `Exist: false` with a nil error.
- `GetObject` returns `storages.ErrFileNotFound` for a missing object.
- `GetObjectRange` transfers only the requested bytes (an HTTP `Range` on S3 and
  Azure, an offset read over SFTP). A negative `length` means "to the end"; a
  range running past the end is clamped, while one that can yield nothing at all
  — offset at or past the object's size, `length == 0`, negative offset — is
  `storages.ErrInvalidRange`.
- `List` is flat and recursive: names relative to the prefix, slash-separated,
  sorted, each with `Size`. The prefix is directory-like, so `data` never matches
  `database`, and a prefix holding nothing is an empty slice with a nil error.

## Safety: the key guard

Storages are guarded by default: a key that escapes the storage (`../x`), is
absolute (`/etc/passwd`) or names the storage root itself is refused with
`storages.ErrUnsafeKey`, so keys can be built from untrusted input. Opt out per
backend with `WithUnsafe()`; wrap your own `Storager` with `storages.Guard`.
Runnable example: [`examples/key_guard`](examples/key_guard).

## Backends

### Directory

Local filesystem. `RootPath` is the mount point and must exist;
the optional `Prefix` is the sub-tree the storage is rooted at, and
`WithCreatePrefix()` creates it when it does not exist yet (the root is never
created, so a typo there stays an error):

```go
st, err := directory.NewStorage(directory.Config{
	RootPath: "/var/dumps",
	Prefix:   "project/session",
}, directory.WithCreatePrefix())
```

### S3

Amazon S3 and compatibles (MinIO, Ceph/RGW, Backblaze B2). A bare
`Config` is complete; defaults are filled in. For MinIO and most S3-compatible
stores set `ForcePathStyle: true` explicitly. Runnable example:
[`examples/s3_with_logger`](examples/s3_with_logger).

```go
st, err := s3.NewStorage(ctx, s3.Config{
	Bucket: "my-bucket",
	Region: "us-east-1",
	Prefix: "dumps",
})
```

Server-side encryption applies to every upload: `SSE` is `AES256` (SSE-S3),
`aws:kms` (SSE-KMS) or `aws:kms:dsse` (DSSE-KMS). `KMSKeyARN` and
`BucketKeyEnabled` need a KMS-backed mode; `New` rejects any other combination
rather than dropping the key. Reads need no settings. SSE-C is not supported.

```go
st, err := s3.NewStorage(ctx, s3.Config{
	Bucket:           "my-bucket",
	Region:           "us-east-1",
	SSE:              "aws:kms",
	KMSKeyARN:        "arn:aws:kms:us-east-1:123456789012:key/…",
	BucketKeyEnabled: true,
})
```

### Azure Blob

```go
st, err := azure.NewStorage(ctx, azure.Config{
	Container:      "my-container",
	StorageAccount: "myaccount",
	AccessKey:      os.Getenv("AZURE_STORAGE_KEY"),
})
```

### SSH/SFTP

Holds a real connection, so `Close()` matters. `SubStorage`
clones share the connection; closing any closes all. Operations on a closed
storage return `ssh.ErrStorageClosed`.

```go
st, err := ssh.NewStorage(ssh.Config{
	Host:           "backup.example.com",
	User:           "deploy",
	PrivateKeyPath: "/home/deploy/.ssh/id_ed25519",
	Prefix:         "/srv/dumps",
})
```

### In-memory

A full, conformant backend for tests; no I/O, no services:

```go
st := inmemory.New("")
```

## Writing your own backend

Implement `Storager` and run the shared conformance suite against it
(`storagetest` depends only on the standard library and `storages`):

```go
func TestMyBackend(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		// Fresh, empty, writable — and guarded, the way your constructor should
		// hand one to your users.
		return storages.Guard(mybackend.New(t.TempDir()))
	})
}
```

## Development

```sh
make test              # main module, no Docker needed
make test-race
make lint
make test-integration  # real MinIO/Azurite/OpenSSH in containers; needs Docker
```

Integration tests live in [`tests/integration`](tests/integration), a separate
module, so `testcontainers` stays out of the published dependency graph. In the
main module `testify` is allowed only in `_test.go` files.

## License

Apache 2.0 — see [LICENSE](LICENSE).
