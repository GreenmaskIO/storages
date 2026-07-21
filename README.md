# storages

Pluggable, backend-agnostic object storage for Go. One `Storager` interface,
interchangeable backends: local directory, Amazon S3, Azure Blob, SSH/SFTP, and
in-memory for tests. Plain whole-object storage — no presigned URLs, ranged
reads, or other provider-specific features.

Extracted from [greenmask](https://github.com/greenmaskio/greenmask).

## Install

```sh
go get github.com/greenmaskio/storages
```

Requires Go 1.25+.

## Usage

Write your code against `storages.Storager`; pick the backend at construction:

```go
st, err := directory.NewStorage(directory.Config{Path: "/var/dumps"})
if err != nil {
	return err
}
defer st.Close()

err = st.PutObject(ctx, "reports/2023.txt", strings.NewReader("annual report"))
files, err := storages.Walk(ctx, st) // -> ["reports/2023.txt"]
```

```go
type Storager interface {
	GetCwd() string
	Dirname() string
	ListDir(ctx context.Context) (files []string, dirs []Storager, err error)
	GetObject(ctx context.Context, filePath string) (io.ReadCloser, error)
	PutObject(ctx context.Context, filePath string, body io.Reader) error
	Delete(ctx context.Context, filePaths ...string) error
	DeleteAll(ctx context.Context, pathPrefix string) error
	Exists(ctx context.Context, fileName string) (bool, error)
	SubStorage(subPath string, relative bool) Storager
	Stat(fileName string) (*ObjectStat, error)
	Ping(ctx context.Context) error
	Close() error
}
```

Semantics, uniform across all backends:

- Object paths are relative to the storage root and use forward slashes on
  every OS. `SubStorage` returns a `Storager` rooted at a sub-path.
- `Delete` is object-level and never recursive; `DeleteAll` is the recursive one.
- **Deleting a missing path is an error, not a no-op** — and nothing gets
  deleted. The error is a `*storages.MissingObjectsError` (with the offending
  paths in `.Paths`) wrapping `storages.ErrFileNotFound`. Code that retries
  deletions should treat `errors.Is(err, storages.ErrFileNotFound)` as success.
- `Stat` reports a missing object as `Exist: false` with a nil error.
- `GetObject` returns `storages.ErrFileNotFound` for a missing object.

## Backends

**Directory** — local filesystem:

```go
st, err := directory.NewStorage(directory.Config{Path: "/var/dumps"})
```

**S3** — Amazon S3 and compatibles (MinIO, Ceph/RGW, Backblaze B2). A bare
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

**Azure Blob:**

```go
st, err := azure.NewStorage(ctx, azure.Config{
	Container:      "my-container",
	StorageAccount: "myaccount",
	AccessKey:      os.Getenv("AZURE_STORAGE_KEY"),
})
```

**SSH/SFTP** — holds a real connection, so `Close()` matters. `SubStorage`
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

**In-memory** — a full, conformant backend for tests; no I/O, no services:

```go
st := inmemory.New("")
```

## Writing your own backend

Implement `Storager` and run the shared conformance suite against it
(`storagetest` depends only on the standard library and `storages`):

```go
func TestMyBackend(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		return mybackend.New(t.TempDir()) // fresh, empty, writable
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
