# storages

Pluggable, backend-agnostic object storage for Go. One `Storager` interface,
several interchangeable backends — local directory, Amazon S3, Azure Blob,
SSH/SFTP, and an in-memory one for tests. Write your file-handling code once and
pick where the bytes actually land at construction time.

Extracted from [greenmask](https://github.com/greenmaskio/greenmask).

## Is this for you?

Yes, if you:

- read and write **whole objects/files** (dumps, backups, exports, reports) and
  want one code path regardless of where they live;
- need the **same code to target different backends** per environment or per
  customer — local disk in dev, S3 in prod, in-memory in tests;
- want a **thin abstraction you can read end to end**, not a framework.

Probably not, if you need provider-specific features (presigned URLs, object
tagging, lifecycle rules, blob leases), random-access or streaming *within* a
single object (seeking, range requests, append), or a queue/DB/KV store. This is
plain object storage.

## Install

```sh
go get github.com/greenmaskio/storages
```

Requires Go 1.25+.

## The interface

Every backend implements `storages.Storager`:

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

Three contract details worth knowing up front, because they are uniform across
every backend:

- **`Delete` is object-level and never recursive.** `DeleteAll` is the recursive
  one. A path naming a directory is not an object, so `Delete` reports it as
  missing rather than removing the sub-tree.
- **Deleting something that is not there is an error, not a no-op.** Both
  `Delete` and `DeleteAll` verify their targets first, so a call naming one bad
  path deletes *nothing*. The error is a `*storages.MissingObjectsError` listing
  the offending paths, wrapping `storages.ErrFileNotFound`:

  ```go
  err := st.Delete(ctx, "a.txt", "b.txt", "c.txt")

  errors.Is(err, storages.ErrFileNotFound) // true if any were missing

  var missing *storages.MissingObjectsError
  if errors.As(err, &missing) {
      log.Printf("not found: %v", missing.Paths) // ["b.txt"]; a.txt and c.txt still there
  }
  ```

  The trade-off is that deletion is **not idempotent**: re-running a deletion
  that already succeeded fails the second time. Code that retries or replays
  deletions should check `errors.Is(err, storages.ErrFileNotFound)` and treat it
  as success.
- **`Stat` reports a missing object as `Exist: false` with a nil error**,
  reserving errors for lookups that actually failed.

A backend is rooted at a "current working directory"; object paths are relative
to it. `SubStorage` returns a `Storager` rooted at a sub-path, so the whole tree
is navigable through the same interface. Also provided: `storages.ErrFileNotFound`
(returned by `GetObject` when the object is absent) and `storages.Walk(ctx, st)`
(recursively lists every file under a storage).

Once constructed, every backend behaves the same, so write your code against
`Storager` and let the caller decide where the bytes land:

```go
func report(ctx context.Context, st storages.Storager) ([]string, error) {
	if err := st.Ping(ctx); err != nil { // health check
		return nil, err
	}
	if err := st.PutObject(ctx, "reports/2023.txt", strings.NewReader("annual report")); err != nil {
		return nil, err
	}
	return storages.Walk(ctx, st) // -> ["reports/2023.txt"]
}
```

The caller picks the backend — any of the constructors below, all of which
return a `Storager`:

```go
st, err := directory.NewStorage(directory.Config{Path: "/var/dumps"})
if err != nil {
	return err
}
defer st.Close() // always close when done

files, err := report(ctx, st)
```

## Backends

### Directory

Local filesystem. The obvious default for single-host tools and development.

```go
st, err := directory.NewStorage(directory.Config{Path: "/var/dumps"})
```

### S3

Amazon S3 and any S3-compatible endpoint (MinIO, Ceph/RGW, Backblaze B2). Built
on `aws-sdk-go-v2`. See [`examples/s3_with_logger`](examples/s3_with_logger) for
a runnable end-to-end scenario against MinIO.

```go
st, err := s3.NewStorage(ctx, s3.Config{
	Bucket: "my-bucket",
	Region: "us-east-1",
	Prefix: "dumps",
})
```

`NewStorage` fills in the numeric/string defaults (retries, part size, storage
class), so a bare `Config` is complete. If you'd rather start from the defaults
and tweak, `s3.DefaultConfig()` returns the same values.

One caveat: `ForcePathStyle` is a `bool`, so `NewStorage` can't tell "unset" from
"false" and leaves it as you set it — default `false` (virtual-hosted addressing,
which is what Amazon S3 wants). For MinIO and most S3-compatible stores set
`ForcePathStyle: true` explicitly.

### Azure Blob

Azure Blob Storage, addressed by storage account + container.

```go
st, err := azure.NewStorage(ctx, azure.Config{
	Container:      "my-container",
	StorageAccount: "myaccount",
	AccessKey:      os.Getenv("AZURE_STORAGE_KEY"),
})
```

### SSH / SFTP

Files over SFTP — useful for on-prem or per-customer servers. Holds a real
connection, so `Close()` matters here.

```go
st, err := ssh.NewStorage(ssh.Config{
	Host:           "backup.example.com",
	User:           "deploy",
	PrivateKeyPath: "/home/deploy/.ssh/id_ed25519",
	Prefix:         "/srv/dumps",
})
defer st.Close() // closes the shared SSH connection
```

`SubStorage` clones share the parent's connection, so closing any of them closes
it for all. Operations on a closed storage return `ssh.ErrStorageClosed`, which
you can test for with `errors.Is`.

### In-memory

A full, conformant backend that keeps everything in memory — no I/O, no
services. Use it in tests to exercise real storage semantics.

```go
st := inmemory.New("")
```

There is no factory: construct the backend you want directly and use it through
the `Storager` interface. To pick one from config, a small type switch in your
own code is all it takes. The `directory` and `inmemory` backends share one
implementation, so they behave identically — handy for tests. Object paths use
forward slashes on every OS (Linux, macOS, Windows).

## Writing your own backend

`Storager` is a small interface, and nothing here is closed to extension — a
backend living in your own repository is a first-class one. To check that it
behaves like the built-in backends, run the shared conformance suite against it:

```go
func TestMyBackend(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storages.Storager {
		return mybackend.New(t.TempDir()) // fresh, empty, writable
	})
}
```

`storagetest` imports nothing but the standard library and `storages` itself, so
no test framework is ever compiled into your module or written to your `vendor/`
directory.

## Development

The main module needs no Docker: each backend's unit tests drive its API seam
with test doubles, and the ssh backend's connection tests run against an
in-process SFTP server.

```sh
make test       # the whole main module, everywhere
make test-race  # same, under the race detector
make lint
```

End-to-end tests against real servers — MinIO, Azurite and OpenSSH in
containers — live in [`tests/integration`](tests/integration), a separate module
so that `testcontainers` (and the ~50 modules behind it) stays out of the
published dependency graph. They need Docker:

```sh
make test-integration
```

Within the main module, `testify` is allowed in `_test.go` files and banned
everywhere else. A `_test.go` file is never compiled on a consumer's side, so
the framework costs them nothing there. A non-test file is a different matter:
in `storagetest` it would pull `testify` into every importer's test builds and
`vendor/` directory, and in `internal/fsbackend` it would reach their production
binaries, since that package is compiled into the s3, ssh and directory
backends. `tests/integration` is a separate module and is not bound by this.

## License

Apache 2.0 — see [LICENSE](LICENSE).
