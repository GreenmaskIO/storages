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

A backend is rooted at a "current working directory"; object paths are relative
to it. `SubStorage` returns a `Storager` rooted at a sub-path, so the whole tree
is navigable through the same interface. Also provided: `storages.ErrFileNotFound`
(returned by `GetObject` when the object is absent) and `storages.Walk(ctx, st,
parent)` (recursively lists every file under a storage).

Once constructed, every backend behaves the same:

```go
var st storages.Storager = /* any backend below */

if err := st.Ping(ctx); err != nil {           // health check
	return err
}
if err := st.PutObject(ctx, "reports/2023.txt", strings.NewReader("annual report")); err != nil {
	return err
}
files, err := storages.Walk(ctx, st, "")       // -> ["reports/2023.txt"]
_ = st.Close()                                  // always call it when done
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

## License

Apache 2.0 — see [LICENSE](LICENSE).
