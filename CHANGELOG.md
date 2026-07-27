# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The release workflow extracts the section matching the pushed tag and uses it
as the body of the draft GitHub release, so every released version must have a
`## [x.y.z]` section here before tagging.

## [0.3.0] - 2026-07-27

### Added

- `directory.Config.Prefix` — an optional relative sub-path inside `RootPath`
  that the storage is rooted at, bringing the directory backend in line with s3
  (`Bucket` + `Prefix`) and ssh (host + `Prefix`). It is slash-separated on every
  OS and the key guard keeps keys inside it; a prefix that is absolute or reaches
  outside the root with `..` is refused.
- `directory.WithCreatePrefix()` — creates a missing `Prefix`, intermediate
  directories included, with mode `0750`. Without it a missing prefix is the new
  `directory.ErrPrefixNotExists`. `RootPath` is never created: an absent root
  stays an error, so a typo there cannot quietly produce a fresh empty tree.

### Changed

- **Breaking, `directory.Config`**: `Path` is replaced by `RootPath` (plus the
  new optional `Prefix`). `directory.Config{Path: p}` becomes
  `directory.Config{RootPath: p}`; the sentinel `ErrPathIsRequired` is renamed
  `ErrRootPathIsRequired`.
- `directory.NewStorage` now goes through `cfg.Validate()` rather than repeating
  the existence check inline, so both report the same thing.

## [0.2.0] - 2026-07-23

### Added

- `Storager.GetObjectRange(ctx, filePath, offset, length)` — reads a byte range
  of an object, transferring only the requested bytes: an HTTP `Range` request
  on s3 and azure, an offset read over SFTP, a section read on the filesystem
  backends. A negative `length` means "to the end of the object" and a range
  running past the end is clamped, while a range that can yield nothing —
  offset at or past the object's size, `length == 0`, negative offset — is the
  new `storages.ErrInvalidRange` sentinel (the storage-level equivalent of HTTP
  416). Azure reports an offset exactly at the end as an empty success rather
  than `InvalidRange`; the backend normalizes that onto the sentinel.
- `Storager.List(ctx, prefix)` — flat, recursive listing with sizes, in one
  paginated request per page on the object stores instead of one per directory
  level. Names are relative to the prefix, slash-separated and sorted
  lexicographically. The prefix is directory-like, so `data` never matches
  `database`, and — unlike `DeleteAll` — a prefix holding nothing is an empty
  slice with a nil error.
- `ObjectStat.Size` — the object's size in bytes, filled in by `Stat` on every
  backend as well as by `List`.
- **Key guard, on by default.** Every backend constructor now returns a storage
  that refuses a key which does not name something strictly inside it: one that
  climbs out with `..` (checked after cleaning, so an interior `a/../b` still
  passes), one that is absolute, or — on the object methods — one that names the
  storage root, which is what made `DeleteAll("")` remove the storage itself.
  The new `storages.ErrUnsafeKey` sentinel reports it, before the backend
  resolves the key. `SubStorage` and `ListDir` return guarded storages too, so
  navigating down cannot navigate out: a relative `SubStorage` path that escapes
  comes back as an error instead of a storage. Keys assembled from untrusted
  input are therefore safe to pass.
- `storages.Guard(st)` — the gate as a decorator over any `Storager`, which is
  what the constructors apply and what a backend implemented outside this module
  can apply to itself.
- `WithUnsafe()` on every backend (`directory`, `inmemory`, `s3`, `azure`,
  `ssh`) — opts out of the guard, handing back the bare backend for callers
  whose keys are all trusted or whose paths carry legitimate `..` segments.
- Conformance cases in `storagetest` for all of the above, so third-party
  backends are held to the same semantics.

### Changed

- **Breaking for implementers of `Storager`** (not for callers): the two new
  methods must be implemented. Every backend in this module already is.
- **Breaking, `SubStorage` signature**: it now returns `(Storager, error)`.
  Re-rooting itself reaches no backend and never fails, so the error is how a
  storage refuses the path — the guarded storage returns `ErrUnsafeKey` for a
  relative sub-path that walks out of it, which is the one case where no storage
  can be handed back at all.
- **Breaking for callers**: the backend constructors now return
  `storages.Storager` rather than their own `*Storage`, since the guard is a
  wrapper around the backend. Code that stores the result in a `storages.Storager`
  — the intended use — is unaffected; code naming the concrete type must switch
  to the interface (or pass `WithUnsafe()`, which hands back the bare backend).
  `inmemory.New` also takes options now.
- **Breaking for callers**, from the guard: keys that used to be accepted and
  quietly reinterpreted are now refused. An absolute key was never an escape —
  every backend resolved it inside the storage, and `azure` trimmed the leading
  slash so `/f.txt` and `f.txt` named the same blob — but it is a bug often
  enough that it is now `ErrUnsafeKey`. Likewise `DeleteAll("")` (and, on
  `azure`, `DeleteAll("/")`), which emptied the storage and, on the filesystem
  backends, removed its root directory. Both remain available via `WithUnsafe()`.
- `storagetest.Run` requires the storage its factory returns to be guarded,
  which is what the backend constructors produce. A bare backend passed to it
  fails the new `Guard*` cases; wrap it in `storages.Guard`.

## [0.1.0] - 2026-07-21

Initial release, extracted from [greenmask](https://github.com/greenmaskio/greenmask).

### Added

- `storages.Storager` — a single backend-agnostic interface for whole-object
  storage: object CRUD (`GetObject`, `PutObject`, `Delete`, `DeleteAll`,
  `Exists`, `Stat`), hierarchical navigation (`ListDir`, `SubStorage`), and
  lifecycle (`Ping`, `Close`). Object paths use forward slashes on every OS.
- Backends implementing it:
  - `directory` — local filesystem;
  - `s3` — Amazon S3 and S3-compatible stores (MinIO, Ceph/RGW, Backblaze B2),
    built on `aws-sdk-go-v2`;
  - `azure` — Azure Blob Storage;
  - `ssh` — files over SFTP, with a lazily-established connection shared by
    `SubStorage` clones and `ssh.ErrStorageClosed` for use-after-close;
  - `inmemory` — a fully conformant in-memory backend for tests.
- Uniform error semantics across all backends: `storages.ErrFileNotFound`
  sentinel; `*storages.MissingObjectsError` from `Delete`/`DeleteAll` listing
  the offending paths, with nothing deleted when any path is missing; `Exists`
  and `Stat` report a missing object as a value with a nil error.
- `storages.Walk` — recursively lists every file under a storage.
- `storagetest` — a shared conformance suite (standard library only) for
  validating third-party backends against the `Storager` contract.

[0.2.0]: https://github.com/greenmaskio/storages/releases/tag/v0.2.0
[0.1.0]: https://github.com/greenmaskio/storages/releases/tag/v0.1.0
