# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The release workflow extracts the section matching the pushed tag and uses it
as the body of the draft GitHub release, so every released version must have a
`## [x.y.z]` section here before tagging.

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

[0.1.0]: https://github.com/greenmaskio/storages/releases/tag/v0.1.0
