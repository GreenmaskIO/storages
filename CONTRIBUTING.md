# Contributing to storages

How we develop storages. A pull request is reviewed against this document.

storages is a library: its exported API is a semver promise, and every module in its
`go.mod` is one its users inherit.

## Workflow

- Branch off `main`: `feat/<subject>`, `fix/<subject>`, `docs/<subject>`.
- One pull request, one change.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):
  `type(scope): summary`, imperative, lowercase. The scope is the package (`s3`, `ssh`,
  `guard`). A breaking change carries `!`.
- A user-visible change adds an entry under `## [Unreleased]` in `CHANGELOG.md`; a
  breaking one starts with **Breaking:**. A tag is released from its changelog section.
- Before you push:

  ```bash
  make lint
  make test
  make test-race
  ```

  `make test-integration` runs the backends against real MinIO, Azurite and OpenSSH in
  Docker. Run it when you touch `s3`, `azure`, `ssh` or `storagetest`.

## Go conventions

Write idiomatic Go: [Effective Go](https://go.dev/doc/effective_go) and
[Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) are the baseline.

### Naming

- A name says what a thing does, to someone reading it for the first time.
- Name functions with a verb (`resolveCopyPartSize`, `copySource`), so a call reads as
  an action.
- Don't stutter: `s3.Storage`, `s3.New`, not `s3.S3Storage`.
- Export only what a user of the library needs. Every exported name is API.

### Errors

- Wrap every returned error with what you were doing, as a verb and a noun:

  ```go
  if err != nil {
      return fmt.Errorf("read SFTP dir %q: %w", dir, err)
  }
  ```

  Not `return err`. The message is lowercase, with no "failed to" or "error", and wraps
  with `%w`.
- A backend maps its SDK's errors onto the sentinels in `errors.go` (`ErrFileNotFound`,
  `ErrInvalidRange`, ...). Callers match them with `errors.Is` or `errors.As`, the same
  way on every backend.
- Handle an error once: return it or log it. A failure during cleanup is logged, not
  returned over the original error.
- Don't panic.

### Context and logging

- `ctx context.Context` is the first parameter of anything that does I/O or blocks, and a
  cancelled call stops and cleans up before it returns.
- A backend logs only through the `*slog.Logger` passed with `WithLogger`; without one it
  logs nothing. Never a global logger, never stdout.

### Simplicity

- Write the simplest code that does the job.
- No speculative code: no unused parameters, no functions only tests call.
- Prefer returning values to mutating arguments.

## Design

- `Storager` (`storager.go`) is the contract, and it behaves the same on every backend.
  A backend that can't do something natively emulates it.
- Changing `Storager` is breaking. It updates `guard.go`, every backend, `storagetest`,
  the README and the changelog in the same pull request.
- Provider-specific features (presigned URLs, ACLs, tags) stay off `Storager`.
- Every backend constructor returns a guarded storage; `WithUnsafe()` is the only way
  out. `guard` writes out every method, so a new one doesn't compile until it is gated.
- Interfaces belong to the consumer: a backend declares the small interface it needs from
  its SDK (`s3API`, `containerAPI`) next to the code using it, and tests mock that.

### Dependencies

Go has no test-only dependency scope, so keep the main module's `go.mod` minimal.

- Add a dependency only when the alternative is a lot of code to maintain.
- `testify` is imported from `_test.go` files only.
- `storagetest` imports only the standard library and `storages`.
- `testcontainers` lives in `tests/integration`, a separate, unpublished module.

These checks must print nothing:

```bash
GOWORK=off go list -deps ./storagetest | grep -E '^[^/]+\.[^/]+/' | grep -v '^github.com/greenmaskio/storages'
GOWORK=off go list -f '{{join .Imports "\n"}}' ./... | grep testify
GOWORK=off go list -deps -test ./... | grep testcontainers
GOWORK=off go list -f '{{join .Imports "\n"}}' . | grep greenmaskio
```

## Testing

The full guide is [`docs/testing.md`](docs/testing.md).

- Every change comes with tests. A bug fix starts with a failing test.
- A rule every backend shares is tested once, in `storagetest`.
- Unit tests need no Docker and no network, and pass on Linux, macOS and Windows. They
  mock the SDK seam or use `httptest`, `afero.NewMemMapFs()` or the in-process SFTP
  server.
- Only the standard library and testify: no other assertion, mock or fake-server library.
- Integration tests in `tests/integration` cover only what a mock can't: how the real
  service behaves. They skip under `-short` and pin image versions.
- Table-driven where a behaviour has several input/output pairs.
- testify: `require` for preconditions, `assert` for the values under test. Mocks are
  strict: `mock.Anything` only for `ctx`, and every mock ends with `AssertExpectations(t)`.
- Arrange, Act, Assert, separated by blank lines.
- Name tests `Test{Type}_{Method}[_{Case}]`.
- No `time.Sleep` for synchronisation.

## Documentation

- Exported identifiers have a short doc comment starting with their name, saying what a
  caller needs: behaviour, what a miss returns, which errors to expect.
- A comment says what the code does, and why only when that isn't obvious.
- A user-facing change updates `README.md` or `docs/` in the same pull request.

## Using AI agents

You may use an AI agent, but you own its output: read every line before you submit it.
Keep comments, commit messages and PR descriptions as short as possible. No AI slop: no
filler, no restating the code in prose, no self-praise, no emoji.
