# Writing tests

Conventions for **new and modified** tests. Some older tests break them: bare `t.Errorf`/`t.Fatalf`, `mock.Anything` on a request argument, `AssertNotCalled`, a `time.Sleep`. Don't copy that style. When you touch such a test, bring it in line with this guide. [`CLAUDE.md`](../CLAUDE.md#testing) has the short form, and [`CONTRIBUTING.md`](../CONTRIBUTING.md#testing) has what a pull request is held to.

Every example below names something that exists in this repository.

## 1. Where a test goes

There are three layers. Each one proves something the others can't.

| Layer | Where | Runs | Proves |
|---|---|---|---|
| Unit | `<package>/*_test.go`, main module | `make test`, on Linux, macOS and Windows, and with `-race` | Our code, against a test double of the SDK seam or an in-process server |
| Conformance | `storagetest.Run`, called by every backend | `directory` and `inmemory` in `make test`; `s3`, `azure` and `ssh` in `make test-integration` | Each backend keeps the `Storager` contract the same way |
| Integration | `tests/integration/<backend>`, its own module | `make test-integration`, needs Docker | The service behaves the way the backend assumes |

- **A contract rule goes into `storagetest` first.** If a behaviour holds for every backend, the suite checks it, and each backend gets it for free. Don't repeat a conformance case in a backend's own tests.
- **A backend's unit tests cover what is specific to it.** That means request shapes, error-code mapping, pagination, batching and multipart. They run against its seam (`s3API`, `uploaderAPI`, `containerAPI`, `blockBlobAPI`) or against the in-process SFTP server in `ssh/testserver_test.go`. No unit test needs Docker or the network.
- **An integration test exists only for what a double can't show.** Each one names the assumption about the real service it pins (see §9).

## 2. Dependencies: as few as possible

The main module is a library. Everything in its `go.mod` is in every consumer's module graph, and Go has no test-only scope. So:

- **The main module's tests use the standard library, `testify` and nothing else new.** They may also use what production code already requires: `afero`, `pkg/sftp` and `x/crypto/ssh` back the in-process fixtures. Don't add `go-cmp`, `gomock`/`mockery`, `testify/suite`, `goleak`, `dockertest`, fake-S3 servers or assertion DSLs. testify covers them:

  | Need | Use |
  |---|---|
  | Deep equality, unordered slices, error chains | `assert.Equal`, `assert.ElementsMatch`, `assert.ErrorIs`, `require.ErrorAs` |
  | A double for an SDK seam | A hand-written `mock.Mock` in the package's `_test.go` (`mockS3` in `s3/storage_test.go`) |
  | A fake HTTP endpoint | `net/http/httptest` |
  | A fake filesystem | `afero.NewMemMapFs()` |
  | A scratch directory | `t.TempDir()` |

- **testify is imported only from `_test.go` files.** A non-test package that imports it makes testify a build dependency of every consumer.
- **`storagetest` is standard library plus `storages`, and nothing else.** It is a published, non-test package that other people's backends import, so it reports with `t.Errorf`/`t.Fatalf`. This is the one place where bare `t.Errorf` is the rule rather than legacy.
- **`testcontainers` and the raw service SDKs belong to `tests/integration` only.** That is why it is a separate module. A new testcontainers module goes there and nowhere else.

`CLAUDE.md` lists the `go list` checks that hold these rules.

## 3. Table-driven wherever possible

If a behaviour has more than one meaningful input/output pair, write it as a table. Use a slice of anonymous structs and one `t.Run` per case, each with a unique, descriptive lowercase `name`.

`TestStorage_GetObject` in `s3/storage_test.go` is the reference. It has one row per error the service can return, and each row says what that error must map to:

```go
tests := []struct {
	name      string
	prefix    string
	filePath  string
	getErr    error
	wantKey   string
	wantErrIs error
	// ...
}{
	{name: "NoSuchKey maps to ErrFileNotFound", prefix: "dumps/", filePath: "a.txt",
		getErr: &types.NoSuchKey{}, wantKey: "dumps/a.txt", wantErrIs: storages.ErrFileNotFound},
	{name: "NotFound maps to ErrFileNotFound", prefix: "dumps/", filePath: "a.txt",
		getErr: &types.NotFound{}, wantKey: "dumps/a.txt", wantErrIs: storages.ErrFileNotFound},
}
```

When a case needs its own assertions instead of a shared `want`, give the row a `verify func(t *testing.T, ...)` field. A single-behaviour test with one input doesn't need a table, so don't force one.

## 4. testify: `require` vs `assert`

Use **`require`** when a failure has to stop the test because everything after it depends on it: setup, error checks, and length checks before indexing. Use **`assert`** for the values under test, so each one is reported on its own and a single run shows every mismatch.

```go
objects, err := st.List(ctx, "data")
require.NoError(t, err)      // no point continuing
require.Len(t, objects, 2)   // guards objects[0] below

assert.Equal(t, "one.txt", objects[0].Name)
assert.Equal(t, int64(2), objects[0].Size)
```

- Match sentinels with `assert.ErrorIs` and typed errors with `require.ErrorAs`. Only match message text (`assert.ErrorContains`) when no sentinel exists.
- Compare times with `.Equal()`, never `==`. Compare floats with `InDelta` and mixed int types with `EqualValues`.
- Don't hand-roll `if got != want { t.Errorf(...) }` in new code outside `storagetest`.

## 5. Mocks are strict

Each backend declares the narrow SDK interface it depends on (`s3API`, `uploaderAPI`, `containerAPI`, `blockBlobAPI`). Its tests mock that interface with a hand-written `testify/mock` double in the same package. Don't generate mocks.

- **`mock.Anything` is allowed only for `ctx`.** Match every other argument exactly. The SDK's variadic `...func(*s3.Options)` never reaches `Called`.
- **An argument you can't spell out in full gets a `mock.MatchedBy` that does the comparison.** Don't widen it to `mock.Anything`. `TestStorage_Delete_VerifiesBeforeDeleting` does this:

  ```go
  svc.On("HeadObject", mock.Anything, mock.MatchedBy(func(in *s3.HeadObjectInput) bool {
      return aws.ToString(in.Key) == "dumps/ghost.txt"
  })).Return(nil, &types.NotFound{})
  ```

- **Every mock that has expectations ends its test with its own `AssertExpectations(t)`.** Use one line per mock, not `AssertExpectationsForObjects`.
- **To show a method isn't called, set no expectation for it and call `AssertExpectations(t)`.** Don't use `AssertNotCalled`. A mock with no expectation fails loudly on any call, but `AssertNotCalled` still passes when you name the wrong method.
- **Pin call order in the expectations.** Declare the `*Call`s in the order the code makes them and chain them with `mock.InOrder(...)`. For a fan-in, use `.NotBefore(a, b)`. If a method is called twice, give it two `.Once()` expectations so each call has its own place in the chain. Don't record calls in a journal and read it back afterwards.
- Mark an expectation that a case may legitimately skip with `.Maybe()`.
- **Read captured arguments through a helper on the double, not through `mock.Calls` in a test body.** `mockS3.getKeys`, `headKeys` and `deleteBatches` are the pattern. If the SDK mutates an input after the call (ListDir reuses one `ListObjectsInput` across pages), the double takes a snapshot at call time (`mockS3.markers`).
- **Fake an HTTP boundary with `httptest.Server`. Use `mock.Mock` for Go interfaces.**

## 6. Arrange, Act, Assert

Every test body runs Arrange, then Act, then Assert, separated by blank lines. Add `// Arrange`, `// Act` and `// Assert` comments when they help a reader scan. Keep the three apart: no assertions inside Arrange, and no new setup after Act. In a table-driven test, Arrange is split between the row and the `t.Run` body. Act and Assert go in the closure, in that order.

```go
// Arrange: "a.txt" is present, "ghost.txt" is not.
svc := &mockS3{}
// ... HeadObject expectations ...
st := newStorage(t, "dumps/", svc, nil)

// Act
err := st.Delete(t.Context(), "a.txt", "ghost.txt")

// Assert
var missing *storages.MissingObjectsError
require.ErrorAs(t, err, &missing)
assert.Equal(t, []string{"ghost.txt"}, missing.Paths)
assert.Empty(t, svc.deleteBatches(), "a failed verification must delete nothing")
svc.AssertExpectations(t)
```

## 7. Naming

```
Test{ObjectName}_{methodName}[_{caseName}]
```

- `{ObjectName}` is the type under test (`Storage`, `Guard`). For a package-level function, leave it out and name the function: `TestStreamCopy_…`, `TestNewCopyOptions`.
- `{methodName}` is the method or function under test, verbatim, with its original casing (`TestStorage_multipartCopy_Ranges`).
- Add `_{caseName}` **only when the test isn't table-driven** and each scenario has its own function. A table-driven test keeps its cases in the table and stops at the method name.

```go
func TestStorage_GetObject(t *testing.T)                          // table-driven, no suffix
func TestStorage_Delete_VerifiesBeforeDeleting(t *testing.T)      // separate scenario
func TestStreamCopy_MissingSourceIsErrFileNotFound(t *testing.T)  // package-level function
```

## 8. Determinism and portability

- **Don't sleep to synchronise.** Use channels, `sync.WaitGroup` or a hook on the double to make goroutines meet. Build times with fixed `time.Date(...)` values. To prove a concurrency limit, block the copies on a channel and count how many are in flight, rather than relying on a sleep to make them overlap.
- Pass `t.Context()` as the context. It is cancelled when the test ends, so a goroutine left behind can't outlive the test.
- Use `t.TempDir()` for files and `t.Setenv` instead of `os.Setenv`. Mark helpers with `t.Helper()`.
- Silence a logger with `slog.New(slog.DiscardHandler)`, or just pass no `WithLogger`, since a backend with no logger logs nothing.
- **Unit tests run on Windows too.** Build paths with `path` for keys and `filepath` for the host filesystem, and never assume `/`, case sensitivity or Unix permission bits. To check Linux-only on-disk behaviour from macOS, run `make test-linux`.
- Some invariants compile cleanly when they are wrong, and only a test pins them: the guard on every method, nothing deleted when one key is missing, a failed put leaving the old object in place. Those tests are load-bearing. Don't make one pass by relaxing it.

## 9. Integration tests

They live in `tests/integration`, which is its own module so that `testcontainers` stays out of the published graph. `make test-integration` runs them, and so does CI on Ubuntu. Every release tag is gated on them.

- **One package per backend. One shared container per package**, started lazily behind a `sync.Once` and terminated in `TestMain`. Each test gets its own isolated root, such as a fresh prefix or a fresh blob container, from a `newTestStorage`/`mustSub` helper. That way cases never see each other's objects.
- **Every package runs `storagetest.Run`** against the real service (`conformance_test.go`).
- **Every other test pins an assumption about the service** and says which one in a comment. Examples are a range past the end being clamped rather than rejected, the service's `InvalidRange` mapping onto `ErrInvalidRange`, folder markers, the 1000-key list and delete pages, SSE metadata on the stored object, and SFTP `Remove` refusing a directory. If the conformance suite or a unit test already proves it, it doesn't belong here.
- **Check the bytes and the metadata that actually landed** with a raw SDK client that bypasses the backend (`rawClient`, `rawContainerClient`, `rawSFTPClient`). The backend can't vouch for itself.
- **Each package skips under `-short`, and fails, rather than skips, when Docker is unreachable.** The module only runs when someone asks for it, so a silent skip there would be a false green.
- **Wait for readiness on the service's own terms.** Use a wait strategy, or poll a real handshake with a deadline (`waitForSSHHandshake`). Polling an external process this way is the one allowed use of a timer. Don't treat an open port as ready.
- **Pin images to a version tag, never `:latest`.** Otherwise an upstream release turns CI red with no code change.
- The conventions in §3–§8 apply here too.
