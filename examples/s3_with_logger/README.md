# s3_with_logger

A runnable example that drives the S3 backend
(`github.com/greenmaskio/storages/s3`) against a local, S3-compatible
[MinIO](https://min.io) server and routes all of its diagnostics through
[zerolog](https://github.com/rs/zerolog).

It does two things at once:

1. **Wires zerolog into the backend.** The backend only accepts an
   `*slog.Logger`; the off-the-shelf
   [`slog-zerolog`](https://github.com/samber/slog-zerolog) adapter bridges
   zerolog to slog in two lines, so there is no custom handler to write:

   ```
   zerolog.Logger  ->  slog-zerolog adapter  ->  *slog.Logger  ->  s3.Storage
   ```

2. **Runs a full end-to-end scenario** so every S3 request, retry and error is
   visible in the log.

## The scenario

`main.go` walks the whole `Storager` lifecycle:

1. Create the storage and `Ping` the bucket.
2. `PutObject` a file at the root (`root.txt`).
3. `PutObject` two files sharing a sub-folder (`reports/2026/q1.txt`,
   `reports/2026/q2.txt`) via a `SubStorage`.
4. `Stat` every object — all three exist.
5. `Delete` the single root file.
6. `DeleteAll` the sub-folder — the file-with-subdirs case, removed recursively.
7. `Stat` again — everything is gone (`Stat` returns an error for a missing key).
8. `Close` the storage.

## Run it

From this directory:

```sh
docker compose up -d   # start MinIO and create the bucket
go run .               # run the scenario
docker compose down -v # stop MinIO and wipe its data
```

`docker compose up -d` starts MinIO and a one-shot init container that creates
the `example-bucket` bucket. The example's defaults match the compose file, so
`go run .` needs no environment variables.

## MinIO console

Open the web UI to watch objects appear and disappear as the scenario runs:

- **URL:** <http://localhost:9001>
- **Username:** `minioadmin`
- **Password:** `minioadmin`

Browse to **Object Browser → `example-bucket`**. Re-run `go run .` and refresh to
see `root.txt` and `reports/2026/…` get created and then deleted.

## Configuration

The example reads these environment variables, falling back to the MinIO
defaults baked into `docker-compose.yml`:

| Variable               | Default                 | Purpose                     |
| ---------------------- | ----------------------- | --------------------------- |
| `S3_ENDPOINT`          | `http://localhost:9000` | S3 API endpoint             |
| `S3_REGION`            | `us-east-1`             | Region                      |
| `S3_BUCKET`            | `example-bucket`        | Bucket name                 |
| `S3_ACCESS_KEY_ID`     | `minioadmin`            | Access key                  |
| `S3_SECRET_ACCESS_KEY` | `minioadmin`            | Secret key                  |

To point it at real AWS (or any other S3-compatible service), set these instead
and skip Docker:

```sh
S3_ENDPOINT=https://s3.us-east-1.amazonaws.com \
S3_BUCKET=my-bucket S3_REGION=us-east-1 \
S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=... \
go run .
```

## Logging notes

- `WithLogger` sets the destination (zerolog, via slog).
- `WithAWSLogLevel` separately opts in to verbose AWS SDK request/retry
  diagnostics via v2's `ClientLogMode` bitmask. Drop it and only the backend's
  own messages are logged.

The two concerns are decoupled: a logger with no `WithAWSLogLevel` still logs
the backend's messages, just without the SDK's request-level detail.
