// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command s3_with_logger shows how to drive the S3 backend's logging through
// zerolog and then walks a small end-to-end scenario against the bucket:
//
//	zerolog.Logger  ->  slog-zerolog adapter  ->  *slog.Logger  ->  s3.Storage
//
// The scenario exercises the whole Storager lifecycle so every request/retry is
// visible in the log:
//
//	1. create the storage and Ping the bucket
//	2. PutObject a file at the root
//	3. PutObject two files sharing a sub-folder via SubStorage
//	4. Stat every object (they exist)
//	5. Delete the single root file
//	6. DeleteAll the sub-folder (file plus its "directory")
//	7. Stat again (everything is gone)
//	8. Close the storage
//
// The repository ships a docker-compose.yml with MinIO wired to the defaults
// below, so the quickest way to run it is:
//
//	docker compose up -d
//	go run ./examples/s3_with_logger
//
// or point it at any other S3-compatible endpoint via the S3_* environment
// variables. See this directory's README.md for the MinIO console URL and
// credentials.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"

	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/s3"
)

func main() {
	// 1. Any zerolog.Logger. ConsoleWriter is used for readable output; in
	//    production you would typically log JSON to os.Stdout.
	zl := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	// 2. Wrap it as an slog.Handler with the off-the-shelf adapter. That is the
	//    whole bridge. Level here is the authoritative gate: DebugLevel is what
	//    lets the AWS SDK's debug records through.
	logger := slog.New(slogzerolog.Option{Level: slog.LevelDebug, Logger: &zl}.NewZerologHandler())

	cfg := s3.DefaultConfig()
	cfg.Endpoint = getenv("S3_ENDPOINT", "http://localhost:9000")
	cfg.Region = getenv("S3_REGION", "us-east-1")
	cfg.Bucket = getenv("S3_BUCKET", "example-bucket")
	cfg.AccessKeyId = getenv("S3_ACCESS_KEY_ID", "minioadmin")
	cfg.SecretAccessKey = getenv("S3_SECRET_ACCESS_KEY", "minioadmin")

	ctx := context.Background()

	// 3. WithLogger sets the destination (zerolog, via slog). WithAWSLogLevel
	//    separately opts in to verbose AWS SDK diagnostics via v2's
	//    ClientLogMode bitmask — the two concerns are decoupled. Drop
	//    WithAWSLogLevel and only the backend's own messages are logged.
	storage, err := s3.NewStorage(ctx, cfg,
		s3.WithLogger(logger),
		s3.WithAWSLogLevel(aws.LogRequest|aws.LogRetries),
	)
	if err != nil {
		logger.Error("cannot create s3 storage", "error", err)
		os.Exit(1)
	}

	if err := runScenario(ctx, logger, storage); err != nil {
		logger.Error("scenario failed", "error", err)
		os.Exit(1)
	}
}

// runScenario walks the create → write → stat → delete → stat → close flow
// described in the package comment. Every step logs what it does so the scenario
// doubles as a readable trace of the backend's HTTP traffic.
func runScenario(ctx context.Context, logger *slog.Logger, storage storages.Storager) error {
	// 4. Ping issues a HeadBucket request; whether it succeeds or fails, the
	//    request/retry/error diagnostics travel through zerolog.
	if err := storage.Ping(ctx); err != nil {
		return fmt.Errorf("ping failed (is the bucket reachable and created?): %w", err)
	}
	logger.Info("ping succeeded", "bucket", storage.GetCwd())

	const rootFile = "root.txt"
	const subDir = "reports/2026"

	// 5. Write a single object at the root of the storage.
	logger.Info("creating root file", "path", rootFile)
	if err := storage.PutObject(ctx, rootFile, strings.NewReader("i live at the root")); err != nil {
		return fmt.Errorf("put %s: %w", rootFile, err)
	}

	// 6. Write two objects that share a sub-folder, addressed through a
	//    SubStorage rooted at that folder. Relative=true joins it onto the
	//    parent's current working directory.
	sub := storage.SubStorage(subDir, true)
	for _, name := range []string{"q1.txt", "q2.txt"} {
		logger.Info("creating file via substorage", "subdir", subDir, "file", name)
		if err := sub.PutObject(ctx, name, strings.NewReader("quarterly report "+name)); err != nil {
			return fmt.Errorf("put %s/%s: %w", subDir, name, err)
		}
	}

	// 7. Stat everything before deletion — all three objects exist.
	logger.Info("--- stat before deletion ---")
	statObject(logger, storage, rootFile)
	statObject(logger, sub, "q1.txt")
	statObject(logger, sub, "q2.txt")

	// 8. Delete the single root object. Deleting is by explicit key.
	logger.Info("deleting single file", "path", rootFile)
	if err := storage.Delete(ctx, rootFile); err != nil {
		return fmt.Errorf("delete %s: %w", rootFile, err)
	}

	// 9. Delete the whole sub-folder recursively. DeleteAll walks the prefix and
	//    removes every object beneath it — the "file with subdirs" case.
	logger.Info("deleting folder recursively", "prefix", "reports")
	if err := storage.DeleteAll(ctx, "reports"); err != nil {
		return fmt.Errorf("delete all reports: %w", err)
	}

	// 10. Stat again — every object is now gone. Stat returns an error for a
	//     missing key, which is exactly what we expect here.
	logger.Info("--- stat after deletion ---")
	statObject(logger, storage, rootFile)
	statObject(logger, sub, "q1.txt")
	statObject(logger, sub, "q2.txt")

	// 11. Always Close when done. It is a no-op for S3 (the client pools its own
	//     connections) but the interface contract asks callers to release the
	//     owning root instance regardless of backend.
	logger.Info("closing storage")
	if err := storage.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	logger.Info("scenario complete")
	return nil
}

// statObject logs the outcome of a Stat call without treating a missing object
// as fatal: after deletion we *want* to observe the "gone" state.
func statObject(logger *slog.Logger, st storages.Storager, name string) {
	stat, err := st.Stat(name)
	if err != nil {
		logger.Info("stat: object is gone", "name", name, "error", err)
		return
	}
	logger.Info("stat: object exists",
		"name", stat.Name,
		"last_modified", stat.LastModified,
		"exist", stat.Exist,
	)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
