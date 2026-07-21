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

// Package s3 exercises the s3 backend end to end against a real MinIO server in
// a container.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/greenmaskio/storages"
	s3storage "github.com/greenmaskio/storages/s3"
)

const bucket = "test-bucket"

var (
	minioOnce      sync.Once
	minioStorage   *s3storage.Storage
	minioContainer *minio.MinioContainer
	minioErr       error
)

// TestMain terminates the shared MinIO container (if one was started) after the
// whole package's tests have run.
func TestMain(m *testing.M) {
	code := m.Run()
	if minioContainer != nil {
		_ = minioContainer.Terminate(context.Background())
	}
	os.Exit(code)
}

// requireMinio lazily starts a single MinIO container shared by all tests here
// and returns a Storage rooted at its bucket. Container tests are skipped under
// -short.
func requireMinio(t *testing.T) *s3storage.Storage {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping MinIO container test in short mode")
	}
	minioOnce.Do(func() {
		minioStorage, minioContainer, minioErr = startMinio(context.Background())
	})
	require.NoError(t, minioErr)
	return minioStorage
}

func startMinio(ctx context.Context) (*s3storage.Storage, *minio.MinioContainer, error) {
	container, err := minio.Run(ctx, "minio/minio:latest")
	if err != nil {
		return nil, nil, fmt.Errorf("starting minio: %w", err)
	}

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, container, fmt.Errorf("minio endpoint: %w", err)
	}
	endpointURL := "http://" + endpoint

	// The bucket has to exist before any object operations. The backend exposes
	// no bucket-management surface, so provision it with an independent client
	// built from the same endpoint and credentials.
	if err := createBucket(ctx, endpointURL, container.Username, container.Password); err != nil {
		return nil, container, err
	}

	cfg := s3storage.DefaultConfig()
	cfg.Bucket = bucket
	cfg.Region = "us-east-1"
	cfg.Endpoint = endpointURL
	cfg.AccessKeyId = container.Username
	cfg.SecretAccessKey = container.Password
	cfg.ForcePathStyle = true
	cfg.NoVerifySsl = true

	st, err := s3storage.NewStorage(ctx, cfg, s3storage.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		return nil, container, fmt.Errorf("new storage: %w", err)
	}
	return st, container, nil
}

func createBucket(ctx context.Context, endpoint, accessKey, secretKey string) error {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}

	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

func TestStorage_Integration(t *testing.T) {
	ctx := context.Background()
	root := requireMinio(t)

	t.Run("PutObject and GetObject round-trip", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-roundtrip", true)
		content := []byte("hello world")

		// Act
		require.NoError(t, st.PutObject(ctx, "file.txt", bytes.NewReader(content)))
		reader, err := st.GetObject(ctx, "file.txt")
		require.NoError(t, err)
		defer reader.Close()
		got, err := io.ReadAll(reader)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, content, got)
	})

	t.Run("GetObject on missing key returns ErrFileNotFound", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-missing", true)

		// Act
		_, err := st.GetObject(ctx, "nope.txt")

		// Assert
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})

	t.Run("Exists and Stat reflect real objects", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-stat", true)
		require.NoError(t, st.PutObject(ctx, "stat.txt", bytes.NewReader([]byte("body"))))

		// Act
		exists, err := st.Exists(ctx, "stat.txt")
		require.NoError(t, err)
		stat, statErr := st.Stat("stat.txt")
		missing, missErr := st.Exists(ctx, "absent.txt")

		// Assert
		assert.True(t, exists)
		require.NoError(t, statErr)
		assert.True(t, stat.Exist)
		assert.Contains(t, stat.Name, "stat.txt")
		require.NoError(t, missErr)
		assert.False(t, missing)
	})

	t.Run("ListDir separates files and sub-directories", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-list", true)
		require.NoError(t, st.PutObject(ctx, "root1.txt", bytes.NewReader([]byte("1"))))
		require.NoError(t, st.PutObject(ctx, "root2.txt", bytes.NewReader([]byte("2"))))
		require.NoError(t, st.PutObject(ctx, "d1/inner1.txt", bytes.NewReader([]byte("3"))))
		require.NoError(t, st.PutObject(ctx, "d1/inner2.txt", bytes.NewReader([]byte("4"))))

		// Act
		files, dirs, err := st.ListDir(ctx)

		// Assert
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"root1.txt", "root2.txt"}, files)
		require.Len(t, dirs, 1)

		subFiles, subDirs, err := dirs[0].ListDir(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"inner1.txt", "inner2.txt"}, subFiles)
		assert.Empty(t, subDirs)
	})

	t.Run("Delete removes a single object", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-delete", true)
		require.NoError(t, st.PutObject(ctx, "bye.txt", bytes.NewReader([]byte("bye"))))

		// Act
		require.NoError(t, st.Delete(ctx, "bye.txt"))

		// Assert
		exists, err := st.Exists(ctx, "bye.txt")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("DeleteAll recursively clears a sub-tree", func(t *testing.T) {
		// Arrange
		st := root.SubStorage("it-deleteall", true)
		require.NoError(t, st.PutObject(ctx, "victims/a.txt", bytes.NewReader([]byte("a"))))
		require.NoError(t, st.PutObject(ctx, "victims/nested/b.txt", bytes.NewReader([]byte("b"))))

		// Act
		require.NoError(t, st.DeleteAll(ctx, "victims"))

		// Assert
		files, err := storages.Walk(ctx, st.SubStorage("victims", true))
		require.NoError(t, err)
		assert.Empty(t, files)
	})

	t.Run("Ping reaches the bucket", func(t *testing.T) {
		// Act & Assert
		assert.NoError(t, root.Ping(ctx))
	})
}
