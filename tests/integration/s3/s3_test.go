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
	minioStorage   storages.Storager
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
func requireMinio(t *testing.T) storages.Storager {
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

func startMinio(ctx context.Context) (storages.Storager, *minio.MinioContainer, error) {
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

	st, err := s3storage.New(ctx, cfg, s3storage.WithLogger(slog.New(slog.DiscardHandler)))
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

// mustSub re-roots st at subPath, failing the test if the storage refuses.
func mustSub(t *testing.T, st storages.Storager, subPath string) storages.Storager {
	t.Helper()
	sub, err := st.SubStorage(subPath, true)
	require.NoError(t, err)
	return sub
}

func objectNames(objects []storages.ObjectStat) []string {
	names := make([]string, 0, len(objects))
	for _, o := range objects {
		names = append(names, o.Name)
	}
	return names
}

func objectSizes(objects []storages.ObjectStat) []int64 {
	sizes := make([]int64, 0, len(objects))
	for _, o := range objects {
		sizes = append(sizes, o.Size)
	}
	return sizes
}

func TestStorage_Integration(t *testing.T) {
	ctx := context.Background()
	root := requireMinio(t)

	t.Run("PutObject and GetObject round-trip", func(t *testing.T) {
		// Arrange
		st := mustSub(t, root, "it-roundtrip")
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
		st := mustSub(t, root, "it-missing")

		// Act
		_, err := st.GetObject(ctx, "nope.txt")

		// Assert
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})

	// The conformance suite already runs these cases against MinIO. They are
	// repeated here because two of them rest on assumptions about the service
	// rather than about our code — that a range crossing the end of an object is
	// clamped rather than rejected, and that an unsatisfiable one comes back as
	// an error isInvalidRange recognizes — and a failure should say which
	// assumption broke.
	t.Run("GetObjectRange against a real service", func(t *testing.T) {
		// Arrange
		st := mustSub(t, root, "it-range")
		const content = "0123456789abcdefghij"
		require.NoError(t, st.PutObject(ctx, "ranged.txt", bytes.NewReader([]byte(content))))
		require.NoError(t, st.PutObject(ctx, "empty.txt", bytes.NewReader(nil)))

		read := func(t *testing.T, offset, length int64) string {
			t.Helper()
			r, err := st.GetObjectRange(ctx, "ranged.txt", offset, length)
			require.NoError(t, err)
			defer func() { _ = r.Close() }()
			got, err := io.ReadAll(r)
			require.NoError(t, err)
			return string(got)
		}

		// Act & Assert
		assert.Equal(t, "5678", read(t, 5, 4), "only the requested bytes must come back")
		assert.Equal(t, "fghij", read(t, 15, -1), "a negative length reads to the end")
		assert.Equal(t, "ghij", read(t, 16, 100), "a range crossing the end is clamped, not rejected")

		for _, tt := range []struct {
			name   string
			key    string
			offset int64
		}{
			{name: "offset at size", key: "ranged.txt", offset: int64(len(content))},
			{name: "offset past size", key: "ranged.txt", offset: int64(len(content)) + 100},
			{name: "empty object", key: "empty.txt", offset: 0},
		} {
			t.Run(tt.name, func(t *testing.T) {
				_, err := st.GetObjectRange(ctx, tt.key, tt.offset, 1)
				assert.ErrorIs(t, err, storages.ErrInvalidRange,
					"the service's InvalidRange must map onto the sentinel")
			})
		}

		_, err := st.GetObjectRange(ctx, "missing.txt", 0, 4)
		assert.ErrorIs(t, err, storages.ErrFileNotFound)
	})

	t.Run("List returns the flat tree with sizes", func(t *testing.T) {
		// Arrange
		st := mustSub(t, root, "it-flat-list")
		require.NoError(t, st.PutObject(ctx, "a.txt", bytes.NewReader([]byte("1"))))
		require.NoError(t, st.PutObject(ctx, "data/one.txt", bytes.NewReader([]byte("22"))))
		require.NoError(t, st.PutObject(ctx, "data/sub/two.txt", bytes.NewReader([]byte("333"))))
		require.NoError(t, st.PutObject(ctx, "database/three.txt", bytes.NewReader([]byte("4444"))))

		// Act
		objects, err := st.List(ctx, "")

		// Assert
		require.NoError(t, err)
		assert.Equal(t,
			[]string{"a.txt", "data/one.txt", "data/sub/two.txt", "database/three.txt"},
			objectNames(objects),
			"a delimiter-less listing must return the whole tree in key order")
		assert.Equal(t, []int64{1, 2, 3, 4}, objectSizes(objects))

		// The prefix is directory-like, so "data" must not reach "database".
		nested, err := st.List(ctx, "data")
		require.NoError(t, err)
		assert.Equal(t, []string{"one.txt", "sub/two.txt"}, objectNames(nested))

		empty, err := st.List(ctx, "never_existed")
		require.NoError(t, err)
		assert.Empty(t, empty)
	})

	t.Run("Exists and Stat reflect real objects", func(t *testing.T) {
		// Arrange
		st := mustSub(t, root, "it-stat")
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
		st := mustSub(t, root, "it-list")
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
		st := mustSub(t, root, "it-delete")
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
		st := mustSub(t, root, "it-deleteall")
		require.NoError(t, st.PutObject(ctx, "victims/a.txt", bytes.NewReader([]byte("a"))))
		require.NoError(t, st.PutObject(ctx, "victims/nested/b.txt", bytes.NewReader([]byte("b"))))

		// Act
		require.NoError(t, st.DeleteAll(ctx, "victims"))

		// Assert
		files, err := storages.Walk(ctx, mustSub(t, st, "victims"))
		require.NoError(t, err)
		assert.Empty(t, files)
	})

	t.Run("Ping reaches the bucket", func(t *testing.T) {
		// Act & Assert
		assert.NoError(t, root.Ping(ctx))
	})
}
