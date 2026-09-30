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

package s3

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/storages"
	s3storage "github.com/greenmaskio/storages/s3"
)

// ssePartSize is the smallest part size S3 accepts. Combined with a body a few
// times that size it forces the multipart path without moving much data.
const ssePartSize = 5 * 1024 * 1024

// TestStorage_SSE asserts that the encryption configured on the storage actually
// lands on the stored object, by reading the object metadata back with a raw
// client. The unit tests can only check the request the backend builds.
func TestStorage_SSE(t *testing.T) {
	ctx := context.Background()
	requireMinio(t)

	raw, err := rawClient(ctx, minioConfig.Endpoint, minioConfig.AccessKeyId, minioConfig.SecretAccessKey)
	require.NoError(t, err)

	// newStorage builds a storage against the shared server with the encryption
	// options under test, rooted at its own prefix.
	newStorage := func(t *testing.T, prefix, sse, kmsKey string, bucketKey bool, partSize int64) storages.Storager {
		t.Helper()
		cfg := minioConfig
		cfg.Prefix = prefix
		cfg.SSE = sse
		cfg.KMSKeyARN = kmsKey
		cfg.BucketKeyEnabled = bucketKey
		if partSize > 0 {
			cfg.MaxPartSize = partSize
		}
		st, err := s3storage.New(ctx, cfg, s3storage.WithLogger(slog.New(slog.DiscardHandler)))
		require.NoError(t, err)
		return st
	}

	// putAndHead uploads body, confirms it reads back intact through the backend
	// and returns the stored object's metadata.
	putAndHead := func(t *testing.T, st storages.Storager, key string, body []byte) *awss3.HeadObjectOutput {
		t.Helper()
		require.NoError(t, st.PutObject(ctx, key, bytes.NewReader(body)))

		r, err := st.GetObject(ctx, key)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, body, got, "encryption must not corrupt the round trip")

		out, err := raw.HeadObject(ctx, &awss3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(st.GetCwd() + key),
		})
		require.NoError(t, err)
		return out
	}

	multipartBody := func() []byte {
		body := bytes.Repeat([]byte("storages-sse-multipart-payload!!"), (ssePartSize*2/32)+1)
		require.Greater(t, len(body), ssePartSize, "payload must exceed one part")
		return body
	}

	t.Run("no sse", func(t *testing.T) {
		st := newStorage(t, "sse-none", "", "", false, 0)

		out := putAndHead(t, st, "plain.txt", []byte("no encryption configured"))

		assert.Empty(t, out.ServerSideEncryption)
	})

	t.Run("sse-s3", func(t *testing.T) {
		st := newStorage(t, "sse-s3", "AES256", "", false, 0)

		out := putAndHead(t, st, "aes256.txt", []byte("encrypted with sse-s3"))

		assert.Equal(t, types.ServerSideEncryptionAes256, out.ServerSideEncryption)
	})

	t.Run("sse-kms", func(t *testing.T) {
		st := newStorage(t, "sse-kms", "aws:kms", kmsKeyID, false, 0)

		out := putAndHead(t, st, "kms.txt", []byte("encrypted with sse-kms"))

		assert.Equal(t, types.ServerSideEncryptionAwsKms, out.ServerSideEncryption)
		assert.Contains(t, aws.ToString(out.SSEKMSKeyId), kmsKeyID)
	})

	// An object larger than MaxPartSize goes through CreateMultipartUpload, and
	// losing the headers there would leave big objects unencrypted while small
	// ones looked fine.
	t.Run("sse-s3 multipart", func(t *testing.T) {
		st := newStorage(t, "sse-s3-multipart", "AES256", "", false, ssePartSize)
		body := multipartBody()

		out := putAndHead(t, st, "multipart.bin", body)

		assert.Equal(t, types.ServerSideEncryptionAes256, out.ServerSideEncryption)
		assert.Equal(t, int64(len(body)), aws.ToInt64(out.ContentLength))
	})

	t.Run("sse-kms multipart with bucket key", func(t *testing.T) {
		st := newStorage(t, "sse-kms-multipart", "aws:kms", kmsKeyID, true, ssePartSize)

		out := putAndHead(t, st, "kms-multipart.bin", multipartBody())

		assert.Equal(t, types.ServerSideEncryptionAwsKms, out.ServerSideEncryption)
		assert.Contains(t, aws.ToString(out.SSEKMSKeyId), kmsKeyID)
	})
}
