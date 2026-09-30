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
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testKMSKeyARN = "arn:aws:kms:us-east-1:123456789012:key/test-key"

func TestConfig_Validate_SSEModes(t *testing.T) {
	for _, mode := range []string{"", "AES256", "aws:kms", "aws:kms:dsse"} {
		t.Run("accepts "+mode, func(t *testing.T) {
			require.NoError(t, (&Config{SSE: mode}).Validate())
		})
	}

	// aws:fsx is a valid SDK enum value but only applies to FSx-backed access
	// points, never to a bucket upload.
	for _, mode := range []string{"aes256", "AWS:KMS", "kms", "none", "sse-c", "aws:fsx"} {
		t.Run("rejects "+mode, func(t *testing.T) {
			err := (&Config{SSE: mode}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown sse")
		})
	}
}

func TestConfig_Validate_KMSKeyARN(t *testing.T) {
	for _, mode := range []string{"aws:kms", "aws:kms:dsse"} {
		t.Run("allowed with "+mode, func(t *testing.T) {
			require.NoError(t, (&Config{SSE: mode, KMSKeyARN: testKMSKeyARN}).Validate())
		})
	}

	// Silently dropping the key would leave objects encrypted by something other
	// than the key the caller asked for, so it must fail loudly instead.
	for _, mode := range []string{"", "AES256"} {
		t.Run("rejected with sse "+mode, func(t *testing.T) {
			err := (&Config{SSE: mode, KMSKeyARN: testKMSKeyARN}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "kms key arn requires sse")
		})
	}
}

func TestConfig_Validate_BucketKeyEnabled(t *testing.T) {
	for _, mode := range []string{"aws:kms", "aws:kms:dsse"} {
		t.Run("allowed with "+mode, func(t *testing.T) {
			require.NoError(t, (&Config{SSE: mode, BucketKeyEnabled: true}).Validate())
		})
	}

	for _, mode := range []string{"", "AES256"} {
		t.Run("rejected with sse "+mode, func(t *testing.T) {
			err := (&Config{SSE: mode, BucketKeyEnabled: true}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "bucket key enabled requires sse")
		})
	}
}

func TestConfig_Validate_DefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	require.NoError(t, cfg.Validate())
}

// A KMS key with no SSE must never reach the wire on its own: the key header
// alone is meaningless to S3. Validate rejects that config, and the early
// return in applyEncryption keeps it structurally impossible regardless.
func TestConfig_applyEncryption_NoSSEEmitsNothing(t *testing.T) {
	cfg := &Config{KMSKeyARN: testKMSKeyARN, BucketKeyEnabled: true}
	in := &s3.PutObjectInput{}

	cfg.applyEncryption(in)

	assert.Empty(t, in.ServerSideEncryption)
	assert.Nil(t, in.SSEKMSKeyId)
	assert.Nil(t, in.BucketKeyEnabled)
}

func TestNew_RejectsInvalidEncryption(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Bucket = "test-bucket"
	cfg.SSE = "AES256"
	cfg.KMSKeyARN = testKMSKeyARN

	_, err := New(context.Background(), cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "kms key arn requires sse")
}

// multipartClient is a manager.UploadAPIClient that records the
// CreateMultipartUpload input and answers every call with a canned success.
type multipartClient struct {
	mu     sync.Mutex
	create *s3.CreateMultipartUploadInput
	puts   int
}

func (c *multipartClient) PutObject(
	context.Context, *s3.PutObjectInput, ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts++
	return &s3.PutObjectOutput{}, nil
}

func (c *multipartClient) UploadPart(
	context.Context, *s3.UploadPartInput, ...func(*s3.Options),
) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{ETag: aws.String("etag")}, nil
}

func (c *multipartClient) CreateMultipartUpload(
	_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options),
) (*s3.CreateMultipartUploadOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.create = in
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-id")}, nil
}

func (c *multipartClient) CompleteMultipartUpload(
	context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options),
) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (c *multipartClient) AbortMultipartUpload(
	context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options),
) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

// TestStorage_PutObject_EncryptionSurvivesMultipart guards the non-obvious half
// of SSE: an object larger than MaxPartSize goes through CreateMultipartUpload,
// which the uploader derives from the PutObjectInput by reflection. If a future
// SDK bump stopped carrying these headers across, large objects would silently
// lose encryption while the single-part tests kept passing. It drives the real
// manager.Uploader, so it checks what the SDK sends rather than what we build.
func TestStorage_PutObject_EncryptionSurvivesMultipart(t *testing.T) {
	// Arrange
	client := &multipartClient{}
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = manager.MinUploadPartSize
		u.Concurrency = 1
	})
	st := newStorage(t, "dumps/", nil, uploader)
	st.config.SSE = string(types.ServerSideEncryptionAwsKms)
	st.config.KMSKeyARN = testKMSKeyARN
	st.config.BucketKeyEnabled = true
	body := bytes.Repeat([]byte{'x'}, int(manager.MinUploadPartSize)+1)

	// Act
	err := st.PutObject(context.Background(), "big.bin", bytes.NewReader(body))

	// Assert
	require.NoError(t, err)
	require.Zero(t, client.puts, "a body over one part must not go through PutObject")
	require.NotNil(t, client.create, "multipart upload was never initiated")
	assert.Equal(t, types.ServerSideEncryptionAwsKms, client.create.ServerSideEncryption,
		"multipart init lost the sse header")
	assert.Equal(t, testKMSKeyARN, aws.ToString(client.create.SSEKMSKeyId),
		"multipart init lost the kms key")
	assert.True(t, aws.ToBool(client.create.BucketKeyEnabled),
		"multipart init lost the bucket key flag")
}
