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
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	defaultMaxRetries   = 3
	defaultMaxPartSize  = 50 * 1024 * 1024
	defaultStorageClass = "STANDARD"
	defaultForcePath    = true
)

type Config struct {
	Endpoint         string
	Bucket           string
	Prefix           string
	Region           string
	StorageClass     string
	DisableSSL       bool
	AccessKeyId      string
	SecretAccessKey  string
	SessionToken     string
	RoleArn          string
	SessionName      string
	MaxRetries       int
	CertFile         string
	MaxPartSize      int64
	Concurrency      int
	UseListObjectsV1 bool
	ForcePathStyle   bool
	UseAccelerate    bool
	NoVerifySsl      bool
	// SSE is the server-side encryption mode applied to every uploaded object:
	// "AES256" (SSE-S3), "aws:kms" (SSE-KMS) or "aws:kms:dsse" (DSSE-KMS). Empty
	// leaves the bucket's default encryption in charge. SSE-C is not supported.
	SSE string
	// KMSKeyARN is the KMS key to encrypt with. It requires a KMS-backed SSE;
	// left empty with one, S3 uses the account's AWS managed key.
	KMSKeyARN string
	// BucketKeyEnabled turns on S3 Bucket Keys, which cuts KMS request cost on
	// large uploads. It requires a KMS-backed SSE.
	BucketKeyEnabled bool
}

// DefaultConfig returns a Config populated with sensible defaults. Set the
// required fields (Bucket, and usually Region) on the result before passing it
// to New.
func DefaultConfig() Config {
	c := Config{ForcePathStyle: defaultForcePath}
	c.applyDefaults()
	return c
}

// applyDefaults fills unset numeric/string fields with their defaults so a
// Config built as a struct literal behaves like one from DefaultConfig.
// New calls this, so Config{Bucket: "..."} is a valid, complete config.
//
// Bool fields cannot be defaulted here: a false zero value is indistinguishable
// from an explicit false, so ForcePathStyle's default (true) is applied only by
// DefaultConfig. A struct literal leaves it false (virtual-hosted addressing);
// set ForcePathStyle: true explicitly for MinIO and most S3-compatible stores.
func (c *Config) applyDefaults() {
	if c.StorageClass == "" {
		c.StorageClass = defaultStorageClass
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.MaxPartSize == 0 {
		c.MaxPartSize = defaultMaxPartSize
	}
}

// sseIsKMS reports whether the configured SSE mode is one of the KMS-backed
// modes, which are the only ones accepting KMSKeyARN and BucketKeyEnabled.
func (c *Config) sseIsKMS() bool {
	sse := types.ServerSideEncryption(c.SSE)
	return sse == types.ServerSideEncryptionAwsKms || sse == types.ServerSideEncryptionAwsKmsDsse
}

// Validate rejects an incoherent encryption setup up front instead of letting it
// fail on the first upload. Nothing is dropped silently: a KMS key with a
// non-KMS mode would leave objects encrypted by something other than the key
// that was asked for, so it is an error rather than a no-op.
func (c *Config) Validate() error {
	switch types.ServerSideEncryption(c.SSE) {
	case "", types.ServerSideEncryptionAes256, types.ServerSideEncryptionAwsKms, types.ServerSideEncryptionAwsKmsDsse:
	default:
		return fmt.Errorf(
			"unknown sse %q: must be one of %s, %s, %s", c.SSE,
			types.ServerSideEncryptionAes256, types.ServerSideEncryptionAwsKms, types.ServerSideEncryptionAwsKmsDsse,
		)
	}

	if c.sseIsKMS() {
		return nil
	}
	if c.KMSKeyARN != "" {
		return fmt.Errorf(
			"kms key arn requires sse to be %s or %s",
			types.ServerSideEncryptionAwsKms, types.ServerSideEncryptionAwsKmsDsse,
		)
	}
	if c.BucketKeyEnabled {
		return fmt.Errorf(
			"bucket key enabled requires sse to be %s or %s",
			types.ServerSideEncryptionAwsKms, types.ServerSideEncryptionAwsKmsDsse,
		)
	}
	return nil
}

// applyEncryption sets the server-side encryption headers on an upload.
// Validate guarantees the KMS-only options are set only with a KMS-backed mode.
func (c *Config) applyEncryption(in *s3.PutObjectInput) {
	if c.SSE == "" {
		return
	}
	in.ServerSideEncryption = types.ServerSideEncryption(c.SSE)
	if c.KMSKeyARN != "" {
		in.SSEKMSKeyId = aws.String(c.KMSKeyARN)
	}
	if c.BucketKeyEnabled {
		in.BucketKeyEnabled = aws.Bool(true)
	}
}
