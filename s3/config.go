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
}

// DefaultConfig returns a Config populated with sensible defaults. Set the
// required fields (Bucket, and usually Region) on the result before passing it
// to NewStorage.
func DefaultConfig() Config {
	c := Config{ForcePathStyle: defaultForcePath}
	c.applyDefaults()
	return c
}

// applyDefaults fills unset numeric/string fields with their defaults so a
// Config built as a struct literal behaves like one from DefaultConfig.
// NewStorage calls this, so Config{Bucket: "..."} is a valid, complete config.
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
