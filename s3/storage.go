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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"github.com/greenmaskio/storages"
)

// Compile-time check that Storage implements the Storager interface.
var _ storages.Storager = (*Storage)(nil)

const DefaultS3ObjectsDelimiter = "/"

// deleteObjectsBatchSize is the maximum number of objects per DeleteObjects API call.
// The S3 API has a hard limit of 1000 objects per request.
const deleteObjectsBatchSize = 1000

const (
	NotFountAwsErrorCode  = "NotFound"
	NoSuchKeyAwsErrorCode = "NoSuchKey"
)

// Option configures a Storage.
type Option func(*Storage)

// WithLogger sets the logger used for diagnostic messages emitted by Storage.
//
// Logging is disabled when this option is omitted. Configuring a logger does
// not by itself enable verbose AWS SDK request or response logging; use
// WithAWSLogLevel for that.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Storage) {
		s.logger = logger
	}
}

// WithAWSLogLevel controls how verbose the AWS SDK's own diagnostic logging is
// (request, response, retry and error details) via v2's ClientLogMode bitmask
// (e.g. aws.LogRequest | aws.LogRetries). It defaults to 0 (no SDK logging) and
// has no effect unless a logger is also configured via WithLogger, which is the
// destination those messages are routed to.
func WithAWSLogLevel(level aws.ClientLogMode) Option {
	return func(s *Storage) {
		s.awsLogMode = level
	}
}

// s3API is the narrow set of S3 operations Storage depends on. v2 dropped the
// generated s3iface package, so we declare our own interface for mockability.
// It also satisfies s3.ListObjectsAPIClient / ListObjectsV2APIClient, which the
// paginators require.
type s3API interface {
	ListObjects(context.Context, *s3.ListObjectsInput, ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// uploaderAPI is the multipart-upload surface Storage depends on, satisfied by
// *manager.Uploader. Declared locally since v2 dropped s3manageriface.
type uploaderAPI interface {
	Upload(context.Context, *s3.PutObjectInput, ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

type Storage struct {
	config     Config
	service    s3API
	uploader   uploaderAPI
	prefix     string
	delimiter  string
	logger     *slog.Logger
	awsLogMode aws.ClientLogMode
}

// NewStorage builds an S3 backend from cfg. Pass WithLogger to enable
// diagnostic output; without it the backend does not log at all. Verbose AWS
// SDK request/response logging is off by default and is controlled separately
// via WithAWSLogLevel.
func NewStorage(ctx context.Context, cfg Config, opts ...Option) (*Storage, error) {
	cfg.applyDefaults()
	s := &Storage{config: cfg}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		// Default to a no-op logger so the rest of the code can call s.logger
		// unconditionally without nil checks; it discards everything until the
		// caller supplies one via WithLogger.
		s.logger = slog.New(slog.DiscardHandler)
	}

	var loadOpts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.Region))
	}

	// cfg.MaxRetries maps 1-to-1 onto v2's WithRetryMaxAttempts. Note the
	// semantic shift versus v1: v1's NumMaxRetries counted retries *after* the
	// first try, whereas v2's MaxAttempts is the *total* number of attempts. We
	// deliberately do not compensate with a +1 — the numeric value is preserved.
	loadOpts = append(loadOpts, config.WithRetryMaxAttempts(cfg.MaxRetries))

	if s.awsLogMode != 0 {
		loadOpts = append(loadOpts,
			config.WithClientLogMode(s.awsLogMode),
			config.WithLogger(LogWrapper{logger: s.logger}),
		)
	}

	if cfg.NoVerifySsl {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		loadOpts = append(loadOpts, config.WithHTTPClient(&http.Client{Transport: tr}))
	}

	if cfg.CertFile != "" {
		file, err := os.Open(cfg.CertFile)
		if err != nil {
			return nil, fmt.Errorf("cannot open certFile %q: %w", cfg.CertFile, err)
		}
		loadOpts = append(loadOpts, config.WithCustomCABundle(file))
		// The CA bundle is read during LoadDefaultConfig below, so it is safe to
		// close the file once that has returned.
		defer func() {
			if err := file.Close(); err != nil {
				s.logger.Warn("error closing cert file", "error", err)
			}
		}()
	}

	// Static credentials are only wired in directly when no role is being
	// assumed; with a RoleArn the assume-role provider supersedes them below.
	if cfg.SecretAccessKey != "" && cfg.AccessKeyId != "" && cfg.RoleArn == "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyId, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("cannot load aws config: %w", err)
	}

	if cfg.RoleArn != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		provider := stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleArn, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = cfg.SessionName
		})
		// Wrap in a cache so the role is re-assumed and credentials refreshed as
		// they near expiry, instead of being fetched once at construction.
		awsCfg.Credentials = aws.NewCredentialsCache(provider)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		o.UseAccelerate = cfg.UseAccelerate
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			// S3-compatible endpoints (older MinIO, Backblaze B2, Ceph/RGW) can
			// mishandle the CRC32 request checksums the v2 SDK computes by
			// default, so only send/validate checksums when the operation
			// actually requires them. Against real AWS we keep the SDK defaults.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = cfg.MaxPartSize
		u.Concurrency = cfg.Concurrency
	})

	s.logger.Debug("s3 storage bucket",
		"region", awsCfg.Region,
		"bucket", cfg.Bucket)

	s.prefix = fixPrefix(cfg.Prefix)
	s.service = client
	s.uploader = uploader
	return s, nil
}

func (s *Storage) GetCwd() string {
	return s.prefix
}

func (s *Storage) Dirname() string {
	return filepath.Base(s.prefix)
}

func (s *Storage) ListDir(ctx context.Context) (files []string, dirs []storages.Storager, err error) {

	listFunc := func(commonPrefixes []types.CommonPrefix, contents []types.Object) {
		for _, prefix := range commonPrefixes {

			dirs = append(
				dirs, &Storage{
					config:   s.config,
					service:  s.service,
					uploader: s.uploader,
					prefix:   fixPrefix(aws.ToString(prefix.Prefix)),
					logger:   s.logger,
				},
			)
		}
		for _, object := range contents {
			files = append(files, strings.TrimPrefix(aws.ToString(object.Key), s.prefix))
		}
	}

	prefix := aws.String(s.prefix)
	delimiter := aws.String(DefaultS3ObjectsDelimiter)
	if s.config.UseListObjectsV1 {
		// v2 ships a paginator only for ListObjectsV2, not the deprecated
		// ListObjects operation, so page the v1 call manually via the marker.
		page := &s3.ListObjectsInput{
			Prefix:    prefix,
			Bucket:    aws.String(s.config.Bucket),
			Delimiter: delimiter,
		}
		for {
			out, perr := s.service.ListObjects(ctx, page)
			if perr != nil {
				return nil, nil, fmt.Errorf("error listing s3 objects v1: %w", perr)
			}
			listFunc(out.CommonPrefixes, out.Contents)
			if !aws.ToBool(out.IsTruncated) {
				break
			}
			// NextMarker is only set by S3 when a delimiter is supplied (it is
			// here); fall back to the last returned key otherwise.
			marker := aws.ToString(out.NextMarker)
			if marker == "" && len(out.Contents) > 0 {
				marker = aws.ToString(out.Contents[len(out.Contents)-1].Key)
			}
			if marker == "" {
				break
			}
			page.Marker = aws.String(marker)
		}
	} else {
		page := &s3.ListObjectsV2Input{
			Prefix:    prefix,
			Bucket:    aws.String(s.config.Bucket),
			Delimiter: delimiter,
		}
		paginator := s3.NewListObjectsV2Paginator(s.service, page)
		for paginator.HasMorePages() {
			out, perr := paginator.NextPage(ctx)
			if perr != nil {
				return nil, nil, fmt.Errorf("error listing s3 objects v2: %w", perr)
			}
			listFunc(out.CommonPrefixes, out.Contents)
		}
	}

	return
}

func (s *Storage) GetObject(ctx context.Context, filePath string) (writer io.ReadCloser, err error) {
	obj, err := s.service.GetObject(
		ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(path.Join(s.prefix, filePath)),
		},
	)
	if err != nil {
		if isNotFound(err) {
			return nil, storages.ErrFileNotFound
		}
		return nil, fmt.Errorf("error getting object: %w", err)
	}
	return obj.Body, nil
}

func (s *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	ui := &s3.PutObjectInput{
		Bucket:       aws.String(s.config.Bucket),
		Key:          aws.String(path.Join(s.prefix, filePath)),
		Body:         body,
		StorageClass: types.StorageClass(s.config.StorageClass),
	}

	// TODO: Implement server side encryption
	if _, err := s.uploader.Upload(ctx, ui); err != nil {
		return fmt.Errorf("s3 object uploading error: %w", err)
	}
	return nil
}

func (s *Storage) Delete(ctx context.Context, filePaths ...string) error {
	objs := make([]types.ObjectIdentifier, len(filePaths))
	for idx, fp := range filePaths {
		objs[idx] = types.ObjectIdentifier{
			Key: aws.String(path.Join(s.prefix, fp)),
		}
	}

	for i := 0; i < len(objs); i += deleteObjectsBatchSize {
		end := i + deleteObjectsBatchSize
		if end > len(objs) {
			end = len(objs)
		}
		input := &s3.DeleteObjectsInput{
			Bucket: aws.String(s.config.Bucket),
			Delete: &types.Delete{
				Objects: objs[i:end],
			},
		}
		if _, err := s.service.DeleteObjects(ctx, input); err != nil {
			return fmt.Errorf("error deleting objects: %w", err)
		}
	}
	return nil
}

func (s *Storage) DeleteAll(ctx context.Context, pathPrefix string) error {
	pathPrefix = fixPrefix(pathPrefix)
	ss := s.SubStorage(pathPrefix, true)
	filesList, err := storages.Walk(ctx, ss, "")
	if err != nil {
		return fmt.Errorf("error walking through storage: %w", err)
	}

	if err = ss.Delete(ctx, filesList...); err != nil {
		return fmt.Errorf("error deleting files: %w", err)
	}
	return nil
}

func (s *Storage) SubStorage(subPath string, relative bool) storages.Storager {
	prefix := subPath
	if relative {
		prefix = fixPrefix(path.Join(s.prefix, prefix))
	}
	return &Storage{
		config:    s.config,
		service:   s.service,
		uploader:  s.uploader,
		prefix:    prefix,
		delimiter: s.delimiter,
		logger:    s.logger,
	}
}

func (s *Storage) Exists(ctx context.Context, fileName string) (bool, error) {
	hoi := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(path.Join(s.prefix, fileName)),
	}

	_, err := s.service.HeadObject(ctx, hoi)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("error getting object info: %w", err)
	}
	return true, nil
}

func (s *Storage) Stat(fileName string) (*storages.ObjectStat, error) {
	fullPath := path.Join(s.prefix, fileName)
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullPath),
	}

	// The Storager interface's Stat has no context parameter; v2's HeadObject
	// always takes one, so pass a background context.
	headObjectOutput, err := s.service.HeadObject(context.Background(), headObjectInput)
	if err != nil {
		return nil, fmt.Errorf("error getting object info: %w", err)
	}

	return &storages.ObjectStat{
		Name:         fullPath,
		LastModified: aws.ToTime(headObjectOutput.LastModified),
		Exist:        true,
	}, nil
}

// Ping checks connectivity to the S3 bucket by issuing a HeadBucket request.
func (s *Storage) Ping(ctx context.Context) error {
	_, err := s.service.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.Bucket),
	})
	if err != nil {
		return fmt.Errorf("error pinging s3 bucket: %w", err)
	}
	return nil
}

// Close is a no-op: the S3 client manages its own pooled HTTP connections, so
// there is nothing for the storage to release.
func (s *Storage) Close() error {
	return nil
}

// isNotFound reports whether err represents a missing S3 object. The primary
// check is on v2's typed errors via errors.As; as a fallback for
// S3-compatible endpoints that don't deserialize into those concrete types, it
// compares the smithy APIError's code against the exported not-found codes.
func isNotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == NotFountAwsErrorCode || code == NoSuchKeyAwsErrorCode
	}
	return false
}

func fixPrefix(prefix string) string {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix = prefix + "/"
	}
	return prefix
}
