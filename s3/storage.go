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
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/storages"
)

// Compile-time check that Storage implements the Storager interface.
var _ storages.Storager = (*Storage)(nil)

const DefaultS3ObjectsDelimiter = "/"

// deleteObjectsBatchSize is the maximum number of objects per DeleteObjects API call.
// The S3 API has a hard limit of 1000 objects per request.
const deleteObjectsBatchSize = 1000

const (
	// maxCopyObjectSize is the largest object a single CopyObject can copy.
	maxCopyObjectSize = 5 << 30
	// copyPartSize is the default UploadPartCopy range; large parts keep the
	// request count low since the bytes never leave S3.
	copyPartSize = 512 << 20
	// maxCopyPartSize is the largest range one UploadPartCopy may cover.
	maxCopyPartSize = 5 << 30
)

const (
	NotFountAwsErrorCode           = "NotFound"
	NoSuchKeyAwsErrorCode          = "NoSuchKey"
	InvalidRangeAwsErrorCode       = "InvalidRange"
	PreconditionFailedAwsErrorCode = "PreconditionFailed"
)

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
	GetObjectTagging(context.Context, *s3.GetObjectTaggingInput, ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPartCopy(context.Context, *s3.UploadPartCopyInput, ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
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
	// unsafe records that the caller asked for the key guard to be left off. It
	// is read once, at the end of New; nothing else consults it, because a
	// sub-storage inherits its safety from the wrapper it was reached through
	// rather than from a flag.
	unsafe bool
}

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

// WithUnsafe turns the key guard off. By default the storage returned by
// New is guarded: a key that reaches outside the configured prefix or
// that names the prefix itself is refused with storages.ErrUnsafeKey before any
// request is sent. Pass this only when every key is known to be trusted and a
// path with legitimate ".." segments has to get through.
func WithUnsafe() Option {
	return func(s *Storage) {
		s.unsafe = true
	}
}

// New builds an S3 backend from cfg, refusing a config that fails
// Config.Validate before any client is created. Pass WithLogger to enable
// diagnostic output; without it the backend does not log at all. Verbose AWS
// SDK request/response logging is off by default and is controlled separately
// via WithAWSLogLevel.
//
// The returned storage is guarded: keys cannot escape cfg.Prefix. See WithUnsafe
// to opt out.
func New(ctx context.Context, cfg Config, opts ...Option) (storages.Storager, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid s3 config: %w", err)
	}
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
	if s.unsafe {
		return s, nil
	}
	return storages.Guard(s), nil
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

func (s *Storage) GetObjectRange(
	ctx context.Context, filePath string, offset, length int64,
) (io.ReadCloser, error) {
	if offset < 0 || length == 0 {
		return nil, storages.ErrInvalidRange
	}

	obj, err := s.service.GetObject(
		ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(path.Join(s.prefix, filePath)),
			Range:  aws.String(byteRangeHeader(offset, length)),
		},
	)
	if err != nil {
		if isNotFound(err) {
			return nil, storages.ErrFileNotFound
		}
		if isInvalidRange(err) {
			return nil, storages.ErrInvalidRange
		}
		return nil, fmt.Errorf("error getting object range: %w", err)
	}
	return obj.Body, nil
}

// byteRangeHeader renders an HTTP Range header value. HTTP ranges are inclusive
// on both ends, so a length of n ends at offset+n-1; a negative length becomes
// the open-ended form, which S3 answers with everything from offset onwards.
func byteRangeHeader(offset, length int64) string {
	// An end past MaxInt64 would wrap; it also cannot be exceeded by any real
	// object, so asking for "to the end" requests exactly the same bytes.
	if length < 0 || offset > math.MaxInt64-length {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

func (s *Storage) List(ctx context.Context, prefix string) ([]storages.ObjectStat, error) {
	// The trailing slash is what keeps the prefix directory-like: without it
	// "data" would also match the sibling "database".
	fullPrefix := fixPrefix(path.Join(s.prefix, prefix))

	res := []storages.ObjectStat{}
	// S3 returns keys in lexicographic order and every key here shares fullPrefix,
	// so trimming it preserves that order — no sorting needed.
	collect := func(contents []types.Object) {
		for _, object := range contents {
			name := strings.TrimPrefix(aws.ToString(object.Key), fullPrefix)
			if name == "" {
				continue
			}
			res = append(res, storages.ObjectStat{
				Name:         name,
				LastModified: aws.ToTime(object.LastModified),
				Exist:        true,
				Size:         aws.ToInt64(object.Size),
				ETag:         aws.ToString(object.ETag),
			})
		}
	}

	// No delimiter: the listing is flat and recursive, which is one paginated
	// request per page instead of one per directory level.
	if s.config.UseListObjectsV1 {
		// v2 ships a paginator only for ListObjectsV2, not the deprecated
		// ListObjects operation, so page the v1 call manually via the marker.
		page := &s3.ListObjectsInput{
			Prefix: aws.String(fullPrefix),
			Bucket: aws.String(s.config.Bucket),
		}
		for {
			out, err := s.service.ListObjects(ctx, page)
			if err != nil {
				return nil, fmt.Errorf("error listing s3 objects v1: %w", err)
			}
			collect(out.Contents)
			if !aws.ToBool(out.IsTruncated) {
				break
			}
			// Without a delimiter S3 does not set NextMarker, so the next page
			// starts after the last key of this one.
			marker := aws.ToString(out.NextMarker)
			if marker == "" && len(out.Contents) > 0 {
				marker = aws.ToString(out.Contents[len(out.Contents)-1].Key)
			}
			if marker == "" {
				break
			}
			page.Marker = aws.String(marker)
		}
		return res, nil
	}

	paginator := s3.NewListObjectsV2Paginator(s.service, &s3.ListObjectsV2Input{
		Prefix: aws.String(fullPrefix),
		Bucket: aws.String(s.config.Bucket),
	})
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error listing s3 objects v2: %w", err)
		}
		collect(out.Contents)
	}
	return res, nil
}

func (s *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	ui := &s3.PutObjectInput{
		Bucket:       aws.String(s.config.Bucket),
		Key:          aws.String(path.Join(s.prefix, filePath)),
		Body:         body,
		StorageClass: types.StorageClass(s.config.StorageClass),
	}

	s.config.encryption().apply(&ui.ServerSideEncryption, &ui.SSEKMSKeyId, &ui.BucketKeyEnabled)

	if _, err := s.uploader.Upload(ctx, ui); err != nil {
		return fmt.Errorf("s3 object uploading error: %w", err)
	}
	return nil
}

// Copy copies srcPath to dstPath server-side: one CopyObject up to 5 GiB, a
// multipart UploadPartCopy above. The copy gets the configured storage class and
// SSE, not the source's; a source overwritten mid-copy fails with ErrSourceChanged.
func (s *Storage) Copy(
	ctx context.Context, srcPath, dstPath string, opts ...storages.CopyOption,
) (storages.CopyStat, error) {
	srcKey := path.Join(s.prefix, srcPath)
	dstKey := path.Join(s.prefix, dstPath)

	head, err := s.service.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(srcKey),
	})
	if err != nil {
		if isNotFound(err) {
			return storages.CopyStat{}, storages.ErrFileNotFound
		}
		return storages.CopyStat{}, fmt.Errorf("error getting object info: %w", err)
	}
	// S3 refuses a copy onto itself that changes nothing.
	if srcKey == dstKey {
		return storages.CopyStat{}, nil
	}
	stat := storages.CopyStat{Bytes: aws.ToInt64(head.ContentLength)}

	if stat.Bytes > maxCopyObjectSize {
		o := storages.NewCopyOptions(opts...)
		if err := s.multipartCopy(ctx, srcKey, dstKey, head, resolveCopyPartSize(o), s.copyConcurrency(o)); err != nil {
			return storages.CopyStat{}, err
		}
		return stat, nil
	}

	return s.copyObject(ctx, srcKey, dstKey, head.ETag, stat.Bytes)
}

// copyObject copies srcKey to dstKey with one CopyObject pinned to etag, failing
// with ErrSourceChanged if the source no longer matches.
func (s *Storage) copyObject(
	ctx context.Context, srcKey, dstKey string, etag *string, size int64,
) (storages.CopyStat, error) {
	in := &s3.CopyObjectInput{
		Bucket:       aws.String(s.config.Bucket),
		CopySource:   aws.String(copySource(s.config.Bucket, srcKey)),
		Key:          aws.String(dstKey),
		StorageClass: types.StorageClass(s.config.StorageClass),
		// Pinned so the reported size is the size copied.
		CopySourceIfMatch: etag,
	}
	s.config.encryption().apply(&in.ServerSideEncryption, &in.SSEKMSKeyId, &in.BucketKeyEnabled)
	if _, err := s.service.CopyObject(ctx, in); err != nil {
		switch {
		case isNotFound(err):
			// The source can vanish after it was inspected.
			return storages.CopyStat{}, storages.ErrFileNotFound
		case isPreconditionFailed(err):
			return storages.CopyStat{}, fmt.Errorf("%w: %w", storages.ErrSourceChanged, err)
		}
		return storages.CopyStat{}, fmt.Errorf("s3 object copying error: %w", err)
	}
	return storages.CopyStat{Bytes: size}, nil
}

// resolveCopyPartSize returns the caller's part size clamped to S3's limits, or
// copyPartSize.
func resolveCopyPartSize(o storages.CopyOptions) int64 {
	if o.PartSize <= 0 {
		return copyPartSize
	}
	return min(max(o.PartSize, manager.MinUploadPartSize), maxCopyPartSize)
}

// copyConcurrency returns the part-copy concurrency: the option, then
// Config.Concurrency, then the SDK upload default.
func (s *Storage) copyConcurrency(o storages.CopyOptions) int {
	switch {
	case o.Concurrency > 0:
		return o.Concurrency
	case s.config.Concurrency > 0:
		return s.config.Concurrency
	default:
		return manager.DefaultUploadConcurrency
	}
}

// multipartCopy copies an object above 5 GiB with UploadPartCopy, carrying over
// head's content headers, metadata and tags as CopyObject would. Failure aborts it.
func (s *Storage) multipartCopy(
	ctx context.Context, srcKey, dstKey string, head *s3.HeadObjectOutput, partSize int64, concurrency int,
) error {
	size := aws.ToInt64(head.ContentLength)
	// S3 caps an upload at 10000 parts.
	partSize = max(partSize, (size+int64(manager.MaxUploadParts)-1)/int64(manager.MaxUploadParts))

	create := &s3.CreateMultipartUploadInput{
		Bucket:             aws.String(s.config.Bucket),
		Key:                aws.String(dstKey),
		StorageClass:       types.StorageClass(s.config.StorageClass),
		CacheControl:       head.CacheControl,
		ContentDisposition: head.ContentDisposition,
		ContentEncoding:    head.ContentEncoding,
		ContentLanguage:    head.ContentLanguage,
		ContentType:        head.ContentType,
		Expires:            head.Expires,
		Metadata:           head.Metadata,
	}
	if aws.ToInt32(head.TagCount) > 0 {
		tagging, err := s.sourceTagging(ctx, srcKey, head.VersionId)
		if err != nil {
			return err
		}
		create.Tagging = aws.String(tagging)
	}
	s.config.encryption().apply(&create.ServerSideEncryption, &create.SSEKMSKeyId, &create.BucketKeyEnabled)
	upload, err := s.service.CreateMultipartUpload(ctx, create)
	if err != nil {
		return fmt.Errorf("error starting multipart copy: %w", err)
	}

	parts, err := s.copyParts(ctx, srcKey, dstKey, upload.UploadId, head.ETag, size, partSize, concurrency)
	if err == nil {
		_, err = s.service.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(s.config.Bucket),
			Key:             aws.String(dstKey),
			UploadId:        upload.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		})
		if err == nil {
			return nil
		}
		err = fmt.Errorf("error completing multipart copy: %w", err)
	}

	// Abort even on cancel: copied parts are billed until the upload is aborted.
	if _, aerr := s.service.AbortMultipartUpload(context.WithoutCancel(ctx), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.config.Bucket),
		Key:      aws.String(dstKey),
		UploadId: upload.UploadId,
	}); aerr != nil {
		s.logger.Warn("error aborting multipart copy", "key", dstKey, "upload_id", aws.ToString(upload.UploadId), "error", aerr)
	}
	switch {
	case isNotFound(err):
		return storages.ErrFileNotFound
	case isPreconditionFailed(err):
		return fmt.Errorf("%w: %w", storages.ErrSourceChanged, err)
	}
	return err
}

// sourceTagging returns the source version's tags as a URL-encoded query string.
func (s *Storage) sourceTagging(ctx context.Context, srcKey string, versionID *string) (string, error) {
	out, err := s.service.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket:    aws.String(s.config.Bucket),
		Key:       aws.String(srcKey),
		VersionId: versionID,
	})
	if err != nil {
		if isNotFound(err) {
			return "", storages.ErrFileNotFound
		}
		return "", fmt.Errorf("error getting object tags: %w", err)
	}
	tags := url.Values{}
	for _, tag := range out.TagSet {
		tags.Set(aws.ToString(tag.Key), aws.ToString(tag.Value))
	}
	// S3 wants %20 for a space; a literal "+" is already %2B.
	return strings.ReplaceAll(tags.Encode(), "+", "%20"), nil
}

// copyParts copies every partSize range of srcKey, each pinned to etag, and
// returns the completed parts in order.
func (s *Storage) copyParts(
	ctx context.Context, srcKey, dstKey string, uploadID, etag *string, size, partSize int64, concurrency int,
) ([]types.CompletedPart, error) {
	count := int((size + partSize - 1) / partSize)
	parts := make([]types.CompletedPart, count)
	source := aws.String(copySource(s.config.Bucket, srcKey))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(concurrency, 1))
	for i := range count {
		g.Go(func() error {
			// Skip parts whose turn comes after a failure rather than send them.
			if err := gctx.Err(); err != nil {
				return err
			}
			number := aws.Int32(int32(i + 1))
			start := int64(i) * partSize
			end := min(start+partSize, size) - 1
			out, err := s.service.UploadPartCopy(gctx, &s3.UploadPartCopyInput{
				Bucket:            aws.String(s.config.Bucket),
				Key:               aws.String(dstKey),
				UploadId:          uploadID,
				PartNumber:        number,
				CopySource:        source,
				CopySourceRange:   aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				CopySourceIfMatch: etag,
			})
			if err != nil {
				return fmt.Errorf("error copying part %d: %w", i+1, err)
			}
			var partETag *string
			if out.CopyPartResult != nil {
				partETag = out.CopyPartResult.ETag
			}
			parts[i] = types.CompletedPart{ETag: partETag, PartNumber: number}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return parts, nil
}

// copySource renders the x-amz-copy-source value, escaping each segment with
// spaces as %20, since S3 reads "+" as a literal plus.
func copySource(bucket, key string) string {
	segments := strings.Split(key, "/")
	for i, seg := range segments {
		segments[i] = strings.ReplaceAll(url.QueryEscape(seg), "+", "%20")
	}
	return bucket + "/" + strings.Join(segments, "/")
}

// Delete removes the named objects. Every key is verified to exist first, so a
// request naming one absent key deletes nothing and reports it.
//
// S3's DeleteObjects succeeds for keys that were never there, so absence cannot
// be learned from the delete itself — this costs one HeadObject per key. Callers
// deleting keys they already listed should not pay for that; DeleteAll uses the
// unchecked deleteKnown path below for exactly that reason.
func (s *Storage) Delete(ctx context.Context, filePaths ...string) error {
	var missing []string
	for _, fp := range filePaths {
		_, err := s.service.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(path.Join(s.prefix, fp)),
		})
		if err != nil {
			if isNotFound(err) {
				missing = append(missing, fp)
				continue
			}
			return fmt.Errorf("error checking object %q: %w", fp, err)
		}
	}
	if len(missing) > 0 {
		return &storages.MissingObjectsError{Paths: missing}
	}
	return s.deleteKnown(ctx, filePaths...)
}

// deleteKnown removes the named objects without checking that they exist. It is
// for callers that just enumerated the keys and would otherwise pay a HeadObject
// per key to re-learn what they already know.
func (s *Storage) deleteKnown(ctx context.Context, filePaths ...string) error {
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

// CopyAll copies every object under srcPrefix to dstPrefix with
// storages.CopyListed, using each object's listed size and ETag in place of a HEAD.
func (s *Storage) CopyAll(
	ctx context.Context, srcPrefix, dstPrefix string, opts ...storages.CopyAllOption,
) (storages.CopyStat, error) {
	return storages.CopyListed(ctx, s, srcPrefix, dstPrefix, s.copyListed, opts...)
}

// copyListed copies a listed object with one CopyObject pinned to its listed
// ETag, falling back to Copy when the listing is stale or not enough.
func (s *Storage) copyListed(
	ctx context.Context, obj storages.ObjectStat, srcPath, dstPath string, opts ...storages.CopyOption,
) (storages.CopyStat, error) {
	srcKey, dstKey := path.Join(s.prefix, srcPath), path.Join(s.prefix, dstPath)
	// Above 5 GiB the multipart copy needs the HEAD's headers.
	if obj.ETag != "" && obj.Size <= maxCopyObjectSize && srcKey != dstKey {
		stat, err := s.copyObject(ctx, srcKey, dstKey, aws.String(obj.ETag), obj.Size)
		if !errors.Is(err, storages.ErrSourceChanged) {
			return stat, err
		}
	}
	return s.Copy(ctx, srcPath, dstPath, opts...)
}

func (s *Storage) DeleteAll(ctx context.Context, pathPrefix string) error {
	pathPrefix = fixPrefix(pathPrefix)
	sub := s.sub(pathPrefix, true)
	filesList, err := storages.Walk(ctx, sub)
	if err != nil {
		return fmt.Errorf("error walking through storage: %w", err)
	}

	// An empty walk means no key carries this prefix, which is the only sense in
	// which a prefix can be "missing" on an object store.
	if len(filesList) == 0 {
		return &storages.MissingObjectsError{Paths: []string{pathPrefix}}
	}

	// The keys came straight from the walk above, so re-checking each one would
	// be a HeadObject per object for nothing.
	if err = sub.deleteKnown(ctx, filesList...); err != nil {
		return fmt.Errorf("error deleting files: %w", err)
	}
	return nil
}

// SubStorage re-roots the backend onto another prefix, which sends no request,
// so the error is always nil here: it exists for the storages that gate the
// path, such as the key guard.
func (s *Storage) SubStorage(subPath string, relative bool) (storages.Storager, error) {
	return s.sub(subPath, relative), nil
}

// sub is SubStorage with the concrete type kept, for the internal callers that
// need the backend's own methods.
func (s *Storage) sub(subPath string, relative bool) *Storage {
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
		// A missing object is reported via Exist rather than as an error, which
		// is the contract every backend implements.
		if isNotFound(err) {
			return &storages.ObjectStat{Name: fullPath, Exist: false}, nil
		}
		return nil, fmt.Errorf("error getting object info: %w", err)
	}

	return &storages.ObjectStat{
		Name:         fullPath,
		LastModified: aws.ToTime(headObjectOutput.LastModified),
		Exist:        true,
		Size:         aws.ToInt64(headObjectOutput.ContentLength),
		ETag:         aws.ToString(headObjectOutput.ETag),
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

// isInvalidRange reports whether err is S3's answer to an unsatisfiable byte
// range. As with isNotFound the typed check comes first, with the HTTP status as
// the fallback for S3-compatible endpoints that phrase the error differently.
func isInvalidRange(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == InvalidRangeAwsErrorCode {
		return true
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable
	}
	return false
}

// isPreconditionFailed reports whether a conditional request failed, by error
// code or, for S3-compatible endpoints, by HTTP 412.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == PreconditionFailedAwsErrorCode {
		return true
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == http.StatusPreconditionFailed
	}
	return false
}

func fixPrefix(prefix string) string {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix = prefix + "/"
	}
	return prefix
}
