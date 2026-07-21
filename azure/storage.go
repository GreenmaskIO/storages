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

// Package azure implements the Storager interface on top of Azure Blob Storage.
// The implementation is inspired by wal-g's pkg/storages/azure.
package azure

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/greenmaskio/storages"
)

const azureBlobDelimiter = "/"

// Compile-time check that Storage implements the Storager interface.
var _ storages.Storager = (*Storage)(nil)

// blockBlobAPI is the narrow set of block-blob operations Storage depends on.
// It is declared locally so the object methods can be tested against a fake;
// the concrete *blockblob.Client satisfies it directly.
type blockBlobAPI interface {
	DownloadStream(ctx context.Context, o *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error)
	UploadStream(ctx context.Context, body io.Reader, o *blockblob.UploadStreamOptions) (blockblob.UploadStreamResponse, error)
	Delete(ctx context.Context, o *blob.DeleteOptions) (blob.DeleteResponse, error)
	GetProperties(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error)
}

// containerAPI is the narrow set of container operations Storage depends on. It
// is satisfied by containerClientAdapter (which wraps the SDK's *container.Client)
// and by test fakes. NewBlockBlobClient returns the blockBlobAPI seam rather
// than the concrete SDK type so blob operations are mockable too.
type containerAPI interface {
	NewBlockBlobClient(blobName string) blockBlobAPI
	NewListBlobsHierarchyPager(delimiter string, o *container.ListBlobsHierarchyOptions) *runtime.Pager[container.ListBlobsHierarchyResponse]
	GetProperties(ctx context.Context, o *container.GetPropertiesOptions) (container.GetPropertiesResponse, error)
	Create(ctx context.Context, o *container.CreateOptions) (container.CreateResponse, error)
	NewListBlobsFlatPager(o *container.ListBlobsFlatOptions) *runtime.Pager[container.ListBlobsFlatResponse]
}

// containerClientAdapter adapts the SDK's *container.Client to containerAPI. The
// embedded client supplies every method directly except NewBlockBlobClient,
// whose concrete return type must be narrowed to the blockBlobAPI seam.
type containerClientAdapter struct {
	*container.Client
}

func (a containerClientAdapter) NewBlockBlobClient(blobName string) blockBlobAPI {
	return a.Client.NewBlockBlobClient(blobName)
}

type Storage struct {
	config              Config
	containerClient     containerAPI
	prefix              string
	uploadStreamOptions blockblob.UploadStreamOptions
	logger              *slog.Logger
}

// apiVersionPolicy overrides the x-ms-version header sent to Azure Storage.
// This allows compatibility with Azure environments that don't support the
// latest API version used by the SDK.
type apiVersionPolicy struct {
	apiVersion string
}

func (p *apiVersionPolicy) Do(req *policy.Request) (*http.Response, error) {
	if p.apiVersion != "" {
		req.Raw().Header["x-ms-version"] = []string{p.apiVersion}
	}
	return req.Next()
}

// buildClientOptions creates container.ClientOptions with the configured retry
// timeout and optional API version override.
func buildClientOptions(cfg Config) *container.ClientOptions {
	opts := &container.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Retry: policy.RetryOptions{TryTimeout: time.Minute * time.Duration(cfg.TryTimeout)},
		},
	}
	if cfg.BlobStoreAPIVersion != "" {
		opts.PerCallPolicies = append(
			opts.PerCallPolicies,
			&apiVersionPolicy{apiVersion: cfg.BlobStoreAPIVersion},
		)
	}
	return opts
}

// containerBaseURL builds the container URL shared by all auth paths. If an
// explicit Endpoint is set it is used path-style ({endpoint}/{container}, e.g.
// Azurite / private deployments). Otherwise the subdomain form
// https://{account}.blob.{suffix}/{container} is used, where the suffix is the
// explicit EndpointSuffix if set, else derived from EnvironmentName.
func containerBaseURL(cfg Config) string {
	if cfg.Endpoint != "" {
		return fmt.Sprintf("%s/%s", strings.TrimSuffix(cfg.Endpoint, "/"), cfg.Container)
	}
	suffix := cfg.EndpointSuffix
	if suffix == "" {
		suffix = getStorageEndpointSuffix(cfg.EnvironmentName)
	}
	return fmt.Sprintf("https://%s.blob.%s/%s", cfg.StorageAccount, suffix, cfg.Container)
}

// NewStorage builds an Azure Blob Storage backend from cfg. Pass WithLogger to
// enable diagnostic output; without it the backend does not log at all. When a
// logger is provided and enabled at debug level, the Azure SDK's request/response
// logging is routed into it.
func NewStorage(ctx context.Context, cfg Config, opts ...Option) (*Storage, error) {
	cfg.applyDefaults()
	s := &Storage{config: cfg}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger != nil {
		setupLogging(ctx, s.logger)
	}

	at, sasToken, accessKey := resolveAuth(cfg)

	var containerClient *container.Client
	var err error
	baseURL := containerBaseURL(cfg)

	switch at {
	case authTypeAccessKey:
		var credential *azblob.SharedKeyCredential
		credential, err = azblob.NewSharedKeyCredential(cfg.StorageAccount, accessKey)
		if err != nil {
			return nil, fmt.Errorf("create shared key credentials: %w", err)
		}
		if _, err = url.Parse(baseURL); err != nil {
			return nil, fmt.Errorf("parse service URL: %w", err)
		}
		containerClient, err = container.NewClientWithSharedKeyCredential(baseURL, credential, buildClientOptions(cfg))
	case authTypeSASToken:
		containerURLString := baseURL + sasToken
		if _, err = url.Parse(containerURLString); err != nil {
			return nil, fmt.Errorf("parse service URL with SAS token: %w", err)
		}
		containerClient, err = container.NewClientWithNoCredential(containerURLString, buildClientOptions(cfg))
	default:
		// If no auth method is specified, try the default credential chain
		// (managed identity / AZURE_CLIENT_ID env / CLI).
		var defaultCredential *azidentity.DefaultAzureCredential
		defaultCredential, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("construct the default Azure credential chain: %w", err)
		}
		if _, err = url.Parse(baseURL); err != nil {
			return nil, fmt.Errorf("parse service URL: %w", err)
		}
		containerClient, err = container.NewClient(baseURL, defaultCredential, buildClientOptions(cfg))
	}
	if err != nil {
		return nil, fmt.Errorf("create Azure container client: %w", err)
	}

	s.containerClient = containerClientAdapter{containerClient}
	s.prefix = fixPrefix(cfg.Prefix)
	s.uploadStreamOptions = blockblob.UploadStreamOptions{
		BlockSize:   int64(cfg.BufferSize),
		Concurrency: cfg.MaxBuffers,
	}
	return s, nil
}

// Option configures a Storage.
type Option func(*Storage)

// WithLogger sets the logger for the backend's diagnostic output, including the
// Azure SDK's request/response logging (emitted when the logger is enabled at
// debug level). Without this option the backend does not log at all.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Storage) {
		s.logger = logger
	}
}

func (s *Storage) GetCwd() string {
	return s.prefix
}

func (s *Storage) Dirname() string {
	return filepath.Base(s.prefix)
}

// blobName builds the full blob path for a relative name, trimming any leading
// slash since Azure has no notion of absolute vs relative paths.
func (s *Storage) blobName(name string) string {
	return strings.TrimPrefix(path.Join(s.prefix, name), "/")
}

func (s *Storage) ListDir(ctx context.Context) (files []string, dirs []storages.Storager, err error) {
	pager := s.containerClient.NewListBlobsHierarchyPager(
		azureBlobDelimiter,
		&container.ListBlobsHierarchyOptions{Prefix: &s.prefix},
	)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("error listing azure blobs: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			files = append(files, strings.TrimPrefix(*item.Name, s.prefix))
		}
		for _, prefix := range page.Segment.BlobPrefixes {
			dirs = append(dirs, &Storage{
				config:              s.config,
				containerClient:     s.containerClient,
				prefix:              fixPrefix(*prefix.Name),
				uploadStreamOptions: s.uploadStreamOptions,
				logger:              s.logger,
			})
		}
	}
	return files, dirs, nil
}

func (s *Storage) GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	blobClient := s.containerClient.NewBlockBlobClient(s.blobName(filePath))
	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, storages.ErrFileNotFound
		}
		return nil, fmt.Errorf("error getting object: %w", err)
	}
	return resp.Body, nil
}

func (s *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	blobClient := s.containerClient.NewBlockBlobClient(s.blobName(filePath))
	if _, err := blobClient.UploadStream(ctx, body, &s.uploadStreamOptions); err != nil {
		return fmt.Errorf("azure object uploading error: %w", err)
	}
	return nil
}

func (s *Storage) Delete(ctx context.Context, filePaths ...string) error {
	deleteSnapshots := blob.DeleteSnapshotsOptionTypeInclude
	for _, fp := range filePaths {
		blobClient := s.containerClient.NewBlockBlobClient(s.blobName(fp))
		_, err := blobClient.Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: &deleteSnapshots})
		if err != nil {
			if bloberror.HasCode(err, bloberror.BlobNotFound) {
				continue
			}
			return fmt.Errorf("error deleting object: %w", err)
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

func (s *Storage) Exists(ctx context.Context, fileName string) (bool, error) {
	blobClient := s.containerClient.NewBlockBlobClient(s.blobName(fileName))
	_, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("error getting object info: %w", err)
	}
	return true, nil
}

func (s *Storage) SubStorage(subPath string, relative bool) storages.Storager {
	prefix := subPath
	if relative {
		prefix = fixPrefix(path.Join(s.prefix, prefix))
	}
	return &Storage{
		config:              s.config,
		containerClient:     s.containerClient,
		prefix:              prefix,
		uploadStreamOptions: s.uploadStreamOptions,
		logger:              s.logger,
	}
}

func (s *Storage) Stat(fileName string) (*storages.ObjectStat, error) {
	fullPath := s.blobName(fileName)
	blobClient := s.containerClient.NewBlockBlobClient(fullPath)
	props, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("error getting object info: %w", err)
	}

	return &storages.ObjectStat{
		Name:         fullPath,
		LastModified: *props.LastModified,
		Exist:        true,
	}, nil
}

// Ping checks connectivity to the Azure container by fetching its properties.
func (s *Storage) Ping(ctx context.Context) error {
	if _, err := s.containerClient.GetProperties(ctx, nil); err != nil {
		return fmt.Errorf("error pinging azure container: %w", err)
	}
	return nil
}

// Close is a no-op: the Azure blob client manages its own pooled HTTP
// connections, so there is nothing for the storage to release.
func (s *Storage) Close() error {
	return nil
}

// fixPrefix normalizes a path prefix for Azure: it trims any leading slash
// (Azure has no absolute-vs-relative path distinction, and blob names are
// stored without a leading slash) and ensures a trailing slash so it acts as a
// directory delimiter in listings.
func fixPrefix(prefix string) string {
	prefix = strings.TrimPrefix(prefix, "/")
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix = prefix + "/"
	}
	return prefix
}
