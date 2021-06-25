// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go_gapic. DO NOT EDIT.

package privatecatalog

import (
	"context"
	"fmt"
	"math"
	"net/url"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	privatecatalogpb "google.golang.org/genproto/googleapis/cloud/privatecatalog/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	SearchCatalogs []gax.CallOption
	SearchProducts []gax.CallOption
	SearchVersions []gax.CallOption
}

func defaultGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("cloudprivatecatalog.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("cloudprivatecatalog.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://cloudprivatecatalog.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		SearchCatalogs: []gax.CallOption{},
		SearchProducts: []gax.CallOption{},
		SearchVersions: []gax.CallOption{},
	}
}

// internalClient is an interface that defines the methods availaible from Cloud Private Catalog API.
type internalClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	SearchCatalogs(context.Context, *privatecatalogpb.SearchCatalogsRequest, ...gax.CallOption) *CatalogIterator
	SearchProducts(context.Context, *privatecatalogpb.SearchProductsRequest, ...gax.CallOption) *ProductIterator
	SearchVersions(context.Context, *privatecatalogpb.SearchVersionsRequest, ...gax.CallOption) *VersionIterator
}

// Client is a client for interacting with Cloud Private Catalog API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// PrivateCatalog allows catalog consumers to retrieve Catalog, Product
// and Version resources under a target resource context.
//
// Catalog is computed based on the Associations linked to the target
// resource and its ancestors. Each association’s
// google.cloud.privatecatalogproducer.v1beta.Catalog is transformed into a
// Catalog. If multiple associations have the same parent
// google.cloud.privatecatalogproducer.v1beta.Catalog, they are
// de-duplicated into one Catalog. Users must have
// cloudprivatecatalog.catalogTargets.get IAM permission on the resource
// context in order to access catalogs. Catalog contains the resource name and
// a subset of data of the original
// google.cloud.privatecatalogproducer.v1beta.Catalog.
//
// Product is child resource of the catalog. A Product contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Product.
//
// Version is child resource of the product. A Version contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Version.
type Client struct {
	// The internal transport-dependent client.
	internalClient internalClient

	// The call options for this service.
	CallOptions *CallOptions
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *Client) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *Client) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *Client) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// SearchCatalogs search Catalog resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *Client) SearchCatalogs(ctx context.Context, req *privatecatalogpb.SearchCatalogsRequest, opts ...gax.CallOption) *CatalogIterator {
	return c.internalClient.SearchCatalogs(ctx, req, opts...)
}

// SearchProducts search Product resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *Client) SearchProducts(ctx context.Context, req *privatecatalogpb.SearchProductsRequest, opts ...gax.CallOption) *ProductIterator {
	return c.internalClient.SearchProducts(ctx, req, opts...)
}

// SearchVersions search Version resources that consumers have access to, within the
// scope of the consumer cloud resource hierarchy context.
func (c *Client) SearchVersions(ctx context.Context, req *privatecatalogpb.SearchVersionsRequest, opts ...gax.CallOption) *VersionIterator {
	return c.internalClient.SearchVersions(ctx, req, opts...)
}

// gRPCClient is a client for interacting with Cloud Private Catalog API over gRPC transport.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type gRPCClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// Points back to the CallOptions field of the containing Client
	CallOptions **CallOptions

	// The gRPC API client.
	client privatecatalogpb.PrivateCatalogClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new private catalog client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// PrivateCatalog allows catalog consumers to retrieve Catalog, Product
// and Version resources under a target resource context.
//
// Catalog is computed based on the Associations linked to the target
// resource and its ancestors. Each association’s
// google.cloud.privatecatalogproducer.v1beta.Catalog is transformed into a
// Catalog. If multiple associations have the same parent
// google.cloud.privatecatalogproducer.v1beta.Catalog, they are
// de-duplicated into one Catalog. Users must have
// cloudprivatecatalog.catalogTargets.get IAM permission on the resource
// context in order to access catalogs. Catalog contains the resource name and
// a subset of data of the original
// google.cloud.privatecatalogproducer.v1beta.Catalog.
//
// Product is child resource of the catalog. A Product contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Product.
//
// Version is child resource of the product. A Version contains the resource
// name and a subset of the data of the original
// google.cloud.privatecatalogproducer.v1beta.Version.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	clientOpts := defaultGRPCClientOptions()
	if newClientHook != nil {
		hookOpts, err := newClientHook(ctx, clientHookParams{})
		if err != nil {
			return nil, err
		}
		clientOpts = append(clientOpts, hookOpts...)
	}

	disableDeadlines, err := checkDisableDeadlines()
	if err != nil {
		return nil, err
	}

	connPool, err := gtransport.DialPool(ctx, append(clientOpts, opts...)...)
	if err != nil {
		return nil, err
	}
	client := Client{CallOptions: defaultCallOptions()}

	c := &gRPCClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		client:           privatecatalogpb.NewPrivateCatalogClient(connPool),
		CallOptions:      &client.CallOptions,
	}
	c.setGoogleClientInfo()

	client.internalClient = c

	return &client, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *gRPCClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *gRPCClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *gRPCClient) Close() error {
	return c.connPool.Close()
}

func (c *gRPCClient) SearchCatalogs(ctx context.Context, req *privatecatalogpb.SearchCatalogsRequest, opts ...gax.CallOption) *CatalogIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).SearchCatalogs[0:len((*c.CallOptions).SearchCatalogs):len((*c.CallOptions).SearchCatalogs)], opts...)
	it := &CatalogIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchCatalogsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Catalog, string, error) {
		var resp *privatecatalogpb.SearchCatalogsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.SearchCatalogs(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetCatalogs(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()
	return it
}

func (c *gRPCClient) SearchProducts(ctx context.Context, req *privatecatalogpb.SearchProductsRequest, opts ...gax.CallOption) *ProductIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).SearchProducts[0:len((*c.CallOptions).SearchProducts):len((*c.CallOptions).SearchProducts)], opts...)
	it := &ProductIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchProductsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Product, string, error) {
		var resp *privatecatalogpb.SearchProductsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.SearchProducts(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetProducts(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()
	return it
}

func (c *gRPCClient) SearchVersions(ctx context.Context, req *privatecatalogpb.SearchVersionsRequest, opts ...gax.CallOption) *VersionIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).SearchVersions[0:len((*c.CallOptions).SearchVersions):len((*c.CallOptions).SearchVersions)], opts...)
	it := &VersionIterator{}
	req = proto.Clone(req).(*privatecatalogpb.SearchVersionsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*privatecatalogpb.Version, string, error) {
		var resp *privatecatalogpb.SearchVersionsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.SearchVersions(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetVersions(), resp.GetNextPageToken(), nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.GetPageSize())
	it.pageInfo.Token = req.GetPageToken()
	return it
}

// CatalogIterator manages a stream of *privatecatalogpb.Catalog.
type CatalogIterator struct {
	items    []*privatecatalogpb.Catalog
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// Response is the raw response for the current page.
	// It must be cast to the RPC response type.
	// Calling Next() or InternalFetch() updates this value.
	Response interface{}

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*privatecatalogpb.Catalog, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *CatalogIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *CatalogIterator) Next() (*privatecatalogpb.Catalog, error) {
	var item *privatecatalogpb.Catalog
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *CatalogIterator) bufLen() int {
	return len(it.items)
}

func (it *CatalogIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// ProductIterator manages a stream of *privatecatalogpb.Product.
type ProductIterator struct {
	items    []*privatecatalogpb.Product
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// Response is the raw response for the current page.
	// It must be cast to the RPC response type.
	// Calling Next() or InternalFetch() updates this value.
	Response interface{}

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*privatecatalogpb.Product, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *ProductIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *ProductIterator) Next() (*privatecatalogpb.Product, error) {
	var item *privatecatalogpb.Product
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ProductIterator) bufLen() int {
	return len(it.items)
}

func (it *ProductIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// VersionIterator manages a stream of *privatecatalogpb.Version.
type VersionIterator struct {
	items    []*privatecatalogpb.Version
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// Response is the raw response for the current page.
	// It must be cast to the RPC response type.
	// Calling Next() or InternalFetch() updates this value.
	Response interface{}

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*privatecatalogpb.Version, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *VersionIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *VersionIterator) Next() (*privatecatalogpb.Version, error) {
	var item *privatecatalogpb.Version
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *VersionIterator) bufLen() int {
	return len(it.items)
}

func (it *VersionIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
