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

package firestore

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	firestorepb "google.golang.org/genproto/googleapis/firestore/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	GetDocument       []gax.CallOption
	ListDocuments     []gax.CallOption
	UpdateDocument    []gax.CallOption
	DeleteDocument    []gax.CallOption
	BatchGetDocuments []gax.CallOption
	BeginTransaction  []gax.CallOption
	Commit            []gax.CallOption
	Rollback          []gax.CallOption
	RunQuery          []gax.CallOption
	PartitionQuery    []gax.CallOption
	Write             []gax.CallOption
	Listen            []gax.CallOption
	ListCollectionIds []gax.CallOption
	BatchWrite        []gax.CallOption
	CreateDocument    []gax.CallOption
}

func defaultGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("firestore.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("firestore.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://firestore.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		internaloption.EnableJwtWithScope(),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		GetDocument: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListDocuments: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		UpdateDocument: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteDocument: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		BatchGetDocuments: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		BeginTransaction: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		Commit: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		Rollback: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		RunQuery: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		PartitionQuery: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		Write: []gax.CallOption{},
		Listen: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListCollectionIds: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Internal,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		BatchWrite: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
					codes.Aborted,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CreateDocument: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.ResourceExhausted,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// internalClient is an interface that defines the methods availaible from Cloud Firestore API.
type internalClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	GetDocument(context.Context, *firestorepb.GetDocumentRequest, ...gax.CallOption) (*firestorepb.Document, error)
	ListDocuments(context.Context, *firestorepb.ListDocumentsRequest, ...gax.CallOption) *DocumentIterator
	UpdateDocument(context.Context, *firestorepb.UpdateDocumentRequest, ...gax.CallOption) (*firestorepb.Document, error)
	DeleteDocument(context.Context, *firestorepb.DeleteDocumentRequest, ...gax.CallOption) error
	BatchGetDocuments(context.Context, *firestorepb.BatchGetDocumentsRequest, ...gax.CallOption) (firestorepb.Firestore_BatchGetDocumentsClient, error)
	BeginTransaction(context.Context, *firestorepb.BeginTransactionRequest, ...gax.CallOption) (*firestorepb.BeginTransactionResponse, error)
	Commit(context.Context, *firestorepb.CommitRequest, ...gax.CallOption) (*firestorepb.CommitResponse, error)
	Rollback(context.Context, *firestorepb.RollbackRequest, ...gax.CallOption) error
	RunQuery(context.Context, *firestorepb.RunQueryRequest, ...gax.CallOption) (firestorepb.Firestore_RunQueryClient, error)
	PartitionQuery(context.Context, *firestorepb.PartitionQueryRequest, ...gax.CallOption) *CursorIterator
	Write(context.Context, ...gax.CallOption) (firestorepb.Firestore_WriteClient, error)
	Listen(context.Context, ...gax.CallOption) (firestorepb.Firestore_ListenClient, error)
	ListCollectionIds(context.Context, *firestorepb.ListCollectionIdsRequest, ...gax.CallOption) *StringIterator
	BatchWrite(context.Context, *firestorepb.BatchWriteRequest, ...gax.CallOption) (*firestorepb.BatchWriteResponse, error)
	CreateDocument(context.Context, *firestorepb.CreateDocumentRequest, ...gax.CallOption) (*firestorepb.Document, error)
}

// Client is a client for interacting with Cloud Firestore API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// The Cloud Firestore service.
//
// Cloud Firestore is a fast, fully managed, serverless, cloud-native NoSQL
// document database that simplifies storing, syncing, and querying data for
// your mobile, web, and IoT apps at global scale. Its client libraries provide
// live synchronization and offline support, while its security features and
// integrations with Firebase and Google Cloud Platform (GCP) accelerate
// building truly serverless apps.
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

// GetDocument gets a single document.
func (c *Client) GetDocument(ctx context.Context, req *firestorepb.GetDocumentRequest, opts ...gax.CallOption) (*firestorepb.Document, error) {
	return c.internalClient.GetDocument(ctx, req, opts...)
}

// ListDocuments lists documents.
func (c *Client) ListDocuments(ctx context.Context, req *firestorepb.ListDocumentsRequest, opts ...gax.CallOption) *DocumentIterator {
	return c.internalClient.ListDocuments(ctx, req, opts...)
}

// UpdateDocument updates or inserts a document.
func (c *Client) UpdateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest, opts ...gax.CallOption) (*firestorepb.Document, error) {
	return c.internalClient.UpdateDocument(ctx, req, opts...)
}

// DeleteDocument deletes a document.
func (c *Client) DeleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest, opts ...gax.CallOption) error {
	return c.internalClient.DeleteDocument(ctx, req, opts...)
}

// BatchGetDocuments gets multiple documents.
//
// Documents returned by this method are not guaranteed to be returned in the
// same order that they were requested.
func (c *Client) BatchGetDocuments(ctx context.Context, req *firestorepb.BatchGetDocumentsRequest, opts ...gax.CallOption) (firestorepb.Firestore_BatchGetDocumentsClient, error) {
	return c.internalClient.BatchGetDocuments(ctx, req, opts...)
}

// BeginTransaction starts a new transaction.
func (c *Client) BeginTransaction(ctx context.Context, req *firestorepb.BeginTransactionRequest, opts ...gax.CallOption) (*firestorepb.BeginTransactionResponse, error) {
	return c.internalClient.BeginTransaction(ctx, req, opts...)
}

// Commit commits a transaction, while optionally updating documents.
func (c *Client) Commit(ctx context.Context, req *firestorepb.CommitRequest, opts ...gax.CallOption) (*firestorepb.CommitResponse, error) {
	return c.internalClient.Commit(ctx, req, opts...)
}

// Rollback rolls back a transaction.
func (c *Client) Rollback(ctx context.Context, req *firestorepb.RollbackRequest, opts ...gax.CallOption) error {
	return c.internalClient.Rollback(ctx, req, opts...)
}

// RunQuery runs a query.
func (c *Client) RunQuery(ctx context.Context, req *firestorepb.RunQueryRequest, opts ...gax.CallOption) (firestorepb.Firestore_RunQueryClient, error) {
	return c.internalClient.RunQuery(ctx, req, opts...)
}

// PartitionQuery partitions a query by returning partition cursors that can be used to run
// the query in parallel. The returned partition cursors are split points that
// can be used by RunQuery as starting/end points for the query results.
func (c *Client) PartitionQuery(ctx context.Context, req *firestorepb.PartitionQueryRequest, opts ...gax.CallOption) *CursorIterator {
	return c.internalClient.PartitionQuery(ctx, req, opts...)
}

// Write streams batches of document updates and deletes, in order.
func (c *Client) Write(ctx context.Context, opts ...gax.CallOption) (firestorepb.Firestore_WriteClient, error) {
	return c.internalClient.Write(ctx, opts...)
}

// Listen listens to changes.
func (c *Client) Listen(ctx context.Context, opts ...gax.CallOption) (firestorepb.Firestore_ListenClient, error) {
	return c.internalClient.Listen(ctx, opts...)
}

// ListCollectionIds lists all the collection IDs underneath a document.
func (c *Client) ListCollectionIds(ctx context.Context, req *firestorepb.ListCollectionIdsRequest, opts ...gax.CallOption) *StringIterator {
	return c.internalClient.ListCollectionIds(ctx, req, opts...)
}

// BatchWrite applies a batch of write operations.
//
// The BatchWrite method does not apply the write operations atomically
// and can apply them out of order. Method does not allow more than one write
// per document. Each write succeeds or fails independently. See the
// BatchWriteResponse for the success status of each write.
//
// If you require an atomically applied set of writes, use
// Commit instead.
func (c *Client) BatchWrite(ctx context.Context, req *firestorepb.BatchWriteRequest, opts ...gax.CallOption) (*firestorepb.BatchWriteResponse, error) {
	return c.internalClient.BatchWrite(ctx, req, opts...)
}

// CreateDocument creates a new document.
func (c *Client) CreateDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest, opts ...gax.CallOption) (*firestorepb.Document, error) {
	return c.internalClient.CreateDocument(ctx, req, opts...)
}

// gRPCClient is a client for interacting with Cloud Firestore API over gRPC transport.
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
	client firestorepb.FirestoreClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new firestore client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// The Cloud Firestore service.
//
// Cloud Firestore is a fast, fully managed, serverless, cloud-native NoSQL
// document database that simplifies storing, syncing, and querying data for
// your mobile, web, and IoT apps at global scale. Its client libraries provide
// live synchronization and offline support, while its security features and
// integrations with Firebase and Google Cloud Platform (GCP) accelerate
// building truly serverless apps.
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
		client:           firestorepb.NewFirestoreClient(connPool),
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

func (c *gRPCClient) GetDocument(ctx context.Context, req *firestorepb.GetDocumentRequest, opts ...gax.CallOption) (*firestorepb.Document, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetDocument[0:len((*c.CallOptions).GetDocument):len((*c.CallOptions).GetDocument)], opts...)
	var resp *firestorepb.Document
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetDocument(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) ListDocuments(ctx context.Context, req *firestorepb.ListDocumentsRequest, opts ...gax.CallOption) *DocumentIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "parent", url.QueryEscape(req.GetParent()), "collection_id", url.QueryEscape(req.GetCollectionId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListDocuments[0:len((*c.CallOptions).ListDocuments):len((*c.CallOptions).ListDocuments)], opts...)
	it := &DocumentIterator{}
	req = proto.Clone(req).(*firestorepb.ListDocumentsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*firestorepb.Document, string, error) {
		var resp *firestorepb.ListDocumentsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListDocuments(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetDocuments(), resp.GetNextPageToken(), nil
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

func (c *gRPCClient) UpdateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest, opts ...gax.CallOption) (*firestorepb.Document, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "document.name", url.QueryEscape(req.GetDocument().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).UpdateDocument[0:len((*c.CallOptions).UpdateDocument):len((*c.CallOptions).UpdateDocument)], opts...)
	var resp *firestorepb.Document
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateDocument(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) DeleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).DeleteDocument[0:len((*c.CallOptions).DeleteDocument):len((*c.CallOptions).DeleteDocument)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteDocument(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

func (c *gRPCClient) BatchGetDocuments(ctx context.Context, req *firestorepb.BatchGetDocumentsRequest, opts ...gax.CallOption) (firestorepb.Firestore_BatchGetDocumentsClient, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "database", url.QueryEscape(req.GetDatabase())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	var resp firestorepb.Firestore_BatchGetDocumentsClient
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.BatchGetDocuments(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) BeginTransaction(ctx context.Context, req *firestorepb.BeginTransactionRequest, opts ...gax.CallOption) (*firestorepb.BeginTransactionResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "database", url.QueryEscape(req.GetDatabase())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).BeginTransaction[0:len((*c.CallOptions).BeginTransaction):len((*c.CallOptions).BeginTransaction)], opts...)
	var resp *firestorepb.BeginTransactionResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.BeginTransaction(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) Commit(ctx context.Context, req *firestorepb.CommitRequest, opts ...gax.CallOption) (*firestorepb.CommitResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "database", url.QueryEscape(req.GetDatabase())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).Commit[0:len((*c.CallOptions).Commit):len((*c.CallOptions).Commit)], opts...)
	var resp *firestorepb.CommitResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.Commit(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) Rollback(ctx context.Context, req *firestorepb.RollbackRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "database", url.QueryEscape(req.GetDatabase())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).Rollback[0:len((*c.CallOptions).Rollback):len((*c.CallOptions).Rollback)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.Rollback(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

func (c *gRPCClient) RunQuery(ctx context.Context, req *firestorepb.RunQueryRequest, opts ...gax.CallOption) (firestorepb.Firestore_RunQueryClient, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	var resp firestorepb.Firestore_RunQueryClient
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.RunQuery(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) PartitionQuery(ctx context.Context, req *firestorepb.PartitionQueryRequest, opts ...gax.CallOption) *CursorIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).PartitionQuery[0:len((*c.CallOptions).PartitionQuery):len((*c.CallOptions).PartitionQuery)], opts...)
	it := &CursorIterator{}
	req = proto.Clone(req).(*firestorepb.PartitionQueryRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*firestorepb.Cursor, string, error) {
		var resp *firestorepb.PartitionQueryResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.PartitionQuery(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetPartitions(), resp.GetNextPageToken(), nil
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

func (c *gRPCClient) Write(ctx context.Context, opts ...gax.CallOption) (firestorepb.Firestore_WriteClient, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	var resp firestorepb.Firestore_WriteClient
	opts = append((*c.CallOptions).Write[0:len((*c.CallOptions).Write):len((*c.CallOptions).Write)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.Write(ctx, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) Listen(ctx context.Context, opts ...gax.CallOption) (firestorepb.Firestore_ListenClient, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	var resp firestorepb.Firestore_ListenClient
	opts = append((*c.CallOptions).Listen[0:len((*c.CallOptions).Listen):len((*c.CallOptions).Listen)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.Listen(ctx, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) ListCollectionIds(ctx context.Context, req *firestorepb.ListCollectionIdsRequest, opts ...gax.CallOption) *StringIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListCollectionIds[0:len((*c.CallOptions).ListCollectionIds):len((*c.CallOptions).ListCollectionIds)], opts...)
	it := &StringIterator{}
	req = proto.Clone(req).(*firestorepb.ListCollectionIdsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]string, string, error) {
		var resp *firestorepb.ListCollectionIdsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListCollectionIds(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetCollectionIds(), resp.GetNextPageToken(), nil
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

func (c *gRPCClient) BatchWrite(ctx context.Context, req *firestorepb.BatchWriteRequest, opts ...gax.CallOption) (*firestorepb.BatchWriteResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "database", url.QueryEscape(req.GetDatabase())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).BatchWrite[0:len((*c.CallOptions).BatchWrite):len((*c.CallOptions).BatchWrite)], opts...)
	var resp *firestorepb.BatchWriteResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.BatchWrite(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) CreateDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest, opts ...gax.CallOption) (*firestorepb.Document, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "parent", url.QueryEscape(req.GetParent()), "collection_id", url.QueryEscape(req.GetCollectionId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).CreateDocument[0:len((*c.CallOptions).CreateDocument):len((*c.CallOptions).CreateDocument)], opts...)
	var resp *firestorepb.Document
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateDocument(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CursorIterator manages a stream of *firestorepb.Cursor.
type CursorIterator struct {
	items    []*firestorepb.Cursor
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
	InternalFetch func(pageSize int, pageToken string) (results []*firestorepb.Cursor, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *CursorIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *CursorIterator) Next() (*firestorepb.Cursor, error) {
	var item *firestorepb.Cursor
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *CursorIterator) bufLen() int {
	return len(it.items)
}

func (it *CursorIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// DocumentIterator manages a stream of *firestorepb.Document.
type DocumentIterator struct {
	items    []*firestorepb.Document
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
	InternalFetch func(pageSize int, pageToken string) (results []*firestorepb.Document, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *DocumentIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *DocumentIterator) Next() (*firestorepb.Document, error) {
	var item *firestorepb.Document
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *DocumentIterator) bufLen() int {
	return len(it.items)
}

func (it *DocumentIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// StringIterator manages a stream of string.
type StringIterator struct {
	items    []string
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
	InternalFetch func(pageSize int, pageToken string) (results []string, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *StringIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *StringIterator) Next() (string, error) {
	var item string
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *StringIterator) bufLen() int {
	return len(it.items)
}

func (it *StringIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
