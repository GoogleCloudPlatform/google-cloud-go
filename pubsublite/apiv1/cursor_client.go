// Copyright 2020 Google LLC
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

package pubsublite

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	"github.com/golang/protobuf/proto"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	pubsublitepb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newCursorClientHook clientHook

// CursorCallOptions contains the retry settings for each method of CursorClient.
type CursorCallOptions struct {
	StreamingCommitCursor []gax.CallOption
	CommitCursor          []gax.CallOption
	ListPartitionCursors  []gax.CallOption
}

func defaultCursorClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("pubsublite.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("pubsublite.mtls.googleapis.com:443"),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCursorCallOptions() *CursorCallOptions {
	return &CursorCallOptions{
		StreamingCommitCursor: []gax.CallOption{},
		CommitCursor: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
					codes.Aborted,
					codes.Internal,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListPartitionCursors: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
					codes.Aborted,
					codes.Internal,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// CursorClient is a client for interacting with .
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type CursorClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// The gRPC API client.
	cursorClient pubsublitepb.CursorServiceClient

	// The call options for this service.
	CallOptions *CursorCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewCursorClient creates a new cursor service client.
//
// The service that a subscriber client application uses to manage committed
// cursors while receiving messsages. A cursor represents a subscriber’s
// progress within a topic partition for a given subscription.
func NewCursorClient(ctx context.Context, opts ...option.ClientOption) (*CursorClient, error) {
	clientOpts := defaultCursorClientOptions()

	if newCursorClientHook != nil {
		hookOpts, err := newCursorClientHook(ctx, clientHookParams{})
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
	c := &CursorClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		CallOptions:      defaultCursorCallOptions(),

		cursorClient: pubsublitepb.NewCursorServiceClient(connPool),
	}
	c.setGoogleClientInfo()

	return c, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *CursorClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *CursorClient) Close() error {
	return c.connPool.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *CursorClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// StreamingCommitCursor establishes a stream with the server for managing committed cursors.
func (c *CursorClient) StreamingCommitCursor(ctx context.Context, opts ...gax.CallOption) (pubsublitepb.CursorService_StreamingCommitCursorClient, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.StreamingCommitCursor[0:len(c.CallOptions.StreamingCommitCursor):len(c.CallOptions.StreamingCommitCursor)], opts...)
	var resp pubsublitepb.CursorService_StreamingCommitCursorClient
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cursorClient.StreamingCommitCursor(ctx, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CommitCursor updates the committed cursor.
func (c *CursorClient) CommitCursor(ctx context.Context, req *pubsublitepb.CommitCursorRequest, opts ...gax.CallOption) (*pubsublitepb.CommitCursorResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CommitCursor[0:len(c.CallOptions.CommitCursor):len(c.CallOptions.CommitCursor)], opts...)
	var resp *pubsublitepb.CommitCursorResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cursorClient.CommitCursor(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListPartitionCursors returns all committed cursor information for a subscription.
func (c *CursorClient) ListPartitionCursors(ctx context.Context, req *pubsublitepb.ListPartitionCursorsRequest, opts ...gax.CallOption) *PartitionCursorIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListPartitionCursors[0:len(c.CallOptions.ListPartitionCursors):len(c.CallOptions.ListPartitionCursors)], opts...)
	it := &PartitionCursorIterator{}
	req = proto.Clone(req).(*pubsublitepb.ListPartitionCursorsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*pubsublitepb.PartitionCursor, string, error) {
		var resp *pubsublitepb.ListPartitionCursorsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.cursorClient.ListPartitionCursors(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetPartitionCursors(), resp.GetNextPageToken(), nil
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

// PartitionCursorIterator manages a stream of *pubsublitepb.PartitionCursor.
type PartitionCursorIterator struct {
	items    []*pubsublitepb.PartitionCursor
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
	InternalFetch func(pageSize int, pageToken string) (results []*pubsublitepb.PartitionCursor, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *PartitionCursorIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *PartitionCursorIterator) Next() (*pubsublitepb.PartitionCursor, error) {
	var item *pubsublitepb.PartitionCursor
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *PartitionCursorIterator) bufLen() int {
	return len(it.items)
}

func (it *PartitionCursorIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
