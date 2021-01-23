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

package storage

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newBigQueryWriteClientHook clientHook

// BigQueryWriteCallOptions contains the retry settings for each method of BigQueryWriteClient.
type BigQueryWriteCallOptions struct {
	CreateWriteStream       []gax.CallOption
	AppendRows              []gax.CallOption
	GetWriteStream          []gax.CallOption
	FinalizeWriteStream     []gax.CallOption
	BatchCommitWriteStreams []gax.CallOption
	FlushRows               []gax.CallOption
}

func defaultBigQueryWriteClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("bigquerystorage.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("bigquerystorage.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://bigquerystorage.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultBigQueryWriteCallOptions() *BigQueryWriteCallOptions {
	return &BigQueryWriteCallOptions{
		CreateWriteStream: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
					codes.ResourceExhausted,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		AppendRows: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.ResourceExhausted,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetWriteStream: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		FinalizeWriteStream: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		BatchCommitWriteStreams: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		FlushRows: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
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

// BigQueryWriteClient is a client for interacting with BigQuery Storage API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type BigQueryWriteClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// The gRPC API client.
	bigQueryWriteClient storagepb.BigQueryWriteClient

	// The call options for this service.
	CallOptions *BigQueryWriteCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewBigQueryWriteClient creates a new big query write client.
//
// BigQuery Write API.
//
// The Write API can be used to write data to BigQuery.
func NewBigQueryWriteClient(ctx context.Context, opts ...option.ClientOption) (*BigQueryWriteClient, error) {
	clientOpts := defaultBigQueryWriteClientOptions()

	if newBigQueryWriteClientHook != nil {
		hookOpts, err := newBigQueryWriteClientHook(ctx, clientHookParams{})
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
	c := &BigQueryWriteClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		CallOptions:      defaultBigQueryWriteCallOptions(),

		bigQueryWriteClient: storagepb.NewBigQueryWriteClient(connPool),
	}
	c.setGoogleClientInfo()

	return c, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *BigQueryWriteClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *BigQueryWriteClient) Close() error {
	return c.connPool.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *BigQueryWriteClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// CreateWriteStream creates a write stream to the given table.
// Additionally, every table has a special COMMITTED stream named ‘_default’
// to which data can be written. This stream doesn’t need to be created using
// CreateWriteStream. It is a stream that can be used simultaneously by any
// number of clients. Data written to this stream is considered committed as
// soon as an acknowledgement is received.
func (c *BigQueryWriteClient) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateWriteStream[0:len(c.CallOptions.CreateWriteStream):len(c.CallOptions.CreateWriteStream)], opts...)
	var resp *storagepb.WriteStream
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.bigQueryWriteClient.CreateWriteStream(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// AppendRows appends data to the given stream.
//
// If offset is specified, the offset is checked against the end of
// stream. The server returns OUT_OF_RANGE in AppendRowsResponse if an
// attempt is made to append to an offset beyond the current end of the stream
// or ALREADY_EXISTS if user provids an offset that has already been
// written to. User can retry with adjusted offset within the same RPC
// stream. If offset is not specified, append happens at the end of the
// stream.
//
// The response contains the offset at which the append happened. Responses
// are received in the same order in which requests are sent. There will be
// one response for each successful request. If the offset is not set in
// response, it means append didn’t happen due to some errors. If one request
// fails, all the subsequent requests will also fail until a success request
// is made again.
//
// If the stream is of PENDING type, data will only be available for read
// operations after the stream is committed.
func (c *BigQueryWriteClient) AppendRows(ctx context.Context, opts ...gax.CallOption) (storagepb.BigQueryWrite_AppendRowsClient, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.AppendRows[0:len(c.CallOptions.AppendRows):len(c.CallOptions.AppendRows)], opts...)
	var resp storagepb.BigQueryWrite_AppendRowsClient
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.bigQueryWriteClient.AppendRows(ctx, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetWriteStream gets a write stream.
func (c *BigQueryWriteClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetWriteStream[0:len(c.CallOptions.GetWriteStream):len(c.CallOptions.GetWriteStream)], opts...)
	var resp *storagepb.WriteStream
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.bigQueryWriteClient.GetWriteStream(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FinalizeWriteStream finalize a write stream so that no new data can be appended to the
// stream. Finalize is not supported on the ‘_default’ stream.
func (c *BigQueryWriteClient) FinalizeWriteStream(ctx context.Context, req *storagepb.FinalizeWriteStreamRequest, opts ...gax.CallOption) (*storagepb.FinalizeWriteStreamResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.FinalizeWriteStream[0:len(c.CallOptions.FinalizeWriteStream):len(c.CallOptions.FinalizeWriteStream)], opts...)
	var resp *storagepb.FinalizeWriteStreamResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.bigQueryWriteClient.FinalizeWriteStream(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// BatchCommitWriteStreams atomically commits a group of PENDING streams that belong to the same
// parent table.
// Streams must be finalized before commit and cannot be committed multiple
// times. Once a stream is committed, data in the stream becomes available
// for read operations.
func (c *BigQueryWriteClient) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.BatchCommitWriteStreams[0:len(c.CallOptions.BatchCommitWriteStreams):len(c.CallOptions.BatchCommitWriteStreams)], opts...)
	var resp *storagepb.BatchCommitWriteStreamsResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.bigQueryWriteClient.BatchCommitWriteStreams(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FlushRows flushes rows to a BUFFERED stream.
// If users are appending rows to BUFFERED stream, flush operation is
// required in order for the rows to become available for reading. A
// Flush operation flushes up to any previously flushed offset in a BUFFERED
// stream, to the offset specified in the request.
// Flush is not supported on the _default stream, since it is not BUFFERED.
func (c *BigQueryWriteClient) FlushRows(ctx context.Context, req *storagepb.FlushRowsRequest, opts ...gax.CallOption) (*storagepb.FlushRowsResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "write_stream", url.QueryEscape(req.GetWriteStream())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.FlushRows[0:len(c.CallOptions.FlushRows):len(c.CallOptions.FlushRows)], opts...)
	var resp *storagepb.FlushRowsResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.bigQueryWriteClient.FlushRows(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
