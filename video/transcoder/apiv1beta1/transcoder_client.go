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

package transcoder

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
	transcoderpb "google.golang.org/genproto/googleapis/cloud/video/transcoder/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	CreateJob         []gax.CallOption
	ListJobs          []gax.CallOption
	GetJob            []gax.CallOption
	DeleteJob         []gax.CallOption
	CreateJobTemplate []gax.CallOption
	ListJobTemplates  []gax.CallOption
	GetJobTemplate    []gax.CallOption
	DeleteJobTemplate []gax.CallOption
}

func defaultGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("transcoder.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("transcoder.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://transcoder.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		CreateJob:         []gax.CallOption{},
		ListJobs:          []gax.CallOption{},
		GetJob:            []gax.CallOption{},
		DeleteJob:         []gax.CallOption{},
		CreateJobTemplate: []gax.CallOption{},
		ListJobTemplates:  []gax.CallOption{},
		GetJobTemplate:    []gax.CallOption{},
		DeleteJobTemplate: []gax.CallOption{},
	}
}

// internalClient is an interface that defines the methods availaible from Transcoder API.
type internalClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	CreateJob(context.Context, *transcoderpb.CreateJobRequest, ...gax.CallOption) (*transcoderpb.Job, error)
	ListJobs(context.Context, *transcoderpb.ListJobsRequest, ...gax.CallOption) *JobIterator
	GetJob(context.Context, *transcoderpb.GetJobRequest, ...gax.CallOption) (*transcoderpb.Job, error)
	DeleteJob(context.Context, *transcoderpb.DeleteJobRequest, ...gax.CallOption) error
	CreateJobTemplate(context.Context, *transcoderpb.CreateJobTemplateRequest, ...gax.CallOption) (*transcoderpb.JobTemplate, error)
	ListJobTemplates(context.Context, *transcoderpb.ListJobTemplatesRequest, ...gax.CallOption) *JobTemplateIterator
	GetJobTemplate(context.Context, *transcoderpb.GetJobTemplateRequest, ...gax.CallOption) (*transcoderpb.JobTemplate, error)
	DeleteJobTemplate(context.Context, *transcoderpb.DeleteJobTemplateRequest, ...gax.CallOption) error
}

// Client is a client for interacting with Transcoder API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// Using the Transcoder API, you can queue asynchronous jobs for transcoding
// media into various output formats. Output formats may include different
// streaming standards such as HTTP Live Streaming (HLS) and Dynamic Adaptive
// Streaming over HTTP (DASH). You can also customize jobs using advanced
// features such as Digital Rights Management (DRM), audio equalization, content
// concatenation, and digital ad-stitch ready content generation.
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

// CreateJob creates a job in the specified region.
func (c *Client) CreateJob(ctx context.Context, req *transcoderpb.CreateJobRequest, opts ...gax.CallOption) (*transcoderpb.Job, error) {
	return c.internalClient.CreateJob(ctx, req, opts...)
}

// ListJobs lists jobs in the specified region.
func (c *Client) ListJobs(ctx context.Context, req *transcoderpb.ListJobsRequest, opts ...gax.CallOption) *JobIterator {
	return c.internalClient.ListJobs(ctx, req, opts...)
}

// GetJob returns the job data.
func (c *Client) GetJob(ctx context.Context, req *transcoderpb.GetJobRequest, opts ...gax.CallOption) (*transcoderpb.Job, error) {
	return c.internalClient.GetJob(ctx, req, opts...)
}

// DeleteJob deletes a job.
func (c *Client) DeleteJob(ctx context.Context, req *transcoderpb.DeleteJobRequest, opts ...gax.CallOption) error {
	return c.internalClient.DeleteJob(ctx, req, opts...)
}

// CreateJobTemplate creates a job template in the specified region.
func (c *Client) CreateJobTemplate(ctx context.Context, req *transcoderpb.CreateJobTemplateRequest, opts ...gax.CallOption) (*transcoderpb.JobTemplate, error) {
	return c.internalClient.CreateJobTemplate(ctx, req, opts...)
}

// ListJobTemplates lists job templates in the specified region.
func (c *Client) ListJobTemplates(ctx context.Context, req *transcoderpb.ListJobTemplatesRequest, opts ...gax.CallOption) *JobTemplateIterator {
	return c.internalClient.ListJobTemplates(ctx, req, opts...)
}

// GetJobTemplate returns the job template data.
func (c *Client) GetJobTemplate(ctx context.Context, req *transcoderpb.GetJobTemplateRequest, opts ...gax.CallOption) (*transcoderpb.JobTemplate, error) {
	return c.internalClient.GetJobTemplate(ctx, req, opts...)
}

// DeleteJobTemplate deletes a job template.
func (c *Client) DeleteJobTemplate(ctx context.Context, req *transcoderpb.DeleteJobTemplateRequest, opts ...gax.CallOption) error {
	return c.internalClient.DeleteJobTemplate(ctx, req, opts...)
}

// gRPCClient is a client for interacting with Transcoder API over gRPC transport.
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
	client transcoderpb.TranscoderServiceClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new transcoder service client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// Using the Transcoder API, you can queue asynchronous jobs for transcoding
// media into various output formats. Output formats may include different
// streaming standards such as HTTP Live Streaming (HLS) and Dynamic Adaptive
// Streaming over HTTP (DASH). You can also customize jobs using advanced
// features such as Digital Rights Management (DRM), audio equalization, content
// concatenation, and digital ad-stitch ready content generation.
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
		client:           transcoderpb.NewTranscoderServiceClient(connPool),
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

func (c *gRPCClient) CreateJob(ctx context.Context, req *transcoderpb.CreateJobRequest, opts ...gax.CallOption) (*transcoderpb.Job, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).CreateJob[0:len((*c.CallOptions).CreateJob):len((*c.CallOptions).CreateJob)], opts...)
	var resp *transcoderpb.Job
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) ListJobs(ctx context.Context, req *transcoderpb.ListJobsRequest, opts ...gax.CallOption) *JobIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListJobs[0:len((*c.CallOptions).ListJobs):len((*c.CallOptions).ListJobs)], opts...)
	it := &JobIterator{}
	req = proto.Clone(req).(*transcoderpb.ListJobsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*transcoderpb.Job, string, error) {
		var resp *transcoderpb.ListJobsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListJobs(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetJobs(), resp.GetNextPageToken(), nil
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

func (c *gRPCClient) GetJob(ctx context.Context, req *transcoderpb.GetJobRequest, opts ...gax.CallOption) (*transcoderpb.Job, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetJob[0:len((*c.CallOptions).GetJob):len((*c.CallOptions).GetJob)], opts...)
	var resp *transcoderpb.Job
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) DeleteJob(ctx context.Context, req *transcoderpb.DeleteJobRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).DeleteJob[0:len((*c.CallOptions).DeleteJob):len((*c.CallOptions).DeleteJob)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

func (c *gRPCClient) CreateJobTemplate(ctx context.Context, req *transcoderpb.CreateJobTemplateRequest, opts ...gax.CallOption) (*transcoderpb.JobTemplate, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).CreateJobTemplate[0:len((*c.CallOptions).CreateJobTemplate):len((*c.CallOptions).CreateJobTemplate)], opts...)
	var resp *transcoderpb.JobTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateJobTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) ListJobTemplates(ctx context.Context, req *transcoderpb.ListJobTemplatesRequest, opts ...gax.CallOption) *JobTemplateIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListJobTemplates[0:len((*c.CallOptions).ListJobTemplates):len((*c.CallOptions).ListJobTemplates)], opts...)
	it := &JobTemplateIterator{}
	req = proto.Clone(req).(*transcoderpb.ListJobTemplatesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*transcoderpb.JobTemplate, string, error) {
		var resp *transcoderpb.ListJobTemplatesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListJobTemplates(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetJobTemplates(), resp.GetNextPageToken(), nil
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

func (c *gRPCClient) GetJobTemplate(ctx context.Context, req *transcoderpb.GetJobTemplateRequest, opts ...gax.CallOption) (*transcoderpb.JobTemplate, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetJobTemplate[0:len((*c.CallOptions).GetJobTemplate):len((*c.CallOptions).GetJobTemplate)], opts...)
	var resp *transcoderpb.JobTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetJobTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) DeleteJobTemplate(ctx context.Context, req *transcoderpb.DeleteJobTemplateRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).DeleteJobTemplate[0:len((*c.CallOptions).DeleteJobTemplate):len((*c.CallOptions).DeleteJobTemplate)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteJobTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// JobIterator manages a stream of *transcoderpb.Job.
type JobIterator struct {
	items    []*transcoderpb.Job
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
	InternalFetch func(pageSize int, pageToken string) (results []*transcoderpb.Job, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *JobIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *JobIterator) Next() (*transcoderpb.Job, error) {
	var item *transcoderpb.Job
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *JobIterator) bufLen() int {
	return len(it.items)
}

func (it *JobIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// JobTemplateIterator manages a stream of *transcoderpb.JobTemplate.
type JobTemplateIterator struct {
	items    []*transcoderpb.JobTemplate
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
	InternalFetch func(pageSize int, pageToken string) (results []*transcoderpb.JobTemplate, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *JobTemplateIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *JobTemplateIterator) Next() (*transcoderpb.JobTemplate, error) {
	var item *transcoderpb.JobTemplate
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *JobTemplateIterator) bufLen() int {
	return len(it.items)
}

func (it *JobTemplateIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
