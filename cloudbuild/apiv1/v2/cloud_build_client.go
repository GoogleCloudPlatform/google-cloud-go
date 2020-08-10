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

package cloudbuild

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	"cloud.google.com/go/longrunning"
	lroauto "cloud.google.com/go/longrunning/autogen"
	"github.com/golang/protobuf/proto"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	cloudbuildpb "google.golang.org/genproto/googleapis/devtools/cloudbuild/v1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	CreateBuild        []gax.CallOption
	GetBuild           []gax.CallOption
	ListBuilds         []gax.CallOption
	CancelBuild        []gax.CallOption
	RetryBuild         []gax.CallOption
	CreateBuildTrigger []gax.CallOption
	GetBuildTrigger    []gax.CallOption
	ListBuildTriggers  []gax.CallOption
	DeleteBuildTrigger []gax.CallOption
	UpdateBuildTrigger []gax.CallOption
	RunBuildTrigger    []gax.CallOption
	CreateWorkerPool   []gax.CallOption
	GetWorkerPool      []gax.CallOption
	DeleteWorkerPool   []gax.CallOption
	UpdateWorkerPool   []gax.CallOption
	ListWorkerPools    []gax.CallOption
}

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("cloudbuild.googleapis.com:443"),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		CreateBuild: []gax.CallOption{},
		GetBuild: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListBuilds: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CancelBuild:        []gax.CallOption{},
		RetryBuild:         []gax.CallOption{},
		CreateBuildTrigger: []gax.CallOption{},
		GetBuildTrigger: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListBuildTriggers: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteBuildTrigger: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		UpdateBuildTrigger: []gax.CallOption{},
		RunBuildTrigger:    []gax.CallOption{},
		CreateWorkerPool:   []gax.CallOption{},
		GetWorkerPool: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteWorkerPool: []gax.CallOption{},
		UpdateWorkerPool: []gax.CallOption{},
		ListWorkerPools: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// Client is a client for interacting with Cloud Build API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type Client struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// The gRPC API client.
	client cloudbuildpb.CloudBuildClient

	// LROClient is used internally to handle longrunning operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient

	// The call options for this service.
	CallOptions *CallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new cloud build client.
//
// Creates and manages builds on Google Cloud Platform.
//
// The main concept used by this API is a Build, which describes the location
// of the source to build, how to build the source, and where to store the
// built artifacts, if any.
//
// A user can list previously-requested builds or get builds by their ID to
// determine the status of the build.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	clientOpts := defaultClientOptions()

	if newClientHook != nil {
		hookOpts, err := newClientHook(ctx, clientHookParams{})
		if err != nil {
			return nil, err
		}
		clientOpts = append(clientOpts, hookOpts...)
	}

	connPool, err := gtransport.DialPool(ctx, append(clientOpts, opts...)...)
	if err != nil {
		return nil, err
	}
	c := &Client{
		connPool:    connPool,
		CallOptions: defaultCallOptions(),

		client: cloudbuildpb.NewCloudBuildClient(connPool),
	}
	c.setGoogleClientInfo()

	c.LROClient, err = lroauto.NewOperationsClient(ctx, gtransport.WithConnPool(connPool))
	if err != nil {
		// This error "should not happen", since we are just reusing old connection pool
		// and never actually need to dial.
		// If this does happen, we could leak connp. However, we cannot close conn:
		// If the user invoked the constructor with option.WithGRPCConn,
		// we would close a connection that's still in use.
		// TODO: investigate error conditions.
		return nil, err
	}
	return c, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *Client) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *Client) Close() error {
	return c.connPool.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *Client) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// CreateBuild starts a build with the specified configuration.
//
// This method returns a long-running Operation, which includes the build
// ID. Pass the build ID to GetBuild to determine the build status (such as
// SUCCESS or FAILURE).
func (c *Client) CreateBuild(ctx context.Context, req *cloudbuildpb.CreateBuildRequest, opts ...gax.CallOption) (*CreateBuildOperation, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "project_id", url.QueryEscape(req.GetProjectId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateBuild[0:len(c.CallOptions.CreateBuild):len(c.CallOptions.CreateBuild)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateBuild(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &CreateBuildOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// GetBuild returns information about a previously requested build.
//
// The Build that is returned includes its status (such as SUCCESS,
// FAILURE, or WORKING), and timing information.
func (c *Client) GetBuild(ctx context.Context, req *cloudbuildpb.GetBuildRequest, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "project_id", url.QueryEscape(req.GetProjectId()), "id", url.QueryEscape(req.GetId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetBuild[0:len(c.CallOptions.GetBuild):len(c.CallOptions.GetBuild)], opts...)
	var resp *cloudbuildpb.Build
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetBuild(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListBuilds lists previously requested builds.
//
// Previously requested builds may still be in-progress, or may have finished
// successfully or unsuccessfully.
func (c *Client) ListBuilds(ctx context.Context, req *cloudbuildpb.ListBuildsRequest, opts ...gax.CallOption) *BuildIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "project_id", url.QueryEscape(req.GetProjectId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListBuilds[0:len(c.CallOptions.ListBuilds):len(c.CallOptions.ListBuilds)], opts...)
	it := &BuildIterator{}
	req = proto.Clone(req).(*cloudbuildpb.ListBuildsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*cloudbuildpb.Build, string, error) {
		var resp *cloudbuildpb.ListBuildsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListBuilds(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetBuilds(), resp.GetNextPageToken(), nil
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

// CancelBuild cancels a build in progress.
func (c *Client) CancelBuild(ctx context.Context, req *cloudbuildpb.CancelBuildRequest, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "project_id", url.QueryEscape(req.GetProjectId()), "id", url.QueryEscape(req.GetId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CancelBuild[0:len(c.CallOptions.CancelBuild):len(c.CallOptions.CancelBuild)], opts...)
	var resp *cloudbuildpb.Build
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CancelBuild(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// RetryBuild creates a new build based on the specified build.
//
// This method creates a new build using the original build request, which may
// or may not result in an identical build.
//
// For triggered builds:
//
//   Triggered builds resolve to a precise revision; therefore a retry of a
//   triggered build will result in a build that uses the same revision.
//
// For non-triggered builds that specify RepoSource:
//
//   If the original build built from the tip of a branch, the retried build
//   will build from the tip of that branch, which may not be the same revision
//   as the original build.
//
//   If the original build specified a commit sha or revision ID, the retried
//   build will use the identical source.
//
// For builds that specify StorageSource:
//
//   If the original build pulled source from Google Cloud Storage without
//   specifying the generation of the object, the new build will use the current
//   object, which may be different from the original build source.
//
//   If the original build pulled source from Cloud Storage and specified the
//   generation of the object, the new build will attempt to use the same
//   object, which may or may not be available depending on the bucket’s
//   lifecycle management settings.
func (c *Client) RetryBuild(ctx context.Context, req *cloudbuildpb.RetryBuildRequest, opts ...gax.CallOption) (*RetryBuildOperation, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "project_id", url.QueryEscape(req.GetProjectId()), "id", url.QueryEscape(req.GetId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.RetryBuild[0:len(c.CallOptions.RetryBuild):len(c.CallOptions.RetryBuild)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.RetryBuild(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &RetryBuildOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// CreateBuildTrigger creates a new BuildTrigger.
//
// This API is experimental.
func (c *Client) CreateBuildTrigger(ctx context.Context, req *cloudbuildpb.CreateBuildTriggerRequest, opts ...gax.CallOption) (*cloudbuildpb.BuildTrigger, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "project_id", url.QueryEscape(req.GetProjectId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateBuildTrigger[0:len(c.CallOptions.CreateBuildTrigger):len(c.CallOptions.CreateBuildTrigger)], opts...)
	var resp *cloudbuildpb.BuildTrigger
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateBuildTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetBuildTrigger returns information about a BuildTrigger.
//
// This API is experimental.
func (c *Client) GetBuildTrigger(ctx context.Context, req *cloudbuildpb.GetBuildTriggerRequest, opts ...gax.CallOption) (*cloudbuildpb.BuildTrigger, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "project_id", url.QueryEscape(req.GetProjectId()), "trigger_id", url.QueryEscape(req.GetTriggerId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetBuildTrigger[0:len(c.CallOptions.GetBuildTrigger):len(c.CallOptions.GetBuildTrigger)], opts...)
	var resp *cloudbuildpb.BuildTrigger
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetBuildTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListBuildTriggers lists existing BuildTriggers.
//
// This API is experimental.
func (c *Client) ListBuildTriggers(ctx context.Context, req *cloudbuildpb.ListBuildTriggersRequest, opts ...gax.CallOption) *BuildTriggerIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "project_id", url.QueryEscape(req.GetProjectId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListBuildTriggers[0:len(c.CallOptions.ListBuildTriggers):len(c.CallOptions.ListBuildTriggers)], opts...)
	it := &BuildTriggerIterator{}
	req = proto.Clone(req).(*cloudbuildpb.ListBuildTriggersRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*cloudbuildpb.BuildTrigger, string, error) {
		var resp *cloudbuildpb.ListBuildTriggersResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListBuildTriggers(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetTriggers(), resp.GetNextPageToken(), nil
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

// DeleteBuildTrigger deletes a BuildTrigger by its project ID and trigger ID.
//
// This API is experimental.
func (c *Client) DeleteBuildTrigger(ctx context.Context, req *cloudbuildpb.DeleteBuildTriggerRequest, opts ...gax.CallOption) error {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "project_id", url.QueryEscape(req.GetProjectId()), "trigger_id", url.QueryEscape(req.GetTriggerId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteBuildTrigger[0:len(c.CallOptions.DeleteBuildTrigger):len(c.CallOptions.DeleteBuildTrigger)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteBuildTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// UpdateBuildTrigger updates a BuildTrigger by its project ID and trigger ID.
//
// This API is experimental.
func (c *Client) UpdateBuildTrigger(ctx context.Context, req *cloudbuildpb.UpdateBuildTriggerRequest, opts ...gax.CallOption) (*cloudbuildpb.BuildTrigger, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "project_id", url.QueryEscape(req.GetProjectId()), "trigger_id", url.QueryEscape(req.GetTriggerId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateBuildTrigger[0:len(c.CallOptions.UpdateBuildTrigger):len(c.CallOptions.UpdateBuildTrigger)], opts...)
	var resp *cloudbuildpb.BuildTrigger
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateBuildTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// RunBuildTrigger runs a BuildTrigger at a particular source revision.
func (c *Client) RunBuildTrigger(ctx context.Context, req *cloudbuildpb.RunBuildTriggerRequest, opts ...gax.CallOption) (*RunBuildTriggerOperation, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v&%s=%v", "project_id", url.QueryEscape(req.GetProjectId()), "trigger_id", url.QueryEscape(req.GetTriggerId())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.RunBuildTrigger[0:len(c.CallOptions.RunBuildTrigger):len(c.CallOptions.RunBuildTrigger)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.RunBuildTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &RunBuildTriggerOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// CreateWorkerPool creates a WorkerPool to run the builds, and returns the new worker pool.
//
// This API is experimental.
func (c *Client) CreateWorkerPool(ctx context.Context, req *cloudbuildpb.CreateWorkerPoolRequest, opts ...gax.CallOption) (*cloudbuildpb.WorkerPool, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateWorkerPool[0:len(c.CallOptions.CreateWorkerPool):len(c.CallOptions.CreateWorkerPool)], opts...)
	var resp *cloudbuildpb.WorkerPool
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateWorkerPool(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetWorkerPool returns information about a WorkerPool.
//
// This API is experimental.
func (c *Client) GetWorkerPool(ctx context.Context, req *cloudbuildpb.GetWorkerPoolRequest, opts ...gax.CallOption) (*cloudbuildpb.WorkerPool, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetWorkerPool[0:len(c.CallOptions.GetWorkerPool):len(c.CallOptions.GetWorkerPool)], opts...)
	var resp *cloudbuildpb.WorkerPool
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetWorkerPool(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteWorkerPool deletes a WorkerPool by its project ID and WorkerPool name.
//
// This API is experimental.
func (c *Client) DeleteWorkerPool(ctx context.Context, req *cloudbuildpb.DeleteWorkerPoolRequest, opts ...gax.CallOption) error {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteWorkerPool[0:len(c.CallOptions.DeleteWorkerPool):len(c.CallOptions.DeleteWorkerPool)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteWorkerPool(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// UpdateWorkerPool update a WorkerPool.
//
// This API is experimental.
func (c *Client) UpdateWorkerPool(ctx context.Context, req *cloudbuildpb.UpdateWorkerPoolRequest, opts ...gax.CallOption) (*cloudbuildpb.WorkerPool, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateWorkerPool[0:len(c.CallOptions.UpdateWorkerPool):len(c.CallOptions.UpdateWorkerPool)], opts...)
	var resp *cloudbuildpb.WorkerPool
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateWorkerPool(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListWorkerPools list project’s WorkerPools.
//
// This API is experimental.
func (c *Client) ListWorkerPools(ctx context.Context, req *cloudbuildpb.ListWorkerPoolsRequest, opts ...gax.CallOption) (*cloudbuildpb.ListWorkerPoolsResponse, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 600000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListWorkerPools[0:len(c.CallOptions.ListWorkerPools):len(c.CallOptions.ListWorkerPools)], opts...)
	var resp *cloudbuildpb.ListWorkerPoolsResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.ListWorkerPools(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateBuildOperation manages a long-running operation from CreateBuild.
type CreateBuildOperation struct {
	lro *longrunning.Operation
}

// CreateBuildOperation returns a new CreateBuildOperation from a given name.
// The name must be that of a previously created CreateBuildOperation, possibly from a different process.
func (c *Client) CreateBuildOperation(name string) *CreateBuildOperation {
	return &CreateBuildOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *CreateBuildOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	var resp cloudbuildpb.Build
	if err := op.lro.WaitWithInterval(ctx, &resp, time.Minute, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully,
// op.Done will return true, and the response of the operation is returned.
// If Poll succeeds and the operation has not completed, the returned response and error are both nil.
func (op *CreateBuildOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	var resp cloudbuildpb.Build
	if err := op.lro.Poll(ctx, &resp, opts...); err != nil {
		return nil, err
	}
	if !op.Done() {
		return nil, nil
	}
	return &resp, nil
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *CreateBuildOperation) Metadata() (*cloudbuildpb.BuildOperationMetadata, error) {
	var meta cloudbuildpb.BuildOperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *CreateBuildOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *CreateBuildOperation) Name() string {
	return op.lro.Name()
}

// RetryBuildOperation manages a long-running operation from RetryBuild.
type RetryBuildOperation struct {
	lro *longrunning.Operation
}

// RetryBuildOperation returns a new RetryBuildOperation from a given name.
// The name must be that of a previously created RetryBuildOperation, possibly from a different process.
func (c *Client) RetryBuildOperation(name string) *RetryBuildOperation {
	return &RetryBuildOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *RetryBuildOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	var resp cloudbuildpb.Build
	if err := op.lro.WaitWithInterval(ctx, &resp, time.Minute, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully,
// op.Done will return true, and the response of the operation is returned.
// If Poll succeeds and the operation has not completed, the returned response and error are both nil.
func (op *RetryBuildOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	var resp cloudbuildpb.Build
	if err := op.lro.Poll(ctx, &resp, opts...); err != nil {
		return nil, err
	}
	if !op.Done() {
		return nil, nil
	}
	return &resp, nil
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *RetryBuildOperation) Metadata() (*cloudbuildpb.BuildOperationMetadata, error) {
	var meta cloudbuildpb.BuildOperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *RetryBuildOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *RetryBuildOperation) Name() string {
	return op.lro.Name()
}

// RunBuildTriggerOperation manages a long-running operation from RunBuildTrigger.
type RunBuildTriggerOperation struct {
	lro *longrunning.Operation
}

// RunBuildTriggerOperation returns a new RunBuildTriggerOperation from a given name.
// The name must be that of a previously created RunBuildTriggerOperation, possibly from a different process.
func (c *Client) RunBuildTriggerOperation(name string) *RunBuildTriggerOperation {
	return &RunBuildTriggerOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *RunBuildTriggerOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	var resp cloudbuildpb.Build
	if err := op.lro.WaitWithInterval(ctx, &resp, time.Minute, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully,
// op.Done will return true, and the response of the operation is returned.
// If Poll succeeds and the operation has not completed, the returned response and error are both nil.
func (op *RunBuildTriggerOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*cloudbuildpb.Build, error) {
	var resp cloudbuildpb.Build
	if err := op.lro.Poll(ctx, &resp, opts...); err != nil {
		return nil, err
	}
	if !op.Done() {
		return nil, nil
	}
	return &resp, nil
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *RunBuildTriggerOperation) Metadata() (*cloudbuildpb.BuildOperationMetadata, error) {
	var meta cloudbuildpb.BuildOperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *RunBuildTriggerOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *RunBuildTriggerOperation) Name() string {
	return op.lro.Name()
}

// BuildIterator manages a stream of *cloudbuildpb.Build.
type BuildIterator struct {
	items    []*cloudbuildpb.Build
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
	InternalFetch func(pageSize int, pageToken string) (results []*cloudbuildpb.Build, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *BuildIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *BuildIterator) Next() (*cloudbuildpb.Build, error) {
	var item *cloudbuildpb.Build
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *BuildIterator) bufLen() int {
	return len(it.items)
}

func (it *BuildIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// BuildTriggerIterator manages a stream of *cloudbuildpb.BuildTrigger.
type BuildTriggerIterator struct {
	items    []*cloudbuildpb.BuildTrigger
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
	InternalFetch func(pageSize int, pageToken string) (results []*cloudbuildpb.BuildTrigger, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *BuildTriggerIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *BuildTriggerIterator) Next() (*cloudbuildpb.BuildTrigger, error) {
	var item *cloudbuildpb.BuildTrigger
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *BuildTriggerIterator) bufLen() int {
	return len(it.items)
}

func (it *BuildTriggerIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
