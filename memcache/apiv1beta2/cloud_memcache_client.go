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

package memcache

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
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	memcachepb "google.golang.org/genproto/googleapis/cloud/memcache/v1beta2"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var newCloudMemcacheClientHook clientHook

// CloudMemcacheCallOptions contains the retry settings for each method of CloudMemcacheClient.
type CloudMemcacheCallOptions struct {
	ListInstances       []gax.CallOption
	GetInstance         []gax.CallOption
	CreateInstance      []gax.CallOption
	UpdateInstance      []gax.CallOption
	UpdateParameters    []gax.CallOption
	DeleteInstance      []gax.CallOption
	ApplyParameters     []gax.CallOption
	ApplySoftwareUpdate []gax.CallOption
}

func defaultCloudMemcacheClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("memcache.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("memcache.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://memcache.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCloudMemcacheCallOptions() *CloudMemcacheCallOptions {
	return &CloudMemcacheCallOptions{
		ListInstances:       []gax.CallOption{},
		GetInstance:         []gax.CallOption{},
		CreateInstance:      []gax.CallOption{},
		UpdateInstance:      []gax.CallOption{},
		UpdateParameters:    []gax.CallOption{},
		DeleteInstance:      []gax.CallOption{},
		ApplyParameters:     []gax.CallOption{},
		ApplySoftwareUpdate: []gax.CallOption{},
	}
}

// CloudMemcacheClient is a client for interacting with Cloud Memorystore for Memcached API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type CloudMemcacheClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// The gRPC API client.
	cloudMemcacheClient memcachepb.CloudMemcacheClient

	// LROClient is used internally to handle longrunning operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient

	// The call options for this service.
	CallOptions *CloudMemcacheCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewCloudMemcacheClient creates a new cloud memcache client.
//
// Configures and manages Cloud Memorystore for Memcached instances.
//
// The memcache.googleapis.com service implements the Google Cloud Memorystore
// for Memcached API and defines the following resource model for managing
// Memorystore Memcached (also called Memcached below) instances:
//
//   The service works with a collection of cloud projects, named: /projects/*
//
//   Each project has a collection of available locations, named: /locations/*
//
//   Each location has a collection of Memcached instances, named:
//   /instances/*
//
//   As such, Memcached instances are resources of the form:
//   /projects/{project_id}/locations/{location_id}/instances/{instance_id}
//
// Note that location_id must be a GCP region; for example:
//
//   projects/my-memcached-project/locations/us-central1/instances/my-memcached
func NewCloudMemcacheClient(ctx context.Context, opts ...option.ClientOption) (*CloudMemcacheClient, error) {
	clientOpts := defaultCloudMemcacheClientOptions()

	if newCloudMemcacheClientHook != nil {
		hookOpts, err := newCloudMemcacheClientHook(ctx, clientHookParams{})
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
	c := &CloudMemcacheClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		CallOptions:      defaultCloudMemcacheCallOptions(),

		cloudMemcacheClient: memcachepb.NewCloudMemcacheClient(connPool),
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
func (c *CloudMemcacheClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *CloudMemcacheClient) Close() error {
	return c.connPool.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *CloudMemcacheClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// ListInstances lists Instances in a given location.
func (c *CloudMemcacheClient) ListInstances(ctx context.Context, req *memcachepb.ListInstancesRequest, opts ...gax.CallOption) *InstanceIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListInstances[0:len(c.CallOptions.ListInstances):len(c.CallOptions.ListInstances)], opts...)
	it := &InstanceIterator{}
	req = proto.Clone(req).(*memcachepb.ListInstancesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*memcachepb.Instance, string, error) {
		var resp *memcachepb.ListInstancesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.cloudMemcacheClient.ListInstances(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetResources(), resp.GetNextPageToken(), nil
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

// GetInstance gets details of a single Instance.
func (c *CloudMemcacheClient) GetInstance(ctx context.Context, req *memcachepb.GetInstanceRequest, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 1200000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetInstance[0:len(c.CallOptions.GetInstance):len(c.CallOptions.GetInstance)], opts...)
	var resp *memcachepb.Instance
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cloudMemcacheClient.GetInstance(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateInstance creates a new Instance in a given location.
func (c *CloudMemcacheClient) CreateInstance(ctx context.Context, req *memcachepb.CreateInstanceRequest, opts ...gax.CallOption) (*CreateInstanceOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 1200000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateInstance[0:len(c.CallOptions.CreateInstance):len(c.CallOptions.CreateInstance)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cloudMemcacheClient.CreateInstance(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &CreateInstanceOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// UpdateInstance updates an existing Instance in a given project and location.
func (c *CloudMemcacheClient) UpdateInstance(ctx context.Context, req *memcachepb.UpdateInstanceRequest, opts ...gax.CallOption) (*UpdateInstanceOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 1200000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource.name", url.QueryEscape(req.GetResource().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateInstance[0:len(c.CallOptions.UpdateInstance):len(c.CallOptions.UpdateInstance)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cloudMemcacheClient.UpdateInstance(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &UpdateInstanceOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// UpdateParameters updates the defined Memcached parameters for an existing instance.
// This method only stages the parameters, it must be followed by
// ApplyParameters to apply the parameters to nodes of the Memcached
// instance.
func (c *CloudMemcacheClient) UpdateParameters(ctx context.Context, req *memcachepb.UpdateParametersRequest, opts ...gax.CallOption) (*UpdateParametersOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 1200000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateParameters[0:len(c.CallOptions.UpdateParameters):len(c.CallOptions.UpdateParameters)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cloudMemcacheClient.UpdateParameters(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &UpdateParametersOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// DeleteInstance deletes a single Instance.
func (c *CloudMemcacheClient) DeleteInstance(ctx context.Context, req *memcachepb.DeleteInstanceRequest, opts ...gax.CallOption) (*DeleteInstanceOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 1200000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteInstance[0:len(c.CallOptions.DeleteInstance):len(c.CallOptions.DeleteInstance)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cloudMemcacheClient.DeleteInstance(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &DeleteInstanceOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// ApplyParameters ApplyParameters restarts the set of specified nodes in order to update
// them to the current set of parameters for the Memcached Instance.
func (c *CloudMemcacheClient) ApplyParameters(ctx context.Context, req *memcachepb.ApplyParametersRequest, opts ...gax.CallOption) (*ApplyParametersOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 1200000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ApplyParameters[0:len(c.CallOptions.ApplyParameters):len(c.CallOptions.ApplyParameters)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cloudMemcacheClient.ApplyParameters(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &ApplyParametersOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// ApplySoftwareUpdate updates software on the selected nodes of the Instance.
func (c *CloudMemcacheClient) ApplySoftwareUpdate(ctx context.Context, req *memcachepb.ApplySoftwareUpdateRequest, opts ...gax.CallOption) (*ApplySoftwareUpdateOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 1200000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "instance", url.QueryEscape(req.GetInstance())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ApplySoftwareUpdate[0:len(c.CallOptions.ApplySoftwareUpdate):len(c.CallOptions.ApplySoftwareUpdate)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.cloudMemcacheClient.ApplySoftwareUpdate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &ApplySoftwareUpdateOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// ApplyParametersOperation manages a long-running operation from ApplyParameters.
type ApplyParametersOperation struct {
	lro *longrunning.Operation
}

// ApplyParametersOperation returns a new ApplyParametersOperation from a given name.
// The name must be that of a previously created ApplyParametersOperation, possibly from a different process.
func (c *CloudMemcacheClient) ApplyParametersOperation(name string) *ApplyParametersOperation {
	return &ApplyParametersOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *ApplyParametersOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *ApplyParametersOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *ApplyParametersOperation) Metadata() (*memcachepb.OperationMetadata, error) {
	var meta memcachepb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *ApplyParametersOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *ApplyParametersOperation) Name() string {
	return op.lro.Name()
}

// ApplySoftwareUpdateOperation manages a long-running operation from ApplySoftwareUpdate.
type ApplySoftwareUpdateOperation struct {
	lro *longrunning.Operation
}

// ApplySoftwareUpdateOperation returns a new ApplySoftwareUpdateOperation from a given name.
// The name must be that of a previously created ApplySoftwareUpdateOperation, possibly from a different process.
func (c *CloudMemcacheClient) ApplySoftwareUpdateOperation(name string) *ApplySoftwareUpdateOperation {
	return &ApplySoftwareUpdateOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *ApplySoftwareUpdateOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *ApplySoftwareUpdateOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *ApplySoftwareUpdateOperation) Metadata() (*memcachepb.OperationMetadata, error) {
	var meta memcachepb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *ApplySoftwareUpdateOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *ApplySoftwareUpdateOperation) Name() string {
	return op.lro.Name()
}

// CreateInstanceOperation manages a long-running operation from CreateInstance.
type CreateInstanceOperation struct {
	lro *longrunning.Operation
}

// CreateInstanceOperation returns a new CreateInstanceOperation from a given name.
// The name must be that of a previously created CreateInstanceOperation, possibly from a different process.
func (c *CloudMemcacheClient) CreateInstanceOperation(name string) *CreateInstanceOperation {
	return &CreateInstanceOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *CreateInstanceOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *CreateInstanceOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *CreateInstanceOperation) Metadata() (*memcachepb.OperationMetadata, error) {
	var meta memcachepb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *CreateInstanceOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *CreateInstanceOperation) Name() string {
	return op.lro.Name()
}

// DeleteInstanceOperation manages a long-running operation from DeleteInstance.
type DeleteInstanceOperation struct {
	lro *longrunning.Operation
}

// DeleteInstanceOperation returns a new DeleteInstanceOperation from a given name.
// The name must be that of a previously created DeleteInstanceOperation, possibly from a different process.
func (c *CloudMemcacheClient) DeleteInstanceOperation(name string) *DeleteInstanceOperation {
	return &DeleteInstanceOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *DeleteInstanceOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, time.Minute, opts...)
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
func (op *DeleteInstanceOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *DeleteInstanceOperation) Metadata() (*memcachepb.OperationMetadata, error) {
	var meta memcachepb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *DeleteInstanceOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *DeleteInstanceOperation) Name() string {
	return op.lro.Name()
}

// UpdateInstanceOperation manages a long-running operation from UpdateInstance.
type UpdateInstanceOperation struct {
	lro *longrunning.Operation
}

// UpdateInstanceOperation returns a new UpdateInstanceOperation from a given name.
// The name must be that of a previously created UpdateInstanceOperation, possibly from a different process.
func (c *CloudMemcacheClient) UpdateInstanceOperation(name string) *UpdateInstanceOperation {
	return &UpdateInstanceOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *UpdateInstanceOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *UpdateInstanceOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *UpdateInstanceOperation) Metadata() (*memcachepb.OperationMetadata, error) {
	var meta memcachepb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *UpdateInstanceOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *UpdateInstanceOperation) Name() string {
	return op.lro.Name()
}

// UpdateParametersOperation manages a long-running operation from UpdateParameters.
type UpdateParametersOperation struct {
	lro *longrunning.Operation
}

// UpdateParametersOperation returns a new UpdateParametersOperation from a given name.
// The name must be that of a previously created UpdateParametersOperation, possibly from a different process.
func (c *CloudMemcacheClient) UpdateParametersOperation(name string) *UpdateParametersOperation {
	return &UpdateParametersOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *UpdateParametersOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *UpdateParametersOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*memcachepb.Instance, error) {
	var resp memcachepb.Instance
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
func (op *UpdateParametersOperation) Metadata() (*memcachepb.OperationMetadata, error) {
	var meta memcachepb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *UpdateParametersOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *UpdateParametersOperation) Name() string {
	return op.lro.Name()
}

// InstanceIterator manages a stream of *memcachepb.Instance.
type InstanceIterator struct {
	items    []*memcachepb.Instance
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
	InternalFetch func(pageSize int, pageToken string) (results []*memcachepb.Instance, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *InstanceIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *InstanceIterator) Next() (*memcachepb.Instance, error) {
	var item *memcachepb.Instance
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *InstanceIterator) bufLen() int {
	return len(it.items)
}

func (it *InstanceIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
