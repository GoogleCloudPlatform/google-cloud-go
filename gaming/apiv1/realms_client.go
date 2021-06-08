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

package gaming

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
	gamingpb "google.golang.org/genproto/googleapis/cloud/gaming/v1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newRealmsClientHook clientHook

// RealmsCallOptions contains the retry settings for each method of RealmsClient.
type RealmsCallOptions struct {
	ListRealms         []gax.CallOption
	GetRealm           []gax.CallOption
	CreateRealm        []gax.CallOption
	DeleteRealm        []gax.CallOption
	UpdateRealm        []gax.CallOption
	PreviewRealmUpdate []gax.CallOption
}

func defaultRealmsGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("gameservices.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("gameservices.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://gameservices.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultRealmsCallOptions() *RealmsCallOptions {
	return &RealmsCallOptions{
		ListRealms: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetRealm: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CreateRealm: []gax.CallOption{},
		DeleteRealm: []gax.CallOption{},
		UpdateRealm: []gax.CallOption{},
		PreviewRealmUpdate: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// internalRealmsClient is an interface that defines the methods availaible from Game Services API.
type internalRealmsClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	ListRealms(context.Context, *gamingpb.ListRealmsRequest, ...gax.CallOption) *RealmIterator
	GetRealm(context.Context, *gamingpb.GetRealmRequest, ...gax.CallOption) (*gamingpb.Realm, error)
	CreateRealm(context.Context, *gamingpb.CreateRealmRequest, ...gax.CallOption) (*CreateRealmOperation, error)
	CreateRealmOperation(name string) *CreateRealmOperation
	DeleteRealm(context.Context, *gamingpb.DeleteRealmRequest, ...gax.CallOption) (*DeleteRealmOperation, error)
	DeleteRealmOperation(name string) *DeleteRealmOperation
	UpdateRealm(context.Context, *gamingpb.UpdateRealmRequest, ...gax.CallOption) (*UpdateRealmOperation, error)
	UpdateRealmOperation(name string) *UpdateRealmOperation
	PreviewRealmUpdate(context.Context, *gamingpb.PreviewRealmUpdateRequest, ...gax.CallOption) (*gamingpb.PreviewRealmUpdateResponse, error)
}

// RealmsClient is a client for interacting with Game Services API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// A realm is a grouping of game server clusters that are considered
// interchangeable.
type RealmsClient struct {
	// The internal transport-dependent client.
	internalClient internalRealmsClient

	// The call options for this service.
	CallOptions *RealmsCallOptions

	// LROClient is used internally to handle long-running operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *RealmsClient) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *RealmsClient) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *RealmsClient) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// ListRealms lists realms in a given project and location.
func (c *RealmsClient) ListRealms(ctx context.Context, req *gamingpb.ListRealmsRequest, opts ...gax.CallOption) *RealmIterator {
	return c.internalClient.ListRealms(ctx, req, opts...)
}

// GetRealm gets details of a single realm.
func (c *RealmsClient) GetRealm(ctx context.Context, req *gamingpb.GetRealmRequest, opts ...gax.CallOption) (*gamingpb.Realm, error) {
	return c.internalClient.GetRealm(ctx, req, opts...)
}

// CreateRealm creates a new realm in a given project and location.
func (c *RealmsClient) CreateRealm(ctx context.Context, req *gamingpb.CreateRealmRequest, opts ...gax.CallOption) (*CreateRealmOperation, error) {
	return c.internalClient.CreateRealm(ctx, req, opts...)
}

// CreateRealmOperation returns a new CreateRealmOperation from a given name.
// The name must be that of a previously created CreateRealmOperation, possibly from a different process.
func (c *RealmsClient) CreateRealmOperation(name string) *CreateRealmOperation {
	return c.internalClient.CreateRealmOperation(name)
}

// DeleteRealm deletes a single realm.
func (c *RealmsClient) DeleteRealm(ctx context.Context, req *gamingpb.DeleteRealmRequest, opts ...gax.CallOption) (*DeleteRealmOperation, error) {
	return c.internalClient.DeleteRealm(ctx, req, opts...)
}

// DeleteRealmOperation returns a new DeleteRealmOperation from a given name.
// The name must be that of a previously created DeleteRealmOperation, possibly from a different process.
func (c *RealmsClient) DeleteRealmOperation(name string) *DeleteRealmOperation {
	return c.internalClient.DeleteRealmOperation(name)
}

// UpdateRealm patches a single realm.
func (c *RealmsClient) UpdateRealm(ctx context.Context, req *gamingpb.UpdateRealmRequest, opts ...gax.CallOption) (*UpdateRealmOperation, error) {
	return c.internalClient.UpdateRealm(ctx, req, opts...)
}

// UpdateRealmOperation returns a new UpdateRealmOperation from a given name.
// The name must be that of a previously created UpdateRealmOperation, possibly from a different process.
func (c *RealmsClient) UpdateRealmOperation(name string) *UpdateRealmOperation {
	return c.internalClient.UpdateRealmOperation(name)
}

// PreviewRealmUpdate previews patches to a single realm.
func (c *RealmsClient) PreviewRealmUpdate(ctx context.Context, req *gamingpb.PreviewRealmUpdateRequest, opts ...gax.CallOption) (*gamingpb.PreviewRealmUpdateResponse, error) {
	return c.internalClient.PreviewRealmUpdate(ctx, req, opts...)
}

// realmsGRPCClient is a client for interacting with Game Services API over gRPC transport.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type realmsGRPCClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// Points back to the CallOptions field of the containing RealmsClient
	CallOptions **RealmsCallOptions

	// The gRPC API client.
	realmsClient gamingpb.RealmsServiceClient

	// LROClient is used internally to handle long-running operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient **lroauto.OperationsClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewRealmsClient creates a new realms service client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// A realm is a grouping of game server clusters that are considered
// interchangeable.
func NewRealmsClient(ctx context.Context, opts ...option.ClientOption) (*RealmsClient, error) {
	clientOpts := defaultRealmsGRPCClientOptions()
	if newRealmsClientHook != nil {
		hookOpts, err := newRealmsClientHook(ctx, clientHookParams{})
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
	client := RealmsClient{CallOptions: defaultRealmsCallOptions()}

	c := &realmsGRPCClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		realmsClient:     gamingpb.NewRealmsServiceClient(connPool),
		CallOptions:      &client.CallOptions,
	}
	c.setGoogleClientInfo()

	client.internalClient = c

	client.LROClient, err = lroauto.NewOperationsClient(ctx, gtransport.WithConnPool(connPool))
	if err != nil {
		// This error "should not happen", since we are just reusing old connection pool
		// and never actually need to dial.
		// If this does happen, we could leak connp. However, we cannot close conn:
		// If the user invoked the constructor with option.WithGRPCConn,
		// we would close a connection that's still in use.
		// TODO: investigate error conditions.
		return nil, err
	}
	c.LROClient = &client.LROClient
	return &client, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *realmsGRPCClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *realmsGRPCClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *realmsGRPCClient) Close() error {
	return c.connPool.Close()
}

func (c *realmsGRPCClient) ListRealms(ctx context.Context, req *gamingpb.ListRealmsRequest, opts ...gax.CallOption) *RealmIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListRealms[0:len((*c.CallOptions).ListRealms):len((*c.CallOptions).ListRealms)], opts...)
	it := &RealmIterator{}
	req = proto.Clone(req).(*gamingpb.ListRealmsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*gamingpb.Realm, string, error) {
		var resp *gamingpb.ListRealmsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.realmsClient.ListRealms(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetRealms(), resp.GetNextPageToken(), nil
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

func (c *realmsGRPCClient) GetRealm(ctx context.Context, req *gamingpb.GetRealmRequest, opts ...gax.CallOption) (*gamingpb.Realm, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetRealm[0:len((*c.CallOptions).GetRealm):len((*c.CallOptions).GetRealm)], opts...)
	var resp *gamingpb.Realm
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.realmsClient.GetRealm(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *realmsGRPCClient) CreateRealm(ctx context.Context, req *gamingpb.CreateRealmRequest, opts ...gax.CallOption) (*CreateRealmOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).CreateRealm[0:len((*c.CallOptions).CreateRealm):len((*c.CallOptions).CreateRealm)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.realmsClient.CreateRealm(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &CreateRealmOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, resp),
	}, nil
}

func (c *realmsGRPCClient) DeleteRealm(ctx context.Context, req *gamingpb.DeleteRealmRequest, opts ...gax.CallOption) (*DeleteRealmOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).DeleteRealm[0:len((*c.CallOptions).DeleteRealm):len((*c.CallOptions).DeleteRealm)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.realmsClient.DeleteRealm(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &DeleteRealmOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, resp),
	}, nil
}

func (c *realmsGRPCClient) UpdateRealm(ctx context.Context, req *gamingpb.UpdateRealmRequest, opts ...gax.CallOption) (*UpdateRealmOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "realm.name", url.QueryEscape(req.GetRealm().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).UpdateRealm[0:len((*c.CallOptions).UpdateRealm):len((*c.CallOptions).UpdateRealm)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.realmsClient.UpdateRealm(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &UpdateRealmOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, resp),
	}, nil
}

func (c *realmsGRPCClient) PreviewRealmUpdate(ctx context.Context, req *gamingpb.PreviewRealmUpdateRequest, opts ...gax.CallOption) (*gamingpb.PreviewRealmUpdateResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "realm.name", url.QueryEscape(req.GetRealm().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).PreviewRealmUpdate[0:len((*c.CallOptions).PreviewRealmUpdate):len((*c.CallOptions).PreviewRealmUpdate)], opts...)
	var resp *gamingpb.PreviewRealmUpdateResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.realmsClient.PreviewRealmUpdate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateRealmOperation manages a long-running operation from CreateRealm.
type CreateRealmOperation struct {
	lro *longrunning.Operation
}

// CreateRealmOperation returns a new CreateRealmOperation from a given name.
// The name must be that of a previously created CreateRealmOperation, possibly from a different process.
func (c *realmsGRPCClient) CreateRealmOperation(name string) *CreateRealmOperation {
	return &CreateRealmOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *CreateRealmOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*gamingpb.Realm, error) {
	var resp gamingpb.Realm
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
func (op *CreateRealmOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*gamingpb.Realm, error) {
	var resp gamingpb.Realm
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
func (op *CreateRealmOperation) Metadata() (*gamingpb.OperationMetadata, error) {
	var meta gamingpb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *CreateRealmOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *CreateRealmOperation) Name() string {
	return op.lro.Name()
}

// DeleteRealmOperation manages a long-running operation from DeleteRealm.
type DeleteRealmOperation struct {
	lro *longrunning.Operation
}

// DeleteRealmOperation returns a new DeleteRealmOperation from a given name.
// The name must be that of a previously created DeleteRealmOperation, possibly from a different process.
func (c *realmsGRPCClient) DeleteRealmOperation(name string) *DeleteRealmOperation {
	return &DeleteRealmOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *DeleteRealmOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
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
func (op *DeleteRealmOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *DeleteRealmOperation) Metadata() (*gamingpb.OperationMetadata, error) {
	var meta gamingpb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *DeleteRealmOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *DeleteRealmOperation) Name() string {
	return op.lro.Name()
}

// UpdateRealmOperation manages a long-running operation from UpdateRealm.
type UpdateRealmOperation struct {
	lro *longrunning.Operation
}

// UpdateRealmOperation returns a new UpdateRealmOperation from a given name.
// The name must be that of a previously created UpdateRealmOperation, possibly from a different process.
func (c *realmsGRPCClient) UpdateRealmOperation(name string) *UpdateRealmOperation {
	return &UpdateRealmOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *UpdateRealmOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*gamingpb.Realm, error) {
	var resp gamingpb.Realm
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
func (op *UpdateRealmOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*gamingpb.Realm, error) {
	var resp gamingpb.Realm
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
func (op *UpdateRealmOperation) Metadata() (*gamingpb.OperationMetadata, error) {
	var meta gamingpb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *UpdateRealmOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *UpdateRealmOperation) Name() string {
	return op.lro.Name()
}

// RealmIterator manages a stream of *gamingpb.Realm.
type RealmIterator struct {
	items    []*gamingpb.Realm
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
	InternalFetch func(pageSize int, pageToken string) (results []*gamingpb.Realm, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *RealmIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *RealmIterator) Next() (*gamingpb.Realm, error) {
	var item *gamingpb.Realm
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *RealmIterator) bufLen() int {
	return len(it.items)
}

func (it *RealmIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
