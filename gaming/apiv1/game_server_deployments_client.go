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

var newGameServerDeploymentsClientHook clientHook

// GameServerDeploymentsCallOptions contains the retry settings for each method of GameServerDeploymentsClient.
type GameServerDeploymentsCallOptions struct {
	ListGameServerDeployments          []gax.CallOption
	GetGameServerDeployment            []gax.CallOption
	CreateGameServerDeployment         []gax.CallOption
	DeleteGameServerDeployment         []gax.CallOption
	UpdateGameServerDeployment         []gax.CallOption
	GetGameServerDeploymentRollout     []gax.CallOption
	UpdateGameServerDeploymentRollout  []gax.CallOption
	PreviewGameServerDeploymentRollout []gax.CallOption
	FetchDeploymentState               []gax.CallOption
}

func defaultGameServerDeploymentsClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("gameservices.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("gameservices.mtls.googleapis.com:443"),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultGameServerDeploymentsCallOptions() *GameServerDeploymentsCallOptions {
	return &GameServerDeploymentsCallOptions{
		ListGameServerDeployments: []gax.CallOption{
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
		GetGameServerDeployment: []gax.CallOption{
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
		CreateGameServerDeployment: []gax.CallOption{},
		DeleteGameServerDeployment: []gax.CallOption{},
		UpdateGameServerDeployment: []gax.CallOption{},
		GetGameServerDeploymentRollout: []gax.CallOption{
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
		UpdateGameServerDeploymentRollout: []gax.CallOption{},
		PreviewGameServerDeploymentRollout: []gax.CallOption{
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
		FetchDeploymentState: []gax.CallOption{
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

// GameServerDeploymentsClient is a client for interacting with .
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type GameServerDeploymentsClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// The gRPC API client.
	gameServerDeploymentsClient gamingpb.GameServerDeploymentsServiceClient

	// LROClient is used internally to handle longrunning operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient

	// The call options for this service.
	CallOptions *GameServerDeploymentsCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewGameServerDeploymentsClient creates a new game server deployments service client.
//
// The game server deployment is used to control the deployment of Agones
// fleets.
func NewGameServerDeploymentsClient(ctx context.Context, opts ...option.ClientOption) (*GameServerDeploymentsClient, error) {
	clientOpts := defaultGameServerDeploymentsClientOptions()

	if newGameServerDeploymentsClientHook != nil {
		hookOpts, err := newGameServerDeploymentsClientHook(ctx, clientHookParams{})
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
	c := &GameServerDeploymentsClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		CallOptions:      defaultGameServerDeploymentsCallOptions(),

		gameServerDeploymentsClient: gamingpb.NewGameServerDeploymentsServiceClient(connPool),
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
func (c *GameServerDeploymentsClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *GameServerDeploymentsClient) Close() error {
	return c.connPool.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *GameServerDeploymentsClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// ListGameServerDeployments lists game server deployments in a given project and location.
func (c *GameServerDeploymentsClient) ListGameServerDeployments(ctx context.Context, req *gamingpb.ListGameServerDeploymentsRequest, opts ...gax.CallOption) *GameServerDeploymentIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListGameServerDeployments[0:len(c.CallOptions.ListGameServerDeployments):len(c.CallOptions.ListGameServerDeployments)], opts...)
	it := &GameServerDeploymentIterator{}
	req = proto.Clone(req).(*gamingpb.ListGameServerDeploymentsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*gamingpb.GameServerDeployment, string, error) {
		var resp *gamingpb.ListGameServerDeploymentsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.gameServerDeploymentsClient.ListGameServerDeployments(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetGameServerDeployments(), resp.GetNextPageToken(), nil
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

// GetGameServerDeployment gets details of a single game server deployment.
func (c *GameServerDeploymentsClient) GetGameServerDeployment(ctx context.Context, req *gamingpb.GetGameServerDeploymentRequest, opts ...gax.CallOption) (*gamingpb.GameServerDeployment, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetGameServerDeployment[0:len(c.CallOptions.GetGameServerDeployment):len(c.CallOptions.GetGameServerDeployment)], opts...)
	var resp *gamingpb.GameServerDeployment
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.GetGameServerDeployment(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateGameServerDeployment creates a new game server deployment in a given project and location.
func (c *GameServerDeploymentsClient) CreateGameServerDeployment(ctx context.Context, req *gamingpb.CreateGameServerDeploymentRequest, opts ...gax.CallOption) (*CreateGameServerDeploymentOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateGameServerDeployment[0:len(c.CallOptions.CreateGameServerDeployment):len(c.CallOptions.CreateGameServerDeployment)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.CreateGameServerDeployment(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &CreateGameServerDeploymentOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// DeleteGameServerDeployment deletes a single game server deployment.
func (c *GameServerDeploymentsClient) DeleteGameServerDeployment(ctx context.Context, req *gamingpb.DeleteGameServerDeploymentRequest, opts ...gax.CallOption) (*DeleteGameServerDeploymentOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteGameServerDeployment[0:len(c.CallOptions.DeleteGameServerDeployment):len(c.CallOptions.DeleteGameServerDeployment)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.DeleteGameServerDeployment(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &DeleteGameServerDeploymentOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// UpdateGameServerDeployment patches a game server deployment.
func (c *GameServerDeploymentsClient) UpdateGameServerDeployment(ctx context.Context, req *gamingpb.UpdateGameServerDeploymentRequest, opts ...gax.CallOption) (*UpdateGameServerDeploymentOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "game_server_deployment.name", url.QueryEscape(req.GetGameServerDeployment().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateGameServerDeployment[0:len(c.CallOptions.UpdateGameServerDeployment):len(c.CallOptions.UpdateGameServerDeployment)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.UpdateGameServerDeployment(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &UpdateGameServerDeploymentOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// GetGameServerDeploymentRollout gets details a single game server deployment rollout.
func (c *GameServerDeploymentsClient) GetGameServerDeploymentRollout(ctx context.Context, req *gamingpb.GetGameServerDeploymentRolloutRequest, opts ...gax.CallOption) (*gamingpb.GameServerDeploymentRollout, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetGameServerDeploymentRollout[0:len(c.CallOptions.GetGameServerDeploymentRollout):len(c.CallOptions.GetGameServerDeploymentRollout)], opts...)
	var resp *gamingpb.GameServerDeploymentRollout
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.GetGameServerDeploymentRollout(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateGameServerDeploymentRollout patches a single game server deployment rollout.
// The method will not return an error if the update does not affect any
// existing realms. For example - if the default_game_server_config is changed
// but all existing realms use the override, that is valid. Similarly, if a
// non existing realm is explicitly called out in game_server_config_overrides
// field, that will also not result in an error.
func (c *GameServerDeploymentsClient) UpdateGameServerDeploymentRollout(ctx context.Context, req *gamingpb.UpdateGameServerDeploymentRolloutRequest, opts ...gax.CallOption) (*UpdateGameServerDeploymentRolloutOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "rollout.name", url.QueryEscape(req.GetRollout().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateGameServerDeploymentRollout[0:len(c.CallOptions.UpdateGameServerDeploymentRollout):len(c.CallOptions.UpdateGameServerDeploymentRollout)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.UpdateGameServerDeploymentRollout(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &UpdateGameServerDeploymentRolloutOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// PreviewGameServerDeploymentRollout previews the game server deployment rollout. This API does not mutate the
// rollout resource.
func (c *GameServerDeploymentsClient) PreviewGameServerDeploymentRollout(ctx context.Context, req *gamingpb.PreviewGameServerDeploymentRolloutRequest, opts ...gax.CallOption) (*gamingpb.PreviewGameServerDeploymentRolloutResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "rollout.name", url.QueryEscape(req.GetRollout().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.PreviewGameServerDeploymentRollout[0:len(c.CallOptions.PreviewGameServerDeploymentRollout):len(c.CallOptions.PreviewGameServerDeploymentRollout)], opts...)
	var resp *gamingpb.PreviewGameServerDeploymentRolloutResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.PreviewGameServerDeploymentRollout(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FetchDeploymentState retrieves information about the current state of the game server
// deployment. Gathers all the Agones fleets and Agones autoscalers,
// including fleets running an older version of the game server deployment.
func (c *GameServerDeploymentsClient) FetchDeploymentState(ctx context.Context, req *gamingpb.FetchDeploymentStateRequest, opts ...gax.CallOption) (*gamingpb.FetchDeploymentStateResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 120000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.FetchDeploymentState[0:len(c.CallOptions.FetchDeploymentState):len(c.CallOptions.FetchDeploymentState)], opts...)
	var resp *gamingpb.FetchDeploymentStateResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.gameServerDeploymentsClient.FetchDeploymentState(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateGameServerDeploymentOperation manages a long-running operation from CreateGameServerDeployment.
type CreateGameServerDeploymentOperation struct {
	lro *longrunning.Operation
}

// CreateGameServerDeploymentOperation returns a new CreateGameServerDeploymentOperation from a given name.
// The name must be that of a previously created CreateGameServerDeploymentOperation, possibly from a different process.
func (c *GameServerDeploymentsClient) CreateGameServerDeploymentOperation(name string) *CreateGameServerDeploymentOperation {
	return &CreateGameServerDeploymentOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *CreateGameServerDeploymentOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*gamingpb.GameServerDeployment, error) {
	var resp gamingpb.GameServerDeployment
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
func (op *CreateGameServerDeploymentOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*gamingpb.GameServerDeployment, error) {
	var resp gamingpb.GameServerDeployment
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
func (op *CreateGameServerDeploymentOperation) Metadata() (*gamingpb.OperationMetadata, error) {
	var meta gamingpb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *CreateGameServerDeploymentOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *CreateGameServerDeploymentOperation) Name() string {
	return op.lro.Name()
}

// DeleteGameServerDeploymentOperation manages a long-running operation from DeleteGameServerDeployment.
type DeleteGameServerDeploymentOperation struct {
	lro *longrunning.Operation
}

// DeleteGameServerDeploymentOperation returns a new DeleteGameServerDeploymentOperation from a given name.
// The name must be that of a previously created DeleteGameServerDeploymentOperation, possibly from a different process.
func (c *GameServerDeploymentsClient) DeleteGameServerDeploymentOperation(name string) *DeleteGameServerDeploymentOperation {
	return &DeleteGameServerDeploymentOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *DeleteGameServerDeploymentOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
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
func (op *DeleteGameServerDeploymentOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *DeleteGameServerDeploymentOperation) Metadata() (*gamingpb.OperationMetadata, error) {
	var meta gamingpb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *DeleteGameServerDeploymentOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *DeleteGameServerDeploymentOperation) Name() string {
	return op.lro.Name()
}

// UpdateGameServerDeploymentOperation manages a long-running operation from UpdateGameServerDeployment.
type UpdateGameServerDeploymentOperation struct {
	lro *longrunning.Operation
}

// UpdateGameServerDeploymentOperation returns a new UpdateGameServerDeploymentOperation from a given name.
// The name must be that of a previously created UpdateGameServerDeploymentOperation, possibly from a different process.
func (c *GameServerDeploymentsClient) UpdateGameServerDeploymentOperation(name string) *UpdateGameServerDeploymentOperation {
	return &UpdateGameServerDeploymentOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *UpdateGameServerDeploymentOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*gamingpb.GameServerDeployment, error) {
	var resp gamingpb.GameServerDeployment
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
func (op *UpdateGameServerDeploymentOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*gamingpb.GameServerDeployment, error) {
	var resp gamingpb.GameServerDeployment
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
func (op *UpdateGameServerDeploymentOperation) Metadata() (*gamingpb.OperationMetadata, error) {
	var meta gamingpb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *UpdateGameServerDeploymentOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *UpdateGameServerDeploymentOperation) Name() string {
	return op.lro.Name()
}

// UpdateGameServerDeploymentRolloutOperation manages a long-running operation from UpdateGameServerDeploymentRollout.
type UpdateGameServerDeploymentRolloutOperation struct {
	lro *longrunning.Operation
}

// UpdateGameServerDeploymentRolloutOperation returns a new UpdateGameServerDeploymentRolloutOperation from a given name.
// The name must be that of a previously created UpdateGameServerDeploymentRolloutOperation, possibly from a different process.
func (c *GameServerDeploymentsClient) UpdateGameServerDeploymentRolloutOperation(name string) *UpdateGameServerDeploymentRolloutOperation {
	return &UpdateGameServerDeploymentRolloutOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *UpdateGameServerDeploymentRolloutOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*gamingpb.GameServerDeployment, error) {
	var resp gamingpb.GameServerDeployment
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
func (op *UpdateGameServerDeploymentRolloutOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*gamingpb.GameServerDeployment, error) {
	var resp gamingpb.GameServerDeployment
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
func (op *UpdateGameServerDeploymentRolloutOperation) Metadata() (*gamingpb.OperationMetadata, error) {
	var meta gamingpb.OperationMetadata
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *UpdateGameServerDeploymentRolloutOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *UpdateGameServerDeploymentRolloutOperation) Name() string {
	return op.lro.Name()
}

// GameServerDeploymentIterator manages a stream of *gamingpb.GameServerDeployment.
type GameServerDeploymentIterator struct {
	items    []*gamingpb.GameServerDeployment
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
	InternalFetch func(pageSize int, pageToken string) (results []*gamingpb.GameServerDeployment, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *GameServerDeploymentIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *GameServerDeploymentIterator) Next() (*gamingpb.GameServerDeployment, error) {
	var item *gamingpb.GameServerDeployment
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *GameServerDeploymentIterator) bufLen() int {
	return len(it.items)
}

func (it *GameServerDeploymentIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
