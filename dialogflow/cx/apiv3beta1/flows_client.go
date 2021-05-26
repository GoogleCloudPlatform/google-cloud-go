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

package cx

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	"cloud.google.com/go/longrunning"
	lroauto "cloud.google.com/go/longrunning/autogen"
	"github.com/golang/protobuf/proto"
	structpb "github.com/golang/protobuf/ptypes/struct"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3beta1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newFlowsClientHook clientHook

// FlowsCallOptions contains the retry settings for each method of FlowsClient.
type FlowsCallOptions struct {
	CreateFlow              []gax.CallOption
	DeleteFlow              []gax.CallOption
	ListFlows               []gax.CallOption
	GetFlow                 []gax.CallOption
	UpdateFlow              []gax.CallOption
	TrainFlow               []gax.CallOption
	ValidateFlow            []gax.CallOption
	GetFlowValidationResult []gax.CallOption
	ImportFlow              []gax.CallOption
	ExportFlow              []gax.CallOption
}

func defaultFlowsGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("dialogflow.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("dialogflow.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://dialogflow.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultFlowsCallOptions() *FlowsCallOptions {
	return &FlowsCallOptions{
		CreateFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListFlows: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		UpdateFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		TrainFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ValidateFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetFlowValidationResult: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ImportFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ExportFlow: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
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

// internalFlowsClient is an interface that defines the methods availaible from Dialogflow API.
type internalFlowsClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	CreateFlow(context.Context, *cxpb.CreateFlowRequest, ...gax.CallOption) (*cxpb.Flow, error)
	DeleteFlow(context.Context, *cxpb.DeleteFlowRequest, ...gax.CallOption) error
	ListFlows(context.Context, *cxpb.ListFlowsRequest, ...gax.CallOption) *FlowIterator
	GetFlow(context.Context, *cxpb.GetFlowRequest, ...gax.CallOption) (*cxpb.Flow, error)
	UpdateFlow(context.Context, *cxpb.UpdateFlowRequest, ...gax.CallOption) (*cxpb.Flow, error)
	TrainFlow(context.Context, *cxpb.TrainFlowRequest, ...gax.CallOption) (*TrainFlowOperation, error)
	TrainFlowOperation(name string) *TrainFlowOperation
	ValidateFlow(context.Context, *cxpb.ValidateFlowRequest, ...gax.CallOption) (*cxpb.FlowValidationResult, error)
	GetFlowValidationResult(context.Context, *cxpb.GetFlowValidationResultRequest, ...gax.CallOption) (*cxpb.FlowValidationResult, error)
	ImportFlow(context.Context, *cxpb.ImportFlowRequest, ...gax.CallOption) (*ImportFlowOperation, error)
	ImportFlowOperation(name string) *ImportFlowOperation
	ExportFlow(context.Context, *cxpb.ExportFlowRequest, ...gax.CallOption) (*ExportFlowOperation, error)
	ExportFlowOperation(name string) *ExportFlowOperation
}

// FlowsClient is a client for interacting with Dialogflow API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// Service for managing Flows.
type FlowsClient struct {
	// The internal transport-dependent client.
	internalClient internalFlowsClient

	// The call options for this service.
	CallOptions *FlowsCallOptions

	// LROClient is used internally to handle long-running operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *FlowsClient) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *FlowsClient) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *FlowsClient) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// CreateFlow creates a flow in the specified agent.
func (c *FlowsClient) CreateFlow(ctx context.Context, req *cxpb.CreateFlowRequest, opts ...gax.CallOption) (*cxpb.Flow, error) {
	return c.internalClient.CreateFlow(ctx, req, opts...)
}

// DeleteFlow deletes a specified flow.
func (c *FlowsClient) DeleteFlow(ctx context.Context, req *cxpb.DeleteFlowRequest, opts ...gax.CallOption) error {
	return c.internalClient.DeleteFlow(ctx, req, opts...)
}

// ListFlows returns the list of all flows in the specified agent.
func (c *FlowsClient) ListFlows(ctx context.Context, req *cxpb.ListFlowsRequest, opts ...gax.CallOption) *FlowIterator {
	return c.internalClient.ListFlows(ctx, req, opts...)
}

// GetFlow retrieves the specified flow.
func (c *FlowsClient) GetFlow(ctx context.Context, req *cxpb.GetFlowRequest, opts ...gax.CallOption) (*cxpb.Flow, error) {
	return c.internalClient.GetFlow(ctx, req, opts...)
}

// UpdateFlow updates the specified flow.
func (c *FlowsClient) UpdateFlow(ctx context.Context, req *cxpb.UpdateFlowRequest, opts ...gax.CallOption) (*cxpb.Flow, error) {
	return c.internalClient.UpdateFlow(ctx, req, opts...)
}

// TrainFlow trains the specified flow. Note that only the flow in ‘draft’ environment
// is trained.
func (c *FlowsClient) TrainFlow(ctx context.Context, req *cxpb.TrainFlowRequest, opts ...gax.CallOption) (*TrainFlowOperation, error) {
	return c.internalClient.TrainFlow(ctx, req, opts...)
}

// TrainFlowOperation returns a new TrainFlowOperation from a given name.
// The name must be that of a previously created TrainFlowOperation, possibly from a different process.
func (c *FlowsClient) TrainFlowOperation(name string) *TrainFlowOperation {
	return c.internalClient.TrainFlowOperation(name)
}

// ValidateFlow validates the specified flow and creates or updates validation results.
// Please call this API after the training is completed to get the complete
// validation results.
func (c *FlowsClient) ValidateFlow(ctx context.Context, req *cxpb.ValidateFlowRequest, opts ...gax.CallOption) (*cxpb.FlowValidationResult, error) {
	return c.internalClient.ValidateFlow(ctx, req, opts...)
}

// GetFlowValidationResult gets the latest flow validation result. Flow validation is performed
// when ValidateFlow is called.
func (c *FlowsClient) GetFlowValidationResult(ctx context.Context, req *cxpb.GetFlowValidationResultRequest, opts ...gax.CallOption) (*cxpb.FlowValidationResult, error) {
	return c.internalClient.GetFlowValidationResult(ctx, req, opts...)
}

// ImportFlow imports the specified flow to the specified agent from a binary file.
func (c *FlowsClient) ImportFlow(ctx context.Context, req *cxpb.ImportFlowRequest, opts ...gax.CallOption) (*ImportFlowOperation, error) {
	return c.internalClient.ImportFlow(ctx, req, opts...)
}

// ImportFlowOperation returns a new ImportFlowOperation from a given name.
// The name must be that of a previously created ImportFlowOperation, possibly from a different process.
func (c *FlowsClient) ImportFlowOperation(name string) *ImportFlowOperation {
	return c.internalClient.ImportFlowOperation(name)
}

// ExportFlow exports the specified flow to a binary file.
//
// Note that resources (e.g. intents, entities, webhooks) that the flow
// references will also be exported.
func (c *FlowsClient) ExportFlow(ctx context.Context, req *cxpb.ExportFlowRequest, opts ...gax.CallOption) (*ExportFlowOperation, error) {
	return c.internalClient.ExportFlow(ctx, req, opts...)
}

// ExportFlowOperation returns a new ExportFlowOperation from a given name.
// The name must be that of a previously created ExportFlowOperation, possibly from a different process.
func (c *FlowsClient) ExportFlowOperation(name string) *ExportFlowOperation {
	return c.internalClient.ExportFlowOperation(name)
}

// flowsGRPCClient is a client for interacting with Dialogflow API over gRPC transport.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type flowsGRPCClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// Points back to the CallOptions field of the containing FlowsClient
	CallOptions **FlowsCallOptions

	// The gRPC API client.
	flowsClient cxpb.FlowsClient

	// LROClient is used internally to handle long-running operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient **lroauto.OperationsClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewFlowsClient creates a new flows client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// Service for managing Flows.
func NewFlowsClient(ctx context.Context, opts ...option.ClientOption) (*FlowsClient, error) {
	clientOpts := defaultFlowsGRPCClientOptions()
	if newFlowsClientHook != nil {
		hookOpts, err := newFlowsClientHook(ctx, clientHookParams{})
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
	client := FlowsClient{CallOptions: defaultFlowsCallOptions()}

	c := &flowsGRPCClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		flowsClient:      cxpb.NewFlowsClient(connPool),
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
func (c *flowsGRPCClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *flowsGRPCClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *flowsGRPCClient) Close() error {
	return c.connPool.Close()
}

func (c *flowsGRPCClient) CreateFlow(ctx context.Context, req *cxpb.CreateFlowRequest, opts ...gax.CallOption) (*cxpb.Flow, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).CreateFlow[0:len((*c.CallOptions).CreateFlow):len((*c.CallOptions).CreateFlow)], opts...)
	var resp *cxpb.Flow
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.CreateFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *flowsGRPCClient) DeleteFlow(ctx context.Context, req *cxpb.DeleteFlowRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).DeleteFlow[0:len((*c.CallOptions).DeleteFlow):len((*c.CallOptions).DeleteFlow)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.flowsClient.DeleteFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

func (c *flowsGRPCClient) ListFlows(ctx context.Context, req *cxpb.ListFlowsRequest, opts ...gax.CallOption) *FlowIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListFlows[0:len((*c.CallOptions).ListFlows):len((*c.CallOptions).ListFlows)], opts...)
	it := &FlowIterator{}
	req = proto.Clone(req).(*cxpb.ListFlowsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*cxpb.Flow, string, error) {
		var resp *cxpb.ListFlowsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.flowsClient.ListFlows(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetFlows(), resp.GetNextPageToken(), nil
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

func (c *flowsGRPCClient) GetFlow(ctx context.Context, req *cxpb.GetFlowRequest, opts ...gax.CallOption) (*cxpb.Flow, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetFlow[0:len((*c.CallOptions).GetFlow):len((*c.CallOptions).GetFlow)], opts...)
	var resp *cxpb.Flow
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.GetFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *flowsGRPCClient) UpdateFlow(ctx context.Context, req *cxpb.UpdateFlowRequest, opts ...gax.CallOption) (*cxpb.Flow, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "flow.name", url.QueryEscape(req.GetFlow().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).UpdateFlow[0:len((*c.CallOptions).UpdateFlow):len((*c.CallOptions).UpdateFlow)], opts...)
	var resp *cxpb.Flow
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.UpdateFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *flowsGRPCClient) TrainFlow(ctx context.Context, req *cxpb.TrainFlowRequest, opts ...gax.CallOption) (*TrainFlowOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).TrainFlow[0:len((*c.CallOptions).TrainFlow):len((*c.CallOptions).TrainFlow)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.TrainFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &TrainFlowOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, resp),
	}, nil
}

func (c *flowsGRPCClient) ValidateFlow(ctx context.Context, req *cxpb.ValidateFlowRequest, opts ...gax.CallOption) (*cxpb.FlowValidationResult, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ValidateFlow[0:len((*c.CallOptions).ValidateFlow):len((*c.CallOptions).ValidateFlow)], opts...)
	var resp *cxpb.FlowValidationResult
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.ValidateFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *flowsGRPCClient) GetFlowValidationResult(ctx context.Context, req *cxpb.GetFlowValidationResultRequest, opts ...gax.CallOption) (*cxpb.FlowValidationResult, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetFlowValidationResult[0:len((*c.CallOptions).GetFlowValidationResult):len((*c.CallOptions).GetFlowValidationResult)], opts...)
	var resp *cxpb.FlowValidationResult
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.GetFlowValidationResult(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *flowsGRPCClient) ImportFlow(ctx context.Context, req *cxpb.ImportFlowRequest, opts ...gax.CallOption) (*ImportFlowOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ImportFlow[0:len((*c.CallOptions).ImportFlow):len((*c.CallOptions).ImportFlow)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.ImportFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &ImportFlowOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, resp),
	}, nil
}

func (c *flowsGRPCClient) ExportFlow(ctx context.Context, req *cxpb.ExportFlowRequest, opts ...gax.CallOption) (*ExportFlowOperation, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ExportFlow[0:len((*c.CallOptions).ExportFlow):len((*c.CallOptions).ExportFlow)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.flowsClient.ExportFlow(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &ExportFlowOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, resp),
	}, nil
}

// ExportFlowOperation manages a long-running operation from ExportFlow.
type ExportFlowOperation struct {
	lro *longrunning.Operation
}

// ExportFlowOperation returns a new ExportFlowOperation from a given name.
// The name must be that of a previously created ExportFlowOperation, possibly from a different process.
func (c *flowsGRPCClient) ExportFlowOperation(name string) *ExportFlowOperation {
	return &ExportFlowOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *ExportFlowOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*cxpb.ExportFlowResponse, error) {
	var resp cxpb.ExportFlowResponse
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
func (op *ExportFlowOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*cxpb.ExportFlowResponse, error) {
	var resp cxpb.ExportFlowResponse
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
func (op *ExportFlowOperation) Metadata() (*structpb.Struct, error) {
	var meta structpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *ExportFlowOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *ExportFlowOperation) Name() string {
	return op.lro.Name()
}

// ImportFlowOperation manages a long-running operation from ImportFlow.
type ImportFlowOperation struct {
	lro *longrunning.Operation
}

// ImportFlowOperation returns a new ImportFlowOperation from a given name.
// The name must be that of a previously created ImportFlowOperation, possibly from a different process.
func (c *flowsGRPCClient) ImportFlowOperation(name string) *ImportFlowOperation {
	return &ImportFlowOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *ImportFlowOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*cxpb.ImportFlowResponse, error) {
	var resp cxpb.ImportFlowResponse
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
func (op *ImportFlowOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*cxpb.ImportFlowResponse, error) {
	var resp cxpb.ImportFlowResponse
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
func (op *ImportFlowOperation) Metadata() (*structpb.Struct, error) {
	var meta structpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *ImportFlowOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *ImportFlowOperation) Name() string {
	return op.lro.Name()
}

// TrainFlowOperation manages a long-running operation from TrainFlow.
type TrainFlowOperation struct {
	lro *longrunning.Operation
}

// TrainFlowOperation returns a new TrainFlowOperation from a given name.
// The name must be that of a previously created TrainFlowOperation, possibly from a different process.
func (c *flowsGRPCClient) TrainFlowOperation(name string) *TrainFlowOperation {
	return &TrainFlowOperation{
		lro: longrunning.InternalNewOperation(*c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *TrainFlowOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
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
func (op *TrainFlowOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *TrainFlowOperation) Metadata() (*structpb.Struct, error) {
	var meta structpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *TrainFlowOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *TrainFlowOperation) Name() string {
	return op.lro.Name()
}

// FlowIterator manages a stream of *cxpb.Flow.
type FlowIterator struct {
	items    []*cxpb.Flow
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
	InternalFetch func(pageSize int, pageToken string) (results []*cxpb.Flow, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *FlowIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *FlowIterator) Next() (*cxpb.Flow, error) {
	var item *cxpb.Flow
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *FlowIterator) bufLen() int {
	return len(it.items)
}

func (it *FlowIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
