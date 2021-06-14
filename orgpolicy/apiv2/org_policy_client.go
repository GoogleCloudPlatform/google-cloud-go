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

package orgpolicy

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
	orgpolicypb "google.golang.org/genproto/googleapis/cloud/orgpolicy/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	ListConstraints    []gax.CallOption
	ListPolicies       []gax.CallOption
	GetPolicy          []gax.CallOption
	GetEffectivePolicy []gax.CallOption
	CreatePolicy       []gax.CallOption
	UpdatePolicy       []gax.CallOption
	DeletePolicy       []gax.CallOption
}

func defaultGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("orgpolicy.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("orgpolicy.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://orgpolicy.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		ListConstraints: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListPolicies: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetPolicy: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetEffectivePolicy: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CreatePolicy: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		UpdatePolicy: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeletePolicy: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// internalClient is an interface that defines the methods availaible from Organization Policy API.
type internalClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	ListConstraints(context.Context, *orgpolicypb.ListConstraintsRequest, ...gax.CallOption) *ConstraintIterator
	ListPolicies(context.Context, *orgpolicypb.ListPoliciesRequest, ...gax.CallOption) *PolicyIterator
	GetPolicy(context.Context, *orgpolicypb.GetPolicyRequest, ...gax.CallOption) (*orgpolicypb.Policy, error)
	GetEffectivePolicy(context.Context, *orgpolicypb.GetEffectivePolicyRequest, ...gax.CallOption) (*orgpolicypb.Policy, error)
	CreatePolicy(context.Context, *orgpolicypb.CreatePolicyRequest, ...gax.CallOption) (*orgpolicypb.Policy, error)
	UpdatePolicy(context.Context, *orgpolicypb.UpdatePolicyRequest, ...gax.CallOption) (*orgpolicypb.Policy, error)
	DeletePolicy(context.Context, *orgpolicypb.DeletePolicyRequest, ...gax.CallOption) error
}

// Client is a client for interacting with Organization Policy API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// An interface for managing organization policies.
//
// The Cloud Org Policy service provides a simple mechanism for organizations to
// restrict the allowed configurations across their entire Cloud Resource
// hierarchy.
//
// You can use a policy to configure restrictions in Cloud resources. For
// example, you can enforce a policy that restricts which Google
// Cloud Platform APIs can be activated in a certain part of your resource
// hierarchy, or prevents serial port access to VM instances in a particular
// folder.
//
// Policies are inherited down through the resource hierarchy. A policy
// applied to a parent resource automatically applies to all its child resources
// unless overridden with a policy lower in the hierarchy.
//
// A constraint defines an aspect of a resource’s configuration that can be
// controlled by an organization’s policy administrator. Policies are a
// collection of constraints that defines their allowable configuration on a
// particular resource and its child resources.
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

// ListConstraints lists Constraints that could be applied on the specified resource.
func (c *Client) ListConstraints(ctx context.Context, req *orgpolicypb.ListConstraintsRequest, opts ...gax.CallOption) *ConstraintIterator {
	return c.internalClient.ListConstraints(ctx, req, opts...)
}

// ListPolicies retrieves all of the Policies that exist on a particular resource.
func (c *Client) ListPolicies(ctx context.Context, req *orgpolicypb.ListPoliciesRequest, opts ...gax.CallOption) *PolicyIterator {
	return c.internalClient.ListPolicies(ctx, req, opts...)
}

// GetPolicy gets a Policy on a resource.
//
// If no Policy is set on the resource, NOT_FOUND is returned. The
// etag value can be used with UpdatePolicy() to update a
// Policy during read-modify-write.
func (c *Client) GetPolicy(ctx context.Context, req *orgpolicypb.GetPolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	return c.internalClient.GetPolicy(ctx, req, opts...)
}

// GetEffectivePolicy gets the effective Policy on a resource. This is the result of merging
// Policies in the resource hierarchy and evaluating conditions. The
// returned Policy will not have an etag or condition set because it is
// a computed Policy across multiple resources.
// Subtrees of Resource Manager resource hierarchy with ‘under:’ prefix will
// not be expanded.
func (c *Client) GetEffectivePolicy(ctx context.Context, req *orgpolicypb.GetEffectivePolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	return c.internalClient.GetEffectivePolicy(ctx, req, opts...)
}

// CreatePolicy creates a Policy.
//
// Returns a google.rpc.Status with google.rpc.Code.NOT_FOUND if the
// constraint does not exist.
// Returns a google.rpc.Status with google.rpc.Code.ALREADY_EXISTS if the
// policy already exists on the given Cloud resource.
func (c *Client) CreatePolicy(ctx context.Context, req *orgpolicypb.CreatePolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	return c.internalClient.CreatePolicy(ctx, req, opts...)
}

// UpdatePolicy updates a Policy.
//
// Returns a google.rpc.Status with google.rpc.Code.NOT_FOUND if the
// constraint or the policy do not exist.
// Returns a google.rpc.Status with google.rpc.Code.ABORTED if the etag
// supplied in the request does not match the persisted etag of the policy
//
// Note: the supplied policy will perform a full overwrite of all
// fields.
func (c *Client) UpdatePolicy(ctx context.Context, req *orgpolicypb.UpdatePolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	return c.internalClient.UpdatePolicy(ctx, req, opts...)
}

// DeletePolicy deletes a Policy.
//
// Returns a google.rpc.Status with google.rpc.Code.NOT_FOUND if the
// constraint or Org Policy does not exist.
func (c *Client) DeletePolicy(ctx context.Context, req *orgpolicypb.DeletePolicyRequest, opts ...gax.CallOption) error {
	return c.internalClient.DeletePolicy(ctx, req, opts...)
}

// gRPCClient is a client for interacting with Organization Policy API over gRPC transport.
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
	client orgpolicypb.OrgPolicyClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new org policy client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// An interface for managing organization policies.
//
// The Cloud Org Policy service provides a simple mechanism for organizations to
// restrict the allowed configurations across their entire Cloud Resource
// hierarchy.
//
// You can use a policy to configure restrictions in Cloud resources. For
// example, you can enforce a policy that restricts which Google
// Cloud Platform APIs can be activated in a certain part of your resource
// hierarchy, or prevents serial port access to VM instances in a particular
// folder.
//
// Policies are inherited down through the resource hierarchy. A policy
// applied to a parent resource automatically applies to all its child resources
// unless overridden with a policy lower in the hierarchy.
//
// A constraint defines an aspect of a resource’s configuration that can be
// controlled by an organization’s policy administrator. Policies are a
// collection of constraints that defines their allowable configuration on a
// particular resource and its child resources.
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
		client:           orgpolicypb.NewOrgPolicyClient(connPool),
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

func (c *gRPCClient) ListConstraints(ctx context.Context, req *orgpolicypb.ListConstraintsRequest, opts ...gax.CallOption) *ConstraintIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListConstraints[0:len((*c.CallOptions).ListConstraints):len((*c.CallOptions).ListConstraints)], opts...)
	it := &ConstraintIterator{}
	req = proto.Clone(req).(*orgpolicypb.ListConstraintsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*orgpolicypb.Constraint, string, error) {
		var resp *orgpolicypb.ListConstraintsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListConstraints(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetConstraints(), resp.GetNextPageToken(), nil
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

func (c *gRPCClient) ListPolicies(ctx context.Context, req *orgpolicypb.ListPoliciesRequest, opts ...gax.CallOption) *PolicyIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListPolicies[0:len((*c.CallOptions).ListPolicies):len((*c.CallOptions).ListPolicies)], opts...)
	it := &PolicyIterator{}
	req = proto.Clone(req).(*orgpolicypb.ListPoliciesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*orgpolicypb.Policy, string, error) {
		var resp *orgpolicypb.ListPoliciesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListPolicies(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetPolicies(), resp.GetNextPageToken(), nil
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

func (c *gRPCClient) GetPolicy(ctx context.Context, req *orgpolicypb.GetPolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetPolicy[0:len((*c.CallOptions).GetPolicy):len((*c.CallOptions).GetPolicy)], opts...)
	var resp *orgpolicypb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetPolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) GetEffectivePolicy(ctx context.Context, req *orgpolicypb.GetEffectivePolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetEffectivePolicy[0:len((*c.CallOptions).GetEffectivePolicy):len((*c.CallOptions).GetEffectivePolicy)], opts...)
	var resp *orgpolicypb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetEffectivePolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) CreatePolicy(ctx context.Context, req *orgpolicypb.CreatePolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).CreatePolicy[0:len((*c.CallOptions).CreatePolicy):len((*c.CallOptions).CreatePolicy)], opts...)
	var resp *orgpolicypb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreatePolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) UpdatePolicy(ctx context.Context, req *orgpolicypb.UpdatePolicyRequest, opts ...gax.CallOption) (*orgpolicypb.Policy, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "policy.name", url.QueryEscape(req.GetPolicy().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).UpdatePolicy[0:len((*c.CallOptions).UpdatePolicy):len((*c.CallOptions).UpdatePolicy)], opts...)
	var resp *orgpolicypb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdatePolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) DeletePolicy(ctx context.Context, req *orgpolicypb.DeletePolicyRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).DeletePolicy[0:len((*c.CallOptions).DeletePolicy):len((*c.CallOptions).DeletePolicy)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeletePolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// ConstraintIterator manages a stream of *orgpolicypb.Constraint.
type ConstraintIterator struct {
	items    []*orgpolicypb.Constraint
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
	InternalFetch func(pageSize int, pageToken string) (results []*orgpolicypb.Constraint, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *ConstraintIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *ConstraintIterator) Next() (*orgpolicypb.Constraint, error) {
	var item *orgpolicypb.Constraint
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ConstraintIterator) bufLen() int {
	return len(it.items)
}

func (it *ConstraintIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// PolicyIterator manages a stream of *orgpolicypb.Policy.
type PolicyIterator struct {
	items    []*orgpolicypb.Policy
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
	InternalFetch func(pageSize int, pageToken string) (results []*orgpolicypb.Policy, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *PolicyIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *PolicyIterator) Next() (*orgpolicypb.Policy, error) {
	var item *orgpolicypb.Policy
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *PolicyIterator) bufLen() int {
	return len(it.items)
}

func (it *PolicyIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
