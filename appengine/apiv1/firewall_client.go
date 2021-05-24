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

package appengine

import (
	"context"
	"fmt"
	"math"
	"net/url"

	"github.com/golang/protobuf/proto"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	appenginepb "google.golang.org/genproto/googleapis/appengine/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var newFirewallClientHook clientHook

// FirewallCallOptions contains the retry settings for each method of FirewallClient.
type FirewallCallOptions struct {
	ListIngressRules        []gax.CallOption
	BatchUpdateIngressRules []gax.CallOption
	CreateIngressRule       []gax.CallOption
	GetIngressRule          []gax.CallOption
	UpdateIngressRule       []gax.CallOption
	DeleteIngressRule       []gax.CallOption
}

func defaultFirewallGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("appengine.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("appengine.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://appengine.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultFirewallCallOptions() *FirewallCallOptions {
	return &FirewallCallOptions{
		ListIngressRules:        []gax.CallOption{},
		BatchUpdateIngressRules: []gax.CallOption{},
		CreateIngressRule:       []gax.CallOption{},
		GetIngressRule:          []gax.CallOption{},
		UpdateIngressRule:       []gax.CallOption{},
		DeleteIngressRule:       []gax.CallOption{},
	}
}

// internalFirewallClient is an interface that defines the methods availaible from App Engine Admin API.
type internalFirewallClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	ListIngressRules(context.Context, *appenginepb.ListIngressRulesRequest, ...gax.CallOption) *FirewallRuleIterator
	BatchUpdateIngressRules(context.Context, *appenginepb.BatchUpdateIngressRulesRequest, ...gax.CallOption) (*appenginepb.BatchUpdateIngressRulesResponse, error)
	CreateIngressRule(context.Context, *appenginepb.CreateIngressRuleRequest, ...gax.CallOption) (*appenginepb.FirewallRule, error)
	GetIngressRule(context.Context, *appenginepb.GetIngressRuleRequest, ...gax.CallOption) (*appenginepb.FirewallRule, error)
	UpdateIngressRule(context.Context, *appenginepb.UpdateIngressRuleRequest, ...gax.CallOption) (*appenginepb.FirewallRule, error)
	DeleteIngressRule(context.Context, *appenginepb.DeleteIngressRuleRequest, ...gax.CallOption) error
}

// FirewallClient is a client for interacting with App Engine Admin API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// Firewall resources are used to define a collection of access control rules
// for an Application. Each rule is defined with a position which specifies
// the rule’s order in the sequence of rules, an IP range to be matched against
// requests, and an action to take upon matching requests.
//
// Every request is evaluated against the Firewall rules in priority order.
// Processesing stops at the first rule which matches the request’s IP address.
// A final rule always specifies an action that applies to all remaining
// IP addresses. The default final rule for a newly-created application will be
// set to “allow” if not otherwise specified by the user.
type FirewallClient struct {
	// The internal transport-dependent client.
	internalClient internalFirewallClient

	// The call options for this service.
	CallOptions *FirewallCallOptions
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *FirewallClient) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *FirewallClient) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *FirewallClient) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// ListIngressRules lists the firewall rules of an application.
func (c *FirewallClient) ListIngressRules(ctx context.Context, req *appenginepb.ListIngressRulesRequest, opts ...gax.CallOption) *FirewallRuleIterator {
	return c.internalClient.ListIngressRules(ctx, req, opts...)
}

// BatchUpdateIngressRules replaces the entire firewall ruleset in one bulk operation. This overrides
// and replaces the rules of an existing firewall with the new rules.
//
// If the final rule does not match traffic with the ‘*’ wildcard IP range,
// then an “allow all” rule is explicitly added to the end of the list.
func (c *FirewallClient) BatchUpdateIngressRules(ctx context.Context, req *appenginepb.BatchUpdateIngressRulesRequest, opts ...gax.CallOption) (*appenginepb.BatchUpdateIngressRulesResponse, error) {
	return c.internalClient.BatchUpdateIngressRules(ctx, req, opts...)
}

// CreateIngressRule creates a firewall rule for the application.
func (c *FirewallClient) CreateIngressRule(ctx context.Context, req *appenginepb.CreateIngressRuleRequest, opts ...gax.CallOption) (*appenginepb.FirewallRule, error) {
	return c.internalClient.CreateIngressRule(ctx, req, opts...)
}

// GetIngressRule gets the specified firewall rule.
func (c *FirewallClient) GetIngressRule(ctx context.Context, req *appenginepb.GetIngressRuleRequest, opts ...gax.CallOption) (*appenginepb.FirewallRule, error) {
	return c.internalClient.GetIngressRule(ctx, req, opts...)
}

// UpdateIngressRule updates the specified firewall rule.
func (c *FirewallClient) UpdateIngressRule(ctx context.Context, req *appenginepb.UpdateIngressRuleRequest, opts ...gax.CallOption) (*appenginepb.FirewallRule, error) {
	return c.internalClient.UpdateIngressRule(ctx, req, opts...)
}

// DeleteIngressRule deletes the specified firewall rule.
func (c *FirewallClient) DeleteIngressRule(ctx context.Context, req *appenginepb.DeleteIngressRuleRequest, opts ...gax.CallOption) error {
	return c.internalClient.DeleteIngressRule(ctx, req, opts...)
}

// firewallGRPCClient is a client for interacting with App Engine Admin API over gRPC transport.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type firewallGRPCClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// Points back to the CallOptions field of the containing FirewallClient
	CallOptions **FirewallCallOptions

	// The gRPC API client.
	firewallClient appenginepb.FirewallClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewFirewallClient creates a new firewall client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// Firewall resources are used to define a collection of access control rules
// for an Application. Each rule is defined with a position which specifies
// the rule’s order in the sequence of rules, an IP range to be matched against
// requests, and an action to take upon matching requests.
//
// Every request is evaluated against the Firewall rules in priority order.
// Processesing stops at the first rule which matches the request’s IP address.
// A final rule always specifies an action that applies to all remaining
// IP addresses. The default final rule for a newly-created application will be
// set to “allow” if not otherwise specified by the user.
func NewFirewallClient(ctx context.Context, opts ...option.ClientOption) (*FirewallClient, error) {
	clientOpts := defaultFirewallGRPCClientOptions()
	if newFirewallClientHook != nil {
		hookOpts, err := newFirewallClientHook(ctx, clientHookParams{})
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
	client := FirewallClient{CallOptions: defaultFirewallCallOptions()}

	c := &firewallGRPCClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		firewallClient:   appenginepb.NewFirewallClient(connPool),
		CallOptions:      &client.CallOptions,
	}
	c.setGoogleClientInfo()

	client.internalClient = c

	return &client, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *firewallGRPCClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *firewallGRPCClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *firewallGRPCClient) Close() error {
	return c.connPool.Close()
}

func (c *firewallGRPCClient) ListIngressRules(ctx context.Context, req *appenginepb.ListIngressRulesRequest, opts ...gax.CallOption) *FirewallRuleIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).ListIngressRules[0:len((*c.CallOptions).ListIngressRules):len((*c.CallOptions).ListIngressRules)], opts...)
	it := &FirewallRuleIterator{}
	req = proto.Clone(req).(*appenginepb.ListIngressRulesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*appenginepb.FirewallRule, string, error) {
		var resp *appenginepb.ListIngressRulesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.firewallClient.ListIngressRules(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetIngressRules(), resp.GetNextPageToken(), nil
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

func (c *firewallGRPCClient) BatchUpdateIngressRules(ctx context.Context, req *appenginepb.BatchUpdateIngressRulesRequest, opts ...gax.CallOption) (*appenginepb.BatchUpdateIngressRulesResponse, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).BatchUpdateIngressRules[0:len((*c.CallOptions).BatchUpdateIngressRules):len((*c.CallOptions).BatchUpdateIngressRules)], opts...)
	var resp *appenginepb.BatchUpdateIngressRulesResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.firewallClient.BatchUpdateIngressRules(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *firewallGRPCClient) CreateIngressRule(ctx context.Context, req *appenginepb.CreateIngressRuleRequest, opts ...gax.CallOption) (*appenginepb.FirewallRule, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).CreateIngressRule[0:len((*c.CallOptions).CreateIngressRule):len((*c.CallOptions).CreateIngressRule)], opts...)
	var resp *appenginepb.FirewallRule
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.firewallClient.CreateIngressRule(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *firewallGRPCClient) GetIngressRule(ctx context.Context, req *appenginepb.GetIngressRuleRequest, opts ...gax.CallOption) (*appenginepb.FirewallRule, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).GetIngressRule[0:len((*c.CallOptions).GetIngressRule):len((*c.CallOptions).GetIngressRule)], opts...)
	var resp *appenginepb.FirewallRule
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.firewallClient.GetIngressRule(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *firewallGRPCClient) UpdateIngressRule(ctx context.Context, req *appenginepb.UpdateIngressRuleRequest, opts ...gax.CallOption) (*appenginepb.FirewallRule, error) {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).UpdateIngressRule[0:len((*c.CallOptions).UpdateIngressRule):len((*c.CallOptions).UpdateIngressRule)], opts...)
	var resp *appenginepb.FirewallRule
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.firewallClient.UpdateIngressRule(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *firewallGRPCClient) DeleteIngressRule(ctx context.Context, req *appenginepb.DeleteIngressRuleRequest, opts ...gax.CallOption) error {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append((*c.CallOptions).DeleteIngressRule[0:len((*c.CallOptions).DeleteIngressRule):len((*c.CallOptions).DeleteIngressRule)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.firewallClient.DeleteIngressRule(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// FirewallRuleIterator manages a stream of *appenginepb.FirewallRule.
type FirewallRuleIterator struct {
	items    []*appenginepb.FirewallRule
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
	InternalFetch func(pageSize int, pageToken string) (results []*appenginepb.FirewallRule, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *FirewallRuleIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *FirewallRuleIterator) Next() (*appenginepb.FirewallRule, error) {
	var item *appenginepb.FirewallRule
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *FirewallRuleIterator) bufLen() int {
	return len(it.items)
}

func (it *FirewallRuleIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
