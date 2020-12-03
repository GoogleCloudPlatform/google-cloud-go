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

package dashboard

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
	dashboardpb "google.golang.org/genproto/googleapis/monitoring/dashboard/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newDashboardsClientHook clientHook

// DashboardsCallOptions contains the retry settings for each method of DashboardsClient.
type DashboardsCallOptions struct {
	CreateDashboard []gax.CallOption
	ListDashboards  []gax.CallOption
	GetDashboard    []gax.CallOption
	DeleteDashboard []gax.CallOption
	UpdateDashboard []gax.CallOption
}

func defaultDashboardsClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("monitoring.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("monitoring.mtls.googleapis.com:443"),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultDashboardsCallOptions() *DashboardsCallOptions {
	return &DashboardsCallOptions{
		CreateDashboard: []gax.CallOption{},
		ListDashboards: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetDashboard: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        10000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteDashboard: []gax.CallOption{},
		UpdateDashboard: []gax.CallOption{},
	}
}

// DashboardsClient is a client for interacting with .
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type DashboardsClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// The gRPC API client.
	dashboardsClient dashboardpb.DashboardsServiceClient

	// The call options for this service.
	CallOptions *DashboardsCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewDashboardsClient creates a new dashboards service client.
//
// Manages Stackdriver dashboards. A dashboard is an arrangement of data display
// widgets in a specific layout.
func NewDashboardsClient(ctx context.Context, opts ...option.ClientOption) (*DashboardsClient, error) {
	clientOpts := defaultDashboardsClientOptions()

	if newDashboardsClientHook != nil {
		hookOpts, err := newDashboardsClientHook(ctx, clientHookParams{})
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
	c := &DashboardsClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		CallOptions:      defaultDashboardsCallOptions(),

		dashboardsClient: dashboardpb.NewDashboardsServiceClient(connPool),
	}
	c.setGoogleClientInfo()

	return c, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *DashboardsClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *DashboardsClient) Close() error {
	return c.connPool.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *DashboardsClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// CreateDashboard creates a new custom dashboard.
//
// This method requires the monitoring.dashboards.create permission
// on the specified project. For more information, see
// Google Cloud IAM (at https://cloud.google.com/iam).
func (c *DashboardsClient) CreateDashboard(ctx context.Context, req *dashboardpb.CreateDashboardRequest, opts ...gax.CallOption) (*dashboardpb.Dashboard, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 30000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateDashboard[0:len(c.CallOptions.CreateDashboard):len(c.CallOptions.CreateDashboard)], opts...)
	var resp *dashboardpb.Dashboard
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.dashboardsClient.CreateDashboard(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListDashboards lists the existing dashboards.
//
// This method requires the monitoring.dashboards.list permission
// on the specified project. For more information, see
// Google Cloud IAM (at https://cloud.google.com/iam).
func (c *DashboardsClient) ListDashboards(ctx context.Context, req *dashboardpb.ListDashboardsRequest, opts ...gax.CallOption) *DashboardIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListDashboards[0:len(c.CallOptions.ListDashboards):len(c.CallOptions.ListDashboards)], opts...)
	it := &DashboardIterator{}
	req = proto.Clone(req).(*dashboardpb.ListDashboardsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dashboardpb.Dashboard, string, error) {
		var resp *dashboardpb.ListDashboardsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.dashboardsClient.ListDashboards(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetDashboards(), resp.GetNextPageToken(), nil
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

// GetDashboard fetches a specific dashboard.
//
// This method requires the monitoring.dashboards.get permission
// on the specified dashboard. For more information, see
// Google Cloud IAM (at https://cloud.google.com/iam).
func (c *DashboardsClient) GetDashboard(ctx context.Context, req *dashboardpb.GetDashboardRequest, opts ...gax.CallOption) (*dashboardpb.Dashboard, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 30000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetDashboard[0:len(c.CallOptions.GetDashboard):len(c.CallOptions.GetDashboard)], opts...)
	var resp *dashboardpb.Dashboard
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.dashboardsClient.GetDashboard(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteDashboard deletes an existing custom dashboard.
//
// This method requires the monitoring.dashboards.delete permission
// on the specified dashboard. For more information, see
// Google Cloud IAM (at https://cloud.google.com/iam).
func (c *DashboardsClient) DeleteDashboard(ctx context.Context, req *dashboardpb.DeleteDashboardRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 30000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteDashboard[0:len(c.CallOptions.DeleteDashboard):len(c.CallOptions.DeleteDashboard)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.dashboardsClient.DeleteDashboard(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// UpdateDashboard replaces an existing custom dashboard with a new definition.
//
// This method requires the monitoring.dashboards.update permission
// on the specified dashboard. For more information, see
// Google Cloud IAM (at https://cloud.google.com/iam).
func (c *DashboardsClient) UpdateDashboard(ctx context.Context, req *dashboardpb.UpdateDashboardRequest, opts ...gax.CallOption) (*dashboardpb.Dashboard, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 30000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "dashboard.name", url.QueryEscape(req.GetDashboard().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateDashboard[0:len(c.CallOptions.UpdateDashboard):len(c.CallOptions.UpdateDashboard)], opts...)
	var resp *dashboardpb.Dashboard
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.dashboardsClient.UpdateDashboard(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DashboardIterator manages a stream of *dashboardpb.Dashboard.
type DashboardIterator struct {
	items    []*dashboardpb.Dashboard
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
	InternalFetch func(pageSize int, pageToken string) (results []*dashboardpb.Dashboard, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *DashboardIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *DashboardIterator) Next() (*dashboardpb.Dashboard, error) {
	var item *dashboardpb.Dashboard
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *DashboardIterator) bufLen() int {
	return len(it.items)
}

func (it *DashboardIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
