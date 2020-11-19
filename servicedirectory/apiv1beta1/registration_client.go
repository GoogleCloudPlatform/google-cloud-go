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

package servicedirectory

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
	servicedirectorypb "google.golang.org/genproto/googleapis/cloud/servicedirectory/v1beta1"
	iampb "google.golang.org/genproto/googleapis/iam/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newRegistrationClientHook clientHook

// RegistrationCallOptions contains the retry settings for each method of RegistrationClient.
type RegistrationCallOptions struct {
	CreateNamespace    []gax.CallOption
	ListNamespaces     []gax.CallOption
	GetNamespace       []gax.CallOption
	UpdateNamespace    []gax.CallOption
	DeleteNamespace    []gax.CallOption
	CreateService      []gax.CallOption
	ListServices       []gax.CallOption
	GetService         []gax.CallOption
	UpdateService      []gax.CallOption
	DeleteService      []gax.CallOption
	CreateEndpoint     []gax.CallOption
	ListEndpoints      []gax.CallOption
	GetEndpoint        []gax.CallOption
	UpdateEndpoint     []gax.CallOption
	DeleteEndpoint     []gax.CallOption
	GetIamPolicy       []gax.CallOption
	SetIamPolicy       []gax.CallOption
	TestIamPermissions []gax.CallOption
}

func defaultRegistrationClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("servicedirectory.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("servicedirectory.mtls.googleapis.com:443"),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultRegistrationCallOptions() *RegistrationCallOptions {
	return &RegistrationCallOptions{
		CreateNamespace: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListNamespaces: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetNamespace: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		UpdateNamespace: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteNamespace: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CreateService: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListServices: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetService: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		UpdateService: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteService: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		CreateEndpoint: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		ListEndpoints: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetEndpoint: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		UpdateEndpoint: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		DeleteEndpoint: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		GetIamPolicy: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		SetIamPolicy: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
		TestIamPermissions: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Unavailable,
					codes.Unknown,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.30,
				})
			}),
		},
	}
}

// RegistrationClient is a client for interacting with Service Directory API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type RegistrationClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// The gRPC API client.
	registrationClient servicedirectorypb.RegistrationServiceClient

	// The call options for this service.
	CallOptions *RegistrationCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewRegistrationClient creates a new registration service client.
//
// Service Directory API for registering services. It defines the following
// resource model:
//
//   The API has a collection of
//   Namespace
//   resources, named projects/*/locations/*/namespaces/*.
//
//   Each Namespace has a collection of
//   Service resources, named
//   projects/*/locations/*/namespaces/*/services/*.
//
//   Each Service has a collection of
//   Endpoint
//   resources, named
//   projects/*/locations/*/namespaces/*/services/*/endpoints/*.
func NewRegistrationClient(ctx context.Context, opts ...option.ClientOption) (*RegistrationClient, error) {
	clientOpts := defaultRegistrationClientOptions()

	if newRegistrationClientHook != nil {
		hookOpts, err := newRegistrationClientHook(ctx, clientHookParams{})
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
	c := &RegistrationClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		CallOptions:      defaultRegistrationCallOptions(),

		registrationClient: servicedirectorypb.NewRegistrationServiceClient(connPool),
	}
	c.setGoogleClientInfo()

	return c, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *RegistrationClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *RegistrationClient) Close() error {
	return c.connPool.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *RegistrationClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// CreateNamespace creates a namespace, and returns the new Namespace.
func (c *RegistrationClient) CreateNamespace(ctx context.Context, req *servicedirectorypb.CreateNamespaceRequest, opts ...gax.CallOption) (*servicedirectorypb.Namespace, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateNamespace[0:len(c.CallOptions.CreateNamespace):len(c.CallOptions.CreateNamespace)], opts...)
	var resp *servicedirectorypb.Namespace
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.CreateNamespace(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListNamespaces lists all namespaces.
func (c *RegistrationClient) ListNamespaces(ctx context.Context, req *servicedirectorypb.ListNamespacesRequest, opts ...gax.CallOption) *NamespaceIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListNamespaces[0:len(c.CallOptions.ListNamespaces):len(c.CallOptions.ListNamespaces)], opts...)
	it := &NamespaceIterator{}
	req = proto.Clone(req).(*servicedirectorypb.ListNamespacesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*servicedirectorypb.Namespace, string, error) {
		var resp *servicedirectorypb.ListNamespacesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.registrationClient.ListNamespaces(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetNamespaces(), resp.GetNextPageToken(), nil
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

// GetNamespace gets a namespace.
func (c *RegistrationClient) GetNamespace(ctx context.Context, req *servicedirectorypb.GetNamespaceRequest, opts ...gax.CallOption) (*servicedirectorypb.Namespace, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetNamespace[0:len(c.CallOptions.GetNamespace):len(c.CallOptions.GetNamespace)], opts...)
	var resp *servicedirectorypb.Namespace
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.GetNamespace(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateNamespace updates a namespace.
func (c *RegistrationClient) UpdateNamespace(ctx context.Context, req *servicedirectorypb.UpdateNamespaceRequest, opts ...gax.CallOption) (*servicedirectorypb.Namespace, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "namespace.name", url.QueryEscape(req.GetNamespace().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateNamespace[0:len(c.CallOptions.UpdateNamespace):len(c.CallOptions.UpdateNamespace)], opts...)
	var resp *servicedirectorypb.Namespace
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.UpdateNamespace(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteNamespace deletes a namespace. This also deletes all services and endpoints in
// the namespace.
func (c *RegistrationClient) DeleteNamespace(ctx context.Context, req *servicedirectorypb.DeleteNamespaceRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteNamespace[0:len(c.CallOptions.DeleteNamespace):len(c.CallOptions.DeleteNamespace)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.registrationClient.DeleteNamespace(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// CreateService creates a service, and returns the new Service.
func (c *RegistrationClient) CreateService(ctx context.Context, req *servicedirectorypb.CreateServiceRequest, opts ...gax.CallOption) (*servicedirectorypb.Service, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateService[0:len(c.CallOptions.CreateService):len(c.CallOptions.CreateService)], opts...)
	var resp *servicedirectorypb.Service
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.CreateService(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListServices lists all services belonging to a namespace.
func (c *RegistrationClient) ListServices(ctx context.Context, req *servicedirectorypb.ListServicesRequest, opts ...gax.CallOption) *ServiceIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListServices[0:len(c.CallOptions.ListServices):len(c.CallOptions.ListServices)], opts...)
	it := &ServiceIterator{}
	req = proto.Clone(req).(*servicedirectorypb.ListServicesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*servicedirectorypb.Service, string, error) {
		var resp *servicedirectorypb.ListServicesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.registrationClient.ListServices(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetServices(), resp.GetNextPageToken(), nil
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

// GetService gets a service.
func (c *RegistrationClient) GetService(ctx context.Context, req *servicedirectorypb.GetServiceRequest, opts ...gax.CallOption) (*servicedirectorypb.Service, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetService[0:len(c.CallOptions.GetService):len(c.CallOptions.GetService)], opts...)
	var resp *servicedirectorypb.Service
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.GetService(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateService updates a service.
func (c *RegistrationClient) UpdateService(ctx context.Context, req *servicedirectorypb.UpdateServiceRequest, opts ...gax.CallOption) (*servicedirectorypb.Service, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "service.name", url.QueryEscape(req.GetService().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateService[0:len(c.CallOptions.UpdateService):len(c.CallOptions.UpdateService)], opts...)
	var resp *servicedirectorypb.Service
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.UpdateService(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteService deletes a service. This also deletes all endpoints associated with
// the service.
func (c *RegistrationClient) DeleteService(ctx context.Context, req *servicedirectorypb.DeleteServiceRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteService[0:len(c.CallOptions.DeleteService):len(c.CallOptions.DeleteService)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.registrationClient.DeleteService(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// CreateEndpoint creates a endpoint, and returns the new Endpoint.
func (c *RegistrationClient) CreateEndpoint(ctx context.Context, req *servicedirectorypb.CreateEndpointRequest, opts ...gax.CallOption) (*servicedirectorypb.Endpoint, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.CreateEndpoint[0:len(c.CallOptions.CreateEndpoint):len(c.CallOptions.CreateEndpoint)], opts...)
	var resp *servicedirectorypb.Endpoint
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.CreateEndpoint(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListEndpoints lists all endpoints.
func (c *RegistrationClient) ListEndpoints(ctx context.Context, req *servicedirectorypb.ListEndpointsRequest, opts ...gax.CallOption) *EndpointIterator {
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ListEndpoints[0:len(c.CallOptions.ListEndpoints):len(c.CallOptions.ListEndpoints)], opts...)
	it := &EndpointIterator{}
	req = proto.Clone(req).(*servicedirectorypb.ListEndpointsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*servicedirectorypb.Endpoint, string, error) {
		var resp *servicedirectorypb.ListEndpointsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.registrationClient.ListEndpoints(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}

		it.Response = resp
		return resp.GetEndpoints(), resp.GetNextPageToken(), nil
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

// GetEndpoint gets a endpoint.
func (c *RegistrationClient) GetEndpoint(ctx context.Context, req *servicedirectorypb.GetEndpointRequest, opts ...gax.CallOption) (*servicedirectorypb.Endpoint, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetEndpoint[0:len(c.CallOptions.GetEndpoint):len(c.CallOptions.GetEndpoint)], opts...)
	var resp *servicedirectorypb.Endpoint
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.GetEndpoint(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateEndpoint updates a endpoint.
func (c *RegistrationClient) UpdateEndpoint(ctx context.Context, req *servicedirectorypb.UpdateEndpointRequest, opts ...gax.CallOption) (*servicedirectorypb.Endpoint, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "endpoint.name", url.QueryEscape(req.GetEndpoint().GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateEndpoint[0:len(c.CallOptions.UpdateEndpoint):len(c.CallOptions.UpdateEndpoint)], opts...)
	var resp *servicedirectorypb.Endpoint
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.UpdateEndpoint(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteEndpoint deletes a endpoint.
func (c *RegistrationClient) DeleteEndpoint(ctx context.Context, req *servicedirectorypb.DeleteEndpointRequest, opts ...gax.CallOption) error {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteEndpoint[0:len(c.CallOptions.DeleteEndpoint):len(c.CallOptions.DeleteEndpoint)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.registrationClient.DeleteEndpoint(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// GetIamPolicy gets the IAM Policy for a resource (namespace or service only).
func (c *RegistrationClient) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest, opts ...gax.CallOption) (*iampb.Policy, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetIamPolicy[0:len(c.CallOptions.GetIamPolicy):len(c.CallOptions.GetIamPolicy)], opts...)
	var resp *iampb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.GetIamPolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// SetIamPolicy sets the IAM Policy for a resource (namespace or service only).
func (c *RegistrationClient) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest, opts ...gax.CallOption) (*iampb.Policy, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.SetIamPolicy[0:len(c.CallOptions.SetIamPolicy):len(c.CallOptions.SetIamPolicy)], opts...)
	var resp *iampb.Policy
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.SetIamPolicy(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// TestIamPermissions tests IAM permissions for a resource (namespace or service only).
func (c *RegistrationClient) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest, opts ...gax.CallOption) (*iampb.TestIamPermissionsResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 15000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "resource", url.QueryEscape(req.GetResource())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.TestIamPermissions[0:len(c.CallOptions.TestIamPermissions):len(c.CallOptions.TestIamPermissions)], opts...)
	var resp *iampb.TestIamPermissionsResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.registrationClient.TestIamPermissions(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// EndpointIterator manages a stream of *servicedirectorypb.Endpoint.
type EndpointIterator struct {
	items    []*servicedirectorypb.Endpoint
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
	InternalFetch func(pageSize int, pageToken string) (results []*servicedirectorypb.Endpoint, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *EndpointIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *EndpointIterator) Next() (*servicedirectorypb.Endpoint, error) {
	var item *servicedirectorypb.Endpoint
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *EndpointIterator) bufLen() int {
	return len(it.items)
}

func (it *EndpointIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// NamespaceIterator manages a stream of *servicedirectorypb.Namespace.
type NamespaceIterator struct {
	items    []*servicedirectorypb.Namespace
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
	InternalFetch func(pageSize int, pageToken string) (results []*servicedirectorypb.Namespace, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *NamespaceIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *NamespaceIterator) Next() (*servicedirectorypb.Namespace, error) {
	var item *servicedirectorypb.Namespace
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *NamespaceIterator) bufLen() int {
	return len(it.items)
}

func (it *NamespaceIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// ServiceIterator manages a stream of *servicedirectorypb.Service.
type ServiceIterator struct {
	items    []*servicedirectorypb.Service
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
	InternalFetch func(pageSize int, pageToken string) (results []*servicedirectorypb.Service, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *ServiceIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *ServiceIterator) Next() (*servicedirectorypb.Service, error) {
	var item *servicedirectorypb.Service
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ServiceIterator) bufLen() int {
	return len(it.items)
}

func (it *ServiceIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
