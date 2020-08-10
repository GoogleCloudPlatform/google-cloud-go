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

package oslogin

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	commonpb "google.golang.org/genproto/googleapis/cloud/oslogin/common"
	osloginpb "google.golang.org/genproto/googleapis/cloud/oslogin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	DeletePosixAccount []gax.CallOption
	DeleteSshPublicKey []gax.CallOption
	GetLoginProfile    []gax.CallOption
	GetSshPublicKey    []gax.CallOption
	ImportSshPublicKey []gax.CallOption
	UpdateSshPublicKey []gax.CallOption
}

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("oslogin.googleapis.com:443"),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithScopes(DefaultAuthScopes()...),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		DeletePosixAccount: []gax.CallOption{
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
		DeleteSshPublicKey: []gax.CallOption{
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
		GetLoginProfile: []gax.CallOption{
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
		GetSshPublicKey: []gax.CallOption{
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
		ImportSshPublicKey: []gax.CallOption{
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
		UpdateSshPublicKey: []gax.CallOption{
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

// Client is a client for interacting with Cloud OS Login API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type Client struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// The gRPC API client.
	client osloginpb.OsLoginServiceClient

	// The call options for this service.
	CallOptions *CallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new os login service client.
//
// Cloud OS Login API
//
// The Cloud OS Login API allows you to manage users and their associated SSH
// public keys for logging into virtual machines on Google Cloud Platform.
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

		client: osloginpb.NewOsLoginServiceClient(connPool),
	}
	c.setGoogleClientInfo()

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

// DeletePosixAccount deletes a POSIX account.
func (c *Client) DeletePosixAccount(ctx context.Context, req *osloginpb.DeletePosixAccountRequest, opts ...gax.CallOption) error {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeletePosixAccount[0:len(c.CallOptions.DeletePosixAccount):len(c.CallOptions.DeletePosixAccount)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeletePosixAccount(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// DeleteSshPublicKey deletes an SSH public key.
func (c *Client) DeleteSshPublicKey(ctx context.Context, req *osloginpb.DeleteSshPublicKeyRequest, opts ...gax.CallOption) error {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.DeleteSshPublicKey[0:len(c.CallOptions.DeleteSshPublicKey):len(c.CallOptions.DeleteSshPublicKey)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// GetLoginProfile retrieves the profile information used for logging in to a virtual machine
// on Google Compute Engine.
func (c *Client) GetLoginProfile(ctx context.Context, req *osloginpb.GetLoginProfileRequest, opts ...gax.CallOption) (*osloginpb.LoginProfile, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetLoginProfile[0:len(c.CallOptions.GetLoginProfile):len(c.CallOptions.GetLoginProfile)], opts...)
	var resp *osloginpb.LoginProfile
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetLoginProfile(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetSshPublicKey retrieves an SSH public key.
func (c *Client) GetSshPublicKey(ctx context.Context, req *osloginpb.GetSshPublicKeyRequest, opts ...gax.CallOption) (*commonpb.SshPublicKey, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.GetSshPublicKey[0:len(c.CallOptions.GetSshPublicKey):len(c.CallOptions.GetSshPublicKey)], opts...)
	var resp *commonpb.SshPublicKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ImportSshPublicKey adds an SSH public key and returns the profile information. Default POSIX
// account information is set when no username and UID exist as part of the
// login profile.
func (c *Client) ImportSshPublicKey(ctx context.Context, req *osloginpb.ImportSshPublicKeyRequest, opts ...gax.CallOption) (*osloginpb.ImportSshPublicKeyResponse, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "parent", url.QueryEscape(req.GetParent())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.ImportSshPublicKey[0:len(c.CallOptions.ImportSshPublicKey):len(c.CallOptions.ImportSshPublicKey)], opts...)
	var resp *osloginpb.ImportSshPublicKeyResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.ImportSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateSshPublicKey updates an SSH public key and returns the profile information. This method
// supports patch semantics.
func (c *Client) UpdateSshPublicKey(ctx context.Context, req *osloginpb.UpdateSshPublicKeyRequest, opts ...gax.CallOption) (*commonpb.SshPublicKey, error) {
	if _, set := ctx.Deadline(); !set {
		cc, cancel := context.WithTimeout(ctx, 10000*time.Millisecond)
		defer cancel()
		ctx = cc
	}
	md := metadata.Pairs("x-goog-request-params", fmt.Sprintf("%s=%v", "name", url.QueryEscape(req.GetName())))
	ctx = insertMetadata(ctx, c.xGoogMetadata, md)
	opts = append(c.CallOptions.UpdateSshPublicKey[0:len(c.CallOptions.UpdateSshPublicKey):len(c.CallOptions.UpdateSshPublicKey)], opts...)
	var resp *commonpb.SshPublicKey
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateSshPublicKey(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
