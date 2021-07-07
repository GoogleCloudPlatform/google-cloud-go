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

package compute

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	httptransport "google.golang.org/api/transport/http"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
)

var newSslPoliciesClientHook clientHook

// SslPoliciesCallOptions contains the retry settings for each method of SslPoliciesClient.
type SslPoliciesCallOptions struct {
	Delete                []gax.CallOption
	Get                   []gax.CallOption
	Insert                []gax.CallOption
	List                  []gax.CallOption
	ListAvailableFeatures []gax.CallOption
	Patch                 []gax.CallOption
}

// internalSslPoliciesClient is an interface that defines the methods availaible from Google Compute Engine API.
type internalSslPoliciesClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	Delete(context.Context, *computepb.DeleteSslPolicyRequest, ...gax.CallOption) (*computepb.Operation, error)
	Get(context.Context, *computepb.GetSslPolicyRequest, ...gax.CallOption) (*computepb.SslPolicy, error)
	Insert(context.Context, *computepb.InsertSslPolicyRequest, ...gax.CallOption) (*computepb.Operation, error)
	List(context.Context, *computepb.ListSslPoliciesRequest, ...gax.CallOption) (*computepb.SslPoliciesList, error)
	ListAvailableFeatures(context.Context, *computepb.ListAvailableFeaturesSslPoliciesRequest, ...gax.CallOption) (*computepb.SslPoliciesListAvailableFeaturesResponse, error)
	Patch(context.Context, *computepb.PatchSslPolicyRequest, ...gax.CallOption) (*computepb.Operation, error)
}

// SslPoliciesClient is a client for interacting with Google Compute Engine API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// The SslPolicies API.
type SslPoliciesClient struct {
	// The internal transport-dependent client.
	internalClient internalSslPoliciesClient

	// The call options for this service.
	CallOptions *SslPoliciesCallOptions
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *SslPoliciesClient) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *SslPoliciesClient) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *SslPoliciesClient) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// Delete deletes the specified SSL policy. The SSL policy resource can be deleted only if it is not in use by any TargetHttpsProxy or TargetSslProxy resources.
func (c *SslPoliciesClient) Delete(ctx context.Context, req *computepb.DeleteSslPolicyRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	return c.internalClient.Delete(ctx, req, opts...)
}

// Get lists all of the ordered rules present in a single specified policy.
func (c *SslPoliciesClient) Get(ctx context.Context, req *computepb.GetSslPolicyRequest, opts ...gax.CallOption) (*computepb.SslPolicy, error) {
	return c.internalClient.Get(ctx, req, opts...)
}

// Insert returns the specified SSL policy resource. Gets a list of available SSL policies by making a list() request.
func (c *SslPoliciesClient) Insert(ctx context.Context, req *computepb.InsertSslPolicyRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	return c.internalClient.Insert(ctx, req, opts...)
}

// List lists all the SSL policies that have been configured for the specified project.
func (c *SslPoliciesClient) List(ctx context.Context, req *computepb.ListSslPoliciesRequest, opts ...gax.CallOption) (*computepb.SslPoliciesList, error) {
	return c.internalClient.List(ctx, req, opts...)
}

// ListAvailableFeatures lists all features that can be specified in the SSL policy when using custom profile.
func (c *SslPoliciesClient) ListAvailableFeatures(ctx context.Context, req *computepb.ListAvailableFeaturesSslPoliciesRequest, opts ...gax.CallOption) (*computepb.SslPoliciesListAvailableFeaturesResponse, error) {
	return c.internalClient.ListAvailableFeatures(ctx, req, opts...)
}

// Patch patches the specified SSL policy with the data included in the request.
func (c *SslPoliciesClient) Patch(ctx context.Context, req *computepb.PatchSslPolicyRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	return c.internalClient.Patch(ctx, req, opts...)
}

// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type sslPoliciesRESTClient struct {
	// The http endpoint to connect to.
	endpoint string

	// The http client.
	httpClient *http.Client

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewSslPoliciesRESTClient creates a new ssl policies rest client.
//
// The SslPolicies API.
func NewSslPoliciesRESTClient(ctx context.Context, opts ...option.ClientOption) (*SslPoliciesClient, error) {
	clientOpts := append(defaultSslPoliciesRESTClientOptions(), opts...)
	httpClient, endpoint, err := httptransport.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, err
	}

	c := &sslPoliciesRESTClient{
		endpoint:   endpoint,
		httpClient: httpClient,
	}
	c.setGoogleClientInfo()

	return &SslPoliciesClient{internalClient: c, CallOptions: &SslPoliciesCallOptions{}}, nil
}

func defaultSslPoliciesRESTClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("https://compute.googleapis.com"),
		internaloption.WithDefaultMTLSEndpoint("https://compute.mtls.googleapis.com"),
		internaloption.WithDefaultAudience("https://compute.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
	}
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *sslPoliciesRESTClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "rest", "UNKNOWN")
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *sslPoliciesRESTClient) Close() error {
	// Replace httpClient with nil to force cleanup.
	c.httpClient = nil
	return nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *sslPoliciesRESTClient) Connection() *grpc.ClientConn {
	return nil
}

// Delete deletes the specified SSL policy. The SSL policy resource can be deleted only if it is not in use by any TargetHttpsProxy or TargetSslProxy resources.
func (c *sslPoliciesRESTClient) Delete(ctx context.Context, req *computepb.DeleteSslPolicyRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/global/sslPolicies/%v", req.GetProject(), req.GetSslPolicy())

	params := url.Values{}
	if req != nil && req.RequestId != nil {
		params.Add("requestId", fmt.Sprintf("%v", req.GetRequestId()))
	}

	baseUrl.RawQuery = params.Encode()

	httpReq, err := http.NewRequest("DELETE", baseUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	// Set the headers
	for k, v := range c.xGoogMetadata {
		httpReq.Header[k] = v
	}
	httpReq.Header["Content-Type"] = []string{"application/json"}

	httpRsp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	if httpRsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(httpRsp.Status)
	}

	buf, err := ioutil.ReadAll(httpRsp.Body)
	if err != nil {
		return nil, err
	}

	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	rsp := &computepb.Operation{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// Get lists all of the ordered rules present in a single specified policy.
func (c *sslPoliciesRESTClient) Get(ctx context.Context, req *computepb.GetSslPolicyRequest, opts ...gax.CallOption) (*computepb.SslPolicy, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/global/sslPolicies/%v", req.GetProject(), req.GetSslPolicy())

	httpReq, err := http.NewRequest("GET", baseUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	// Set the headers
	for k, v := range c.xGoogMetadata {
		httpReq.Header[k] = v
	}
	httpReq.Header["Content-Type"] = []string{"application/json"}

	httpRsp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	if httpRsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(httpRsp.Status)
	}

	buf, err := ioutil.ReadAll(httpRsp.Body)
	if err != nil {
		return nil, err
	}

	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	rsp := &computepb.SslPolicy{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// Insert returns the specified SSL policy resource. Gets a list of available SSL policies by making a list() request.
func (c *sslPoliciesRESTClient) Insert(ctx context.Context, req *computepb.InsertSslPolicyRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	m := protojson.MarshalOptions{AllowPartial: true, EmitUnpopulated: true}
	body := req.GetSslPolicyResource()
	jsonReq, err := m.Marshal(body)
	if err != nil {
		return nil, err
	}

	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/global/sslPolicies", req.GetProject())

	params := url.Values{}
	if req != nil && req.RequestId != nil {
		params.Add("requestId", fmt.Sprintf("%v", req.GetRequestId()))
	}

	baseUrl.RawQuery = params.Encode()

	httpReq, err := http.NewRequest("POST", baseUrl.String(), bytes.NewReader(jsonReq))
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	// Set the headers
	for k, v := range c.xGoogMetadata {
		httpReq.Header[k] = v
	}
	httpReq.Header["Content-Type"] = []string{"application/json"}

	httpRsp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	if httpRsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(httpRsp.Status)
	}

	buf, err := ioutil.ReadAll(httpRsp.Body)
	if err != nil {
		return nil, err
	}

	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	rsp := &computepb.Operation{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// List lists all the SSL policies that have been configured for the specified project.
func (c *sslPoliciesRESTClient) List(ctx context.Context, req *computepb.ListSslPoliciesRequest, opts ...gax.CallOption) (*computepb.SslPoliciesList, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/global/sslPolicies", req.GetProject())

	params := url.Values{}
	if req != nil && req.Filter != nil {
		params.Add("filter", fmt.Sprintf("%v", req.GetFilter()))
	}
	if req != nil && req.MaxResults != nil {
		params.Add("maxResults", fmt.Sprintf("%v", req.GetMaxResults()))
	}
	if req != nil && req.OrderBy != nil {
		params.Add("orderBy", fmt.Sprintf("%v", req.GetOrderBy()))
	}
	if req != nil && req.PageToken != nil {
		params.Add("pageToken", fmt.Sprintf("%v", req.GetPageToken()))
	}
	if req != nil && req.ReturnPartialSuccess != nil {
		params.Add("returnPartialSuccess", fmt.Sprintf("%v", req.GetReturnPartialSuccess()))
	}

	baseUrl.RawQuery = params.Encode()

	httpReq, err := http.NewRequest("GET", baseUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	// Set the headers
	for k, v := range c.xGoogMetadata {
		httpReq.Header[k] = v
	}
	httpReq.Header["Content-Type"] = []string{"application/json"}

	httpRsp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	if httpRsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(httpRsp.Status)
	}

	buf, err := ioutil.ReadAll(httpRsp.Body)
	if err != nil {
		return nil, err
	}

	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	rsp := &computepb.SslPoliciesList{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// ListAvailableFeatures lists all features that can be specified in the SSL policy when using custom profile.
func (c *sslPoliciesRESTClient) ListAvailableFeatures(ctx context.Context, req *computepb.ListAvailableFeaturesSslPoliciesRequest, opts ...gax.CallOption) (*computepb.SslPoliciesListAvailableFeaturesResponse, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/global/sslPolicies/listAvailableFeatures", req.GetProject())

	params := url.Values{}
	if req != nil && req.Filter != nil {
		params.Add("filter", fmt.Sprintf("%v", req.GetFilter()))
	}
	if req != nil && req.MaxResults != nil {
		params.Add("maxResults", fmt.Sprintf("%v", req.GetMaxResults()))
	}
	if req != nil && req.OrderBy != nil {
		params.Add("orderBy", fmt.Sprintf("%v", req.GetOrderBy()))
	}
	if req != nil && req.PageToken != nil {
		params.Add("pageToken", fmt.Sprintf("%v", req.GetPageToken()))
	}
	if req != nil && req.ReturnPartialSuccess != nil {
		params.Add("returnPartialSuccess", fmt.Sprintf("%v", req.GetReturnPartialSuccess()))
	}

	baseUrl.RawQuery = params.Encode()

	httpReq, err := http.NewRequest("GET", baseUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	// Set the headers
	for k, v := range c.xGoogMetadata {
		httpReq.Header[k] = v
	}
	httpReq.Header["Content-Type"] = []string{"application/json"}

	httpRsp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	if httpRsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(httpRsp.Status)
	}

	buf, err := ioutil.ReadAll(httpRsp.Body)
	if err != nil {
		return nil, err
	}

	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	rsp := &computepb.SslPoliciesListAvailableFeaturesResponse{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// Patch patches the specified SSL policy with the data included in the request.
func (c *sslPoliciesRESTClient) Patch(ctx context.Context, req *computepb.PatchSslPolicyRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	m := protojson.MarshalOptions{AllowPartial: true, EmitUnpopulated: true}
	body := req.GetSslPolicyResource()
	jsonReq, err := m.Marshal(body)
	if err != nil {
		return nil, err
	}

	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/global/sslPolicies/%v", req.GetProject(), req.GetSslPolicy())

	params := url.Values{}
	if req != nil && req.RequestId != nil {
		params.Add("requestId", fmt.Sprintf("%v", req.GetRequestId()))
	}

	baseUrl.RawQuery = params.Encode()

	httpReq, err := http.NewRequest("PATCH", baseUrl.String(), bytes.NewReader(jsonReq))
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	// Set the headers
	for k, v := range c.xGoogMetadata {
		httpReq.Header[k] = v
	}
	httpReq.Header["Content-Type"] = []string{"application/json"}

	httpRsp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpRsp.Body.Close()

	if httpRsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(httpRsp.Status)
	}

	buf, err := ioutil.ReadAll(httpRsp.Body)
	if err != nil {
		return nil, err
	}

	unm := protojson.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}
	rsp := &computepb.Operation{}

	return rsp, unm.Unmarshal(buf, rsp)
}
