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

var newNodeTemplatesClientHook clientHook

// NodeTemplatesCallOptions contains the retry settings for each method of NodeTemplatesClient.
type NodeTemplatesCallOptions struct {
	AggregatedList     []gax.CallOption
	Delete             []gax.CallOption
	Get                []gax.CallOption
	GetIamPolicy       []gax.CallOption
	Insert             []gax.CallOption
	List               []gax.CallOption
	SetIamPolicy       []gax.CallOption
	TestIamPermissions []gax.CallOption
}

// internalNodeTemplatesClient is an interface that defines the methods availaible from Google Compute Engine API.
type internalNodeTemplatesClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	AggregatedList(context.Context, *computepb.AggregatedListNodeTemplatesRequest, ...gax.CallOption) (*computepb.NodeTemplateAggregatedList, error)
	Delete(context.Context, *computepb.DeleteNodeTemplateRequest, ...gax.CallOption) (*computepb.Operation, error)
	Get(context.Context, *computepb.GetNodeTemplateRequest, ...gax.CallOption) (*computepb.NodeTemplate, error)
	GetIamPolicy(context.Context, *computepb.GetIamPolicyNodeTemplateRequest, ...gax.CallOption) (*computepb.Policy, error)
	Insert(context.Context, *computepb.InsertNodeTemplateRequest, ...gax.CallOption) (*computepb.Operation, error)
	List(context.Context, *computepb.ListNodeTemplatesRequest, ...gax.CallOption) (*computepb.NodeTemplateList, error)
	SetIamPolicy(context.Context, *computepb.SetIamPolicyNodeTemplateRequest, ...gax.CallOption) (*computepb.Policy, error)
	TestIamPermissions(context.Context, *computepb.TestIamPermissionsNodeTemplateRequest, ...gax.CallOption) (*computepb.TestPermissionsResponse, error)
}

// NodeTemplatesClient is a client for interacting with Google Compute Engine API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// The NodeTemplates API.
type NodeTemplatesClient struct {
	// The internal transport-dependent client.
	internalClient internalNodeTemplatesClient

	// The call options for this service.
	CallOptions *NodeTemplatesCallOptions
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *NodeTemplatesClient) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *NodeTemplatesClient) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *NodeTemplatesClient) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// AggregatedList retrieves an aggregated list of node templates.
func (c *NodeTemplatesClient) AggregatedList(ctx context.Context, req *computepb.AggregatedListNodeTemplatesRequest, opts ...gax.CallOption) (*computepb.NodeTemplateAggregatedList, error) {
	return c.internalClient.AggregatedList(ctx, req, opts...)
}

// Delete deletes the specified NodeTemplate resource.
func (c *NodeTemplatesClient) Delete(ctx context.Context, req *computepb.DeleteNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	return c.internalClient.Delete(ctx, req, opts...)
}

// Get returns the specified node template. Gets a list of available node templates by making a list() request.
func (c *NodeTemplatesClient) Get(ctx context.Context, req *computepb.GetNodeTemplateRequest, opts ...gax.CallOption) (*computepb.NodeTemplate, error) {
	return c.internalClient.Get(ctx, req, opts...)
}

// GetIamPolicy gets the access control policy for a resource. May be empty if no such policy or resource exists.
func (c *NodeTemplatesClient) GetIamPolicy(ctx context.Context, req *computepb.GetIamPolicyNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Policy, error) {
	return c.internalClient.GetIamPolicy(ctx, req, opts...)
}

// Insert creates a NodeTemplate resource in the specified project using the data included in the request.
func (c *NodeTemplatesClient) Insert(ctx context.Context, req *computepb.InsertNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	return c.internalClient.Insert(ctx, req, opts...)
}

// List retrieves a list of node templates available to the specified project.
func (c *NodeTemplatesClient) List(ctx context.Context, req *computepb.ListNodeTemplatesRequest, opts ...gax.CallOption) (*computepb.NodeTemplateList, error) {
	return c.internalClient.List(ctx, req, opts...)
}

// SetIamPolicy sets the access control policy on the specified resource. Replaces any existing policy.
func (c *NodeTemplatesClient) SetIamPolicy(ctx context.Context, req *computepb.SetIamPolicyNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Policy, error) {
	return c.internalClient.SetIamPolicy(ctx, req, opts...)
}

// TestIamPermissions returns permissions that a caller has on the specified resource.
func (c *NodeTemplatesClient) TestIamPermissions(ctx context.Context, req *computepb.TestIamPermissionsNodeTemplateRequest, opts ...gax.CallOption) (*computepb.TestPermissionsResponse, error) {
	return c.internalClient.TestIamPermissions(ctx, req, opts...)
}

// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type nodeTemplatesRESTClient struct {
	// The http endpoint to connect to.
	endpoint string

	// The http client.
	httpClient *http.Client

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewNodeTemplatesRESTClient creates a new node templates rest client.
//
// The NodeTemplates API.
func NewNodeTemplatesRESTClient(ctx context.Context, opts ...option.ClientOption) (*NodeTemplatesClient, error) {
	clientOpts := append(defaultNodeTemplatesRESTClientOptions(), opts...)
	httpClient, endpoint, err := httptransport.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, err
	}

	c := &nodeTemplatesRESTClient{
		endpoint:   endpoint,
		httpClient: httpClient,
	}
	c.setGoogleClientInfo()

	return &NodeTemplatesClient{internalClient: c, CallOptions: &NodeTemplatesCallOptions{}}, nil
}

func defaultNodeTemplatesRESTClientOptions() []option.ClientOption {
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
func (c *nodeTemplatesRESTClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "rest", "UNKNOWN")
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *nodeTemplatesRESTClient) Close() error {
	// Replace httpClient with nil to force cleanup.
	c.httpClient = nil
	return nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *nodeTemplatesRESTClient) Connection() *grpc.ClientConn {
	return nil
}

// AggregatedList retrieves an aggregated list of node templates.
func (c *nodeTemplatesRESTClient) AggregatedList(ctx context.Context, req *computepb.AggregatedListNodeTemplatesRequest, opts ...gax.CallOption) (*computepb.NodeTemplateAggregatedList, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/aggregated/nodeTemplates", req.GetProject())

	params := url.Values{}
	if req != nil && req.Filter != nil {
		params.Add("filter", fmt.Sprintf("%v", req.GetFilter()))
	}
	if req != nil && req.IncludeAllScopes != nil {
		params.Add("includeAllScopes", fmt.Sprintf("%v", req.GetIncludeAllScopes()))
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
	rsp := &computepb.NodeTemplateAggregatedList{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// Delete deletes the specified NodeTemplate resource.
func (c *nodeTemplatesRESTClient) Delete(ctx context.Context, req *computepb.DeleteNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/regions/%v/nodeTemplates/%v", req.GetProject(), req.GetRegion(), req.GetNodeTemplate())

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

// Get returns the specified node template. Gets a list of available node templates by making a list() request.
func (c *nodeTemplatesRESTClient) Get(ctx context.Context, req *computepb.GetNodeTemplateRequest, opts ...gax.CallOption) (*computepb.NodeTemplate, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/regions/%v/nodeTemplates/%v", req.GetProject(), req.GetRegion(), req.GetNodeTemplate())

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
	rsp := &computepb.NodeTemplate{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// GetIamPolicy gets the access control policy for a resource. May be empty if no such policy or resource exists.
func (c *nodeTemplatesRESTClient) GetIamPolicy(ctx context.Context, req *computepb.GetIamPolicyNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Policy, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/regions/%v/nodeTemplates/%v/getIamPolicy", req.GetProject(), req.GetRegion(), req.GetResource())

	params := url.Values{}
	if req != nil && req.OptionsRequestedPolicyVersion != nil {
		params.Add("optionsRequestedPolicyVersion", fmt.Sprintf("%v", req.GetOptionsRequestedPolicyVersion()))
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
	rsp := &computepb.Policy{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// Insert creates a NodeTemplate resource in the specified project using the data included in the request.
func (c *nodeTemplatesRESTClient) Insert(ctx context.Context, req *computepb.InsertNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Operation, error) {
	m := protojson.MarshalOptions{AllowPartial: true, EmitUnpopulated: true}
	body := req.GetNodeTemplateResource()
	jsonReq, err := m.Marshal(body)
	if err != nil {
		return nil, err
	}

	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/regions/%v/nodeTemplates", req.GetProject(), req.GetRegion())

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

// List retrieves a list of node templates available to the specified project.
func (c *nodeTemplatesRESTClient) List(ctx context.Context, req *computepb.ListNodeTemplatesRequest, opts ...gax.CallOption) (*computepb.NodeTemplateList, error) {
	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/regions/%v/nodeTemplates", req.GetProject(), req.GetRegion())

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
	rsp := &computepb.NodeTemplateList{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// SetIamPolicy sets the access control policy on the specified resource. Replaces any existing policy.
func (c *nodeTemplatesRESTClient) SetIamPolicy(ctx context.Context, req *computepb.SetIamPolicyNodeTemplateRequest, opts ...gax.CallOption) (*computepb.Policy, error) {
	m := protojson.MarshalOptions{AllowPartial: true, EmitUnpopulated: true}
	body := req.GetRegionSetPolicyRequestResource()
	jsonReq, err := m.Marshal(body)
	if err != nil {
		return nil, err
	}

	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/regions/%v/nodeTemplates/%v/setIamPolicy", req.GetProject(), req.GetRegion(), req.GetResource())

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
	rsp := &computepb.Policy{}

	return rsp, unm.Unmarshal(buf, rsp)
}

// TestIamPermissions returns permissions that a caller has on the specified resource.
func (c *nodeTemplatesRESTClient) TestIamPermissions(ctx context.Context, req *computepb.TestIamPermissionsNodeTemplateRequest, opts ...gax.CallOption) (*computepb.TestPermissionsResponse, error) {
	m := protojson.MarshalOptions{AllowPartial: true, EmitUnpopulated: true}
	body := req.GetTestPermissionsRequestResource()
	jsonReq, err := m.Marshal(body)
	if err != nil {
		return nil, err
	}

	baseUrl, _ := url.Parse(c.endpoint)
	baseUrl.Path += fmt.Sprintf("/compute/v1/projects/%v/regions/%v/nodeTemplates/%v/testIamPermissions", req.GetProject(), req.GetRegion(), req.GetResource())

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
	rsp := &computepb.TestPermissionsResponse{}

	return rsp, unm.Unmarshal(buf, rsp)
}
