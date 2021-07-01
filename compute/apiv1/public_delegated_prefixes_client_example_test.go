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

package compute_test

import (
	"context"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
)

func ExampleNewPublicDelegatedPrefixesRESTClient() {
	ctx := context.Background()
	c, err := compute.NewPublicDelegatedPrefixesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExamplePublicDelegatedPrefixesClient_AggregatedList() {
	ctx := context.Background()
	c, err := compute.NewPublicDelegatedPrefixesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &computepb.AggregatedListPublicDelegatedPrefixesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.AggregatedList(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExamplePublicDelegatedPrefixesClient_Delete() {
	ctx := context.Background()
	c, err := compute.NewPublicDelegatedPrefixesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &computepb.DeletePublicDelegatedPrefixeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Delete(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExamplePublicDelegatedPrefixesClient_Get() {
	ctx := context.Background()
	c, err := compute.NewPublicDelegatedPrefixesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &computepb.GetPublicDelegatedPrefixeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Get(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExamplePublicDelegatedPrefixesClient_Insert() {
	ctx := context.Background()
	c, err := compute.NewPublicDelegatedPrefixesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &computepb.InsertPublicDelegatedPrefixeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Insert(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExamplePublicDelegatedPrefixesClient_List() {
	ctx := context.Background()
	c, err := compute.NewPublicDelegatedPrefixesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &computepb.ListPublicDelegatedPrefixesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.List(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExamplePublicDelegatedPrefixesClient_Patch() {
	ctx := context.Background()
	c, err := compute.NewPublicDelegatedPrefixesRESTClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &computepb.PatchPublicDelegatedPrefixeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Patch(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
