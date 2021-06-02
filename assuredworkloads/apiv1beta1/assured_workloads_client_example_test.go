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

package assuredworkloads_test

import (
	"context"

	assuredworkloads "cloud.google.com/go/assuredworkloads/apiv1beta1"
	"google.golang.org/api/iterator"
	assuredworkloadspb "google.golang.org/genproto/googleapis/cloud/assuredworkloads/v1beta1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := assuredworkloads.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExampleClient_CreateWorkload() {
	ctx := context.Background()
	c, err := assuredworkloads.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &assuredworkloadspb.CreateWorkloadRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.CreateWorkload(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_UpdateWorkload() {
	ctx := context.Background()
	c, err := assuredworkloads.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &assuredworkloadspb.UpdateWorkloadRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateWorkload(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_DeleteWorkload() {
	ctx := context.Background()
	c, err := assuredworkloads.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &assuredworkloadspb.DeleteWorkloadRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteWorkload(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_GetWorkload() {
	ctx := context.Background()
	c, err := assuredworkloads.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &assuredworkloadspb.GetWorkloadRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetWorkload(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListWorkloads() {
	ctx := context.Background()
	c, err := assuredworkloads.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &assuredworkloadspb.ListWorkloadsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListWorkloads(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}
