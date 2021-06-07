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

package privatecatalog_test

import (
	"context"

	privatecatalog "cloud.google.com/go/privatecatalog/apiv1beta1"
	"google.golang.org/api/iterator"
	privatecatalogpb "google.golang.org/genproto/googleapis/cloud/privatecatalog/v1beta1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := privatecatalog.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExampleClient_SearchCatalogs() {
	ctx := context.Background()
	c, err := privatecatalog.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &privatecatalogpb.SearchCatalogsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.SearchCatalogs(ctx, req)
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

func ExampleClient_SearchProducts() {
	ctx := context.Background()
	c, err := privatecatalog.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &privatecatalogpb.SearchProductsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.SearchProducts(ctx, req)
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

func ExampleClient_SearchVersions() {
	ctx := context.Background()
	c, err := privatecatalog.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &privatecatalogpb.SearchVersionsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.SearchVersions(ctx, req)
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
