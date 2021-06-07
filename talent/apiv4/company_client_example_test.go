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

package talent_test

import (
	"context"

	talent "cloud.google.com/go/talent/apiv4"
	"google.golang.org/api/iterator"
	talentpb "google.golang.org/genproto/googleapis/cloud/talent/v4"
)

func ExampleNewCompanyClient() {
	ctx := context.Background()
	c, err := talent.NewCompanyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExampleCompanyClient_CreateCompany() {
	ctx := context.Background()
	c, err := talent.NewCompanyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &talentpb.CreateCompanyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateCompany(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleCompanyClient_GetCompany() {
	ctx := context.Background()
	c, err := talent.NewCompanyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &talentpb.GetCompanyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetCompany(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleCompanyClient_UpdateCompany() {
	ctx := context.Background()
	c, err := talent.NewCompanyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &talentpb.UpdateCompanyRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateCompany(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleCompanyClient_DeleteCompany() {
	ctx := context.Background()
	c, err := talent.NewCompanyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &talentpb.DeleteCompanyRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteCompany(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleCompanyClient_ListCompanies() {
	ctx := context.Background()
	c, err := talent.NewCompanyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &talentpb.ListCompaniesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListCompanies(ctx, req)
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
