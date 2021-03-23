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

package cx_test

import (
	"context"

	cx "cloud.google.com/go/dialogflow/cx/apiv3"
	"google.golang.org/api/iterator"
	cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"
)

func ExampleNewTestCasesClient() {
	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleTestCasesClient_ListTestCases() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"
	// import "google.golang.org/api/iterator"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.ListTestCasesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListTestCases(ctx, req)
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

func ExampleTestCasesClient_BatchDeleteTestCases() {
	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.BatchDeleteTestCasesRequest{
		// TODO: Fill request struct fields.
	}
	err = c.BatchDeleteTestCases(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleTestCasesClient_GetTestCase() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.GetTestCaseRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetTestCase(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleTestCasesClient_CreateTestCase() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.CreateTestCaseRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateTestCase(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleTestCasesClient_UpdateTestCase() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.UpdateTestCaseRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateTestCase(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleTestCasesClient_RunTestCase() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.RunTestCaseRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.RunTestCase(ctx, req)
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

func ExampleTestCasesClient_BatchRunTestCases() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.BatchRunTestCasesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchRunTestCases(ctx, req)
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

func ExampleTestCasesClient_CalculateCoverage() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.CalculateCoverageRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CalculateCoverage(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleTestCasesClient_ImportTestCases() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.ImportTestCasesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.ImportTestCases(ctx, req)
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

func ExampleTestCasesClient_ExportTestCases() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.ExportTestCasesRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.ExportTestCases(ctx, req)
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

func ExampleTestCasesClient_ListTestCaseResults() {
	// import cxpb "google.golang.org/genproto/googleapis/cloud/dialogflow/cx/v3"
	// import "google.golang.org/api/iterator"

	ctx := context.Background()
	c, err := cx.NewTestCasesClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cxpb.ListTestCaseResultsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListTestCaseResults(ctx, req)
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
