// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START workflows_v1beta_generated_Workflows_UpdateWorkflow_sync]

package main

import (
	workflows "cloud.google.com/go/workflows/apiv1beta"
	"context"
	workflowspb "google.golang.org/genproto/googleapis/cloud/workflows/v1beta"
)

func main() {
	// import workflowspb "google.golang.org/genproto/googleapis/cloud/workflows/v1beta"

	ctx := context.Background()
	c, err := workflows.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &workflowspb.UpdateWorkflowRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.UpdateWorkflow(ctx, req)
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

// [END workflows_v1beta_generated_Workflows_UpdateWorkflow_sync]
