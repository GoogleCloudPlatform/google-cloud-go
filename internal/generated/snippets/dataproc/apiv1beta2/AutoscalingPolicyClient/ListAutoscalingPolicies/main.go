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

// [START dataproc_v1beta2_generated_AutoscalingPolicyService_ListAutoscalingPolicies_sync]

package main

import (
	dataproc "cloud.google.com/go/dataproc/apiv1beta2"
	"context"
	"google.golang.org/api/iterator"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1beta2"
)

func main() {
	// import dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1beta2"
	// import "google.golang.org/api/iterator"

	ctx := context.Background()
	c, err := dataproc.NewAutoscalingPolicyClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.ListAutoscalingPoliciesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListAutoscalingPolicies(ctx, req)
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

// [END dataproc_v1beta2_generated_AutoscalingPolicyService_ListAutoscalingPolicies_sync]
