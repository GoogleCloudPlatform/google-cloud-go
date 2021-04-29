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

// [START datalabeling_v1beta1_generated_DataLabelingService_SearchEvaluations_sync]

package main

import (
	datalabeling "cloud.google.com/go/datalabeling/apiv1beta1"
	"context"
	"google.golang.org/api/iterator"
	datalabelingpb "google.golang.org/genproto/googleapis/cloud/datalabeling/v1beta1"
)

func main() {
	// import datalabelingpb "google.golang.org/genproto/googleapis/cloud/datalabeling/v1beta1"
	// import "google.golang.org/api/iterator"

	ctx := context.Background()
	c, err := datalabeling.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &datalabelingpb.SearchEvaluationsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.SearchEvaluations(ctx, req)
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

// [END datalabeling_v1beta1_generated_DataLabelingService_SearchEvaluations_sync]
