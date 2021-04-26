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

// [START cloudbuild_generated_cloudbuild_apiv1_v2_Client_ListBuilds]

package main

import (
	"context"

	cloudbuild "cloud.google.com/go/cloudbuild/apiv1/v2"
	"google.golang.org/api/iterator"
	cloudbuildpb "google.golang.org/genproto/googleapis/devtools/cloudbuild/v1"
)

func main() {
	// import cloudbuildpb "google.golang.org/genproto/googleapis/devtools/cloudbuild/v1"
	// import "google.golang.org/api/iterator"

	ctx := context.Background()
	c, err := cloudbuild.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cloudbuildpb.ListBuildsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListBuilds(ctx, req)
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

// [END cloudbuild_generated_cloudbuild_apiv1_v2_Client_ListBuilds]
