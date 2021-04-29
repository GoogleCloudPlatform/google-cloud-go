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

// [START logging_v2_generated_ConfigServiceV2_ListBuckets_sync]

package main

import (
	logging "cloud.google.com/go/logging/apiv2"
	"context"
	"google.golang.org/api/iterator"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
)

func main() {
	// import loggingpb "google.golang.org/genproto/googleapis/logging/v2"
	// import "google.golang.org/api/iterator"

	ctx := context.Background()
	c, err := logging.NewConfigClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &loggingpb.ListBucketsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListBuckets(ctx, req)
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

// [END logging_v2_generated_ConfigServiceV2_ListBuckets_sync]
