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

// [START cloudbuild_v1_generated_CloudBuild_DeleteWorkerPool_sync]

package main

import (
	cloudbuild "cloud.google.com/go/cloudbuild/apiv1/v2"
	"context"
	cloudbuildpb "google.golang.org/genproto/googleapis/devtools/cloudbuild/v1"
)

func main() {
	ctx := context.Background()
	c, err := cloudbuild.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cloudbuildpb.DeleteWorkerPoolRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteWorkerPool(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

// [END cloudbuild_v1_generated_CloudBuild_DeleteWorkerPool_sync]
