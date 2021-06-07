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

// [START jobs_v4beta1_generated_ProfileService_DeleteProfile_sync]

package main

import (
	"context"

	talent "cloud.google.com/go/talent/apiv4beta1"
	talentpb "google.golang.org/genproto/googleapis/cloud/talent/v4beta1"
)

func main() {
	ctx := context.Background()
	c, err := talent.NewProfileClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &talentpb.DeleteProfileRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteProfile(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

// [END jobs_v4beta1_generated_ProfileService_DeleteProfile_sync]
