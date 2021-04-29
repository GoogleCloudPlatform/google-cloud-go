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

// [START dataqna_v1alpha_generated_AutoSuggestionService_SuggestQueries_sync]

package main

import (
	dataqna "cloud.google.com/go/dataqna/apiv1alpha"
	"context"
	dataqnapb "google.golang.org/genproto/googleapis/cloud/dataqna/v1alpha"
)

func main() {
	// import dataqnapb "google.golang.org/genproto/googleapis/cloud/dataqna/v1alpha"

	ctx := context.Background()
	c, err := dataqna.NewAutoSuggestionClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataqnapb.SuggestQueriesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.SuggestQueries(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

// [END dataqna_v1alpha_generated_AutoSuggestionService_SuggestQueries_sync]
