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

// [START speech_v1p1beta1_generated_Speech_Recognize_sync]

package main

import (
	speech "cloud.google.com/go/speech/apiv1p1beta1"
	"context"
	speechpb "google.golang.org/genproto/googleapis/cloud/speech/v1p1beta1"
)

func main() {
	// import speechpb "google.golang.org/genproto/googleapis/cloud/speech/v1p1beta1"

	ctx := context.Background()
	c, err := speech.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &speechpb.RecognizeRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Recognize(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

// [END speech_v1p1beta1_generated_Speech_Recognize_sync]
