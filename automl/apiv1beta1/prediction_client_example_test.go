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

package automl_test

import (
	"context"

	automl "cloud.google.com/go/automl/apiv1beta1"
	automlpb "google.golang.org/genproto/googleapis/cloud/automl/v1beta1"
)

func ExampleNewPredictionClient() {
	ctx := context.Background()
	c, err := automl.NewPredictionClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExamplePredictionClient_Predict() {
	ctx := context.Background()
	c, err := automl.NewPredictionClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &automlpb.PredictRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Predict(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExamplePredictionClient_BatchPredict() {
	ctx := context.Background()
	c, err := automl.NewPredictionClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &automlpb.BatchPredictRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.BatchPredict(ctx, req)
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
