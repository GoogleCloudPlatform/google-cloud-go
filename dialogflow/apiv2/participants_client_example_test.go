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

package dialogflow_test

import (
	"context"

	dialogflow "cloud.google.com/go/dialogflow/apiv2"
	"google.golang.org/api/iterator"
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
)

func ExampleNewParticipantsClient() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	// TODO: Use client.
	_ = c
}

func ExampleParticipantsClient_CreateParticipant() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &dialogflowpb.CreateParticipantRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateParticipant(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleParticipantsClient_GetParticipant() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &dialogflowpb.GetParticipantRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetParticipant(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleParticipantsClient_ListParticipants() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &dialogflowpb.ListParticipantsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListParticipants(ctx, req)
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

func ExampleParticipantsClient_UpdateParticipant() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &dialogflowpb.UpdateParticipantRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateParticipant(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleParticipantsClient_AnalyzeContent() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &dialogflowpb.AnalyzeContentRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.AnalyzeContent(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleParticipantsClient_SuggestArticles() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &dialogflowpb.SuggestArticlesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.SuggestArticles(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleParticipantsClient_SuggestFaqAnswers() {
	ctx := context.Background()
	c, err := dialogflow.NewParticipantsClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &dialogflowpb.SuggestFaqAnswersRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.SuggestFaqAnswers(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
