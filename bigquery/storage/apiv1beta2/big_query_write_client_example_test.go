// Copyright 2020 Google LLC
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

package storage_test

import (
	"context"
	"io"

	storage "cloud.google.com/go/bigquery/storage/apiv1beta2"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"
)

func ExampleNewBigQueryWriteClient() {
	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleBigQueryWriteClient_CreateWriteStream() {
	// import storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"

	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.CreateWriteStreamRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateWriteStream(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleBigQueryWriteClient_AppendRows() {
	// import storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"

	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	stream, err := c.AppendRows(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	go func() {
		reqs := []*storagepb.AppendRowsRequest{
			// TODO: Create requests.
		}
		for _, req := range reqs {
			if err := stream.Send(req); err != nil {
				// TODO: Handle error.
			}
		}
		stream.CloseSend()
	}()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleBigQueryWriteClient_GetWriteStream() {
	// import storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"

	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.GetWriteStreamRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetWriteStream(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleBigQueryWriteClient_FinalizeWriteStream() {
	// import storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"

	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.FinalizeWriteStreamRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.FinalizeWriteStream(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleBigQueryWriteClient_BatchCommitWriteStreams() {
	// import storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"

	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.BatchCommitWriteStreamsRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.BatchCommitWriteStreams(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleBigQueryWriteClient_FlushRows() {
	// import storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"

	ctx := context.Background()
	c, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &storagepb.FlushRowsRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.FlushRows(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
