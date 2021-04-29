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

// [START bigquerystorage_v1beta2_generated_BigQueryWrite_AppendRows_sync]

package main

import (
	storage "cloud.google.com/go/bigquery/storage/apiv1beta2"
	"context"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta2"
	"io"
)

func main() {
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

// [END bigquerystorage_v1beta2_generated_BigQueryWrite_AppendRows_sync]
