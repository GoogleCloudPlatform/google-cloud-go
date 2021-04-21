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

// [START spanner_generated_spanner_admin_database_apiv1_DatabaseAdminClient_CreateDatabase]

package main

import (
	"context"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func main() {
	// import databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	ctx := context.Background()
	c, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &databasepb.CreateDatabaseRequest{
		// TODO: Fill request struct fields.
	}
	op, err := c.CreateDatabase(ctx, req)
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

// [END spanner_generated_spanner_admin_database_apiv1_DatabaseAdminClient_CreateDatabase]
