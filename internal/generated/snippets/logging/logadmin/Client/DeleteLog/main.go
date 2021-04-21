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

// [START logging_generated_logging_logadmin_Client_DeleteLog]

package main

import (
	"context"

	"cloud.google.com/go/logging/logadmin"
)

func main() {
	ctx := context.Background()
	client, err := logadmin.NewClient(ctx, "my-project")
	if err != nil {
		// TODO: Handle error.
	}
	err = client.DeleteLog(ctx, "my-log")
	if err != nil {
		// TODO: Handle error.
	}
}

// [END logging_generated_logging_logadmin_Client_DeleteLog]
