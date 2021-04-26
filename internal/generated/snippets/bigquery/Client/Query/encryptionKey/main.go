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

// [START bigquery_generated_bigquery_Client_Query_encryptionKey]

package main

import (
	"context"

	"cloud.google.com/go/bigquery"
)

func main() {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "project-id")
	if err != nil {
		// TODO: Handle error.
	}
	q := client.Query("select name, num from t1")
	// TODO: Replace this key with a key you have created in Cloud KMS.
	keyName := "projects/P/locations/L/keyRings/R/cryptoKeys/K"
	q.DestinationEncryptionConfig = &bigquery.EncryptionConfig{KMSKeyName: keyName}
	// TODO: set other options on the Query.
	// TODO: Call Query.Run or Query.Read.
}

// [END bigquery_generated_bigquery_Client_Query_encryptionKey]
