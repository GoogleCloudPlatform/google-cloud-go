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

// [START monitoring_v3_generated_UptimeCheckService_GetUptimeCheckConfig_sync]

package main

import (
	"context"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

func main() {
	ctx := context.Background()
	c, err := monitoring.NewUptimeCheckClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	defer c.Close()

	req := &monitoringpb.GetUptimeCheckConfigRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetUptimeCheckConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

// [END monitoring_v3_generated_UptimeCheckService_GetUptimeCheckConfig_sync]
