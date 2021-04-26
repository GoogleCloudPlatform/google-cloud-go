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

// [START pubsub_generated_pubsublite_pscompat_SubscriberClient_Receive_errorHandling]

package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
	"golang.org/x/xerrors"
)

func main() {
	ctx := context.Background()
	const subscription = "projects/my-project/locations/zone/subscriptions/my-subscription"
	settings := pscompat.ReceiveSettings{
		// The SubscriberClient will terminate when it cannot connect to backends
		// for more than 5 minutes.
		Timeout: 5 * time.Minute,
	}
	subscriber, err := pscompat.NewSubscriberClientWithSettings(ctx, subscription, settings)
	if err != nil {
		// TODO: Handle error.
	}

	for {
		cctx, cancel := context.WithCancel(ctx)
		err = subscriber.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			// TODO: Handle message.
			// NOTE: May be called concurrently; synchronize access to shared memory.
			m.Ack()
		})
		if err != nil {
			fmt.Printf("Subscriber client stopped receiving due to error: %v\n", err)
			if xerrors.Is(err, pscompat.ErrBackendUnavailable) {
				// TODO: Alert if necessary. Receive can be retried.
			} else {
				// TODO: Handle fatal error.
				break
			}
		}

		// Call cancel from the receiver callback or another goroutine to stop
		// receiving.
		cancel()
	}
}

// [END pubsub_generated_pubsublite_pscompat_SubscriberClient_Receive_errorHandling]
