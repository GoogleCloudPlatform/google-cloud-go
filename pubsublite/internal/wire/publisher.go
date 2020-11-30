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

package wire

import (
	"context"
	"errors"
	"reflect"

	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errInvalidInitialPubResponse = errors.New("pubsublite: first response from server was not an initial response for publish")
	errInvalidMsgPubResponse     = errors.New("pubsublite: received invalid publish response from server")
)

// singlePartitionPublisher publishes messages to a single topic partition.
//
// Life of a successfully published message:
// - Publish() receives the message from the user.
// - It is added to `batcher.msgBundler`, which performs batching in accordance
//   with user-configured PublishSettings.
// - onNewBatch() receives new message batches from the bundler. The batch is
//   added to `batcher.publishQueue` (in-flight batches) and sent to the publish
//   stream, if connected. If the stream is currently reconnecting, the entire
//   queue is resent to the stream immediately after it has reconnected, in
//   onStreamStatusChange().
// - onResponse() receives the first cursor offset for the first batch in
//   `batcher.publishQueue`. It assigns the cursor offsets for each message and
//   releases the publish results to the user.
//
// See comments for unsafeInitiateShutdown() for error scenarios.
type singlePartitionPublisher struct {
	// Immutable after creation.
	pubClient  *vkit.PublisherClient
	topic      topicPartition
	initialReq *pb.PublishRequest

	// Fields below must be guarded with mu.
	stream             *retryableStream
	batcher            *publishMessageBatcher
	enableSendToStream bool

	abstractService
}

// singlePartitionPublisherFactory creates instances of singlePartitionPublisher
// for given partition numbers.
type singlePartitionPublisherFactory struct {
	ctx       context.Context
	pubClient *vkit.PublisherClient
	settings  PublishSettings
	topicPath string
}

func (f *singlePartitionPublisherFactory) New(partition int) *singlePartitionPublisher {
	pp := &singlePartitionPublisher{
		pubClient: f.pubClient,
		topic:     topicPartition{Path: f.topicPath, Partition: partition},
		initialReq: &pb.PublishRequest{
			RequestType: &pb.PublishRequest_InitialRequest{
				InitialRequest: &pb.InitialPublishRequest{
					Topic:     f.topicPath,
					Partition: int64(partition),
				},
			},
		},
	}
	pp.batcher = newPublishMessageBatcher(&f.settings, partition, pp.onNewBatch)
	pp.stream = newRetryableStream(f.ctx, pp, f.settings.Timeout, reflect.TypeOf(pb.PublishResponse{}))
	return pp
}

// Start attempts to establish a publish stream connection.
func (pp *singlePartitionPublisher) Start() {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	if pp.unsafeUpdateStatus(serviceStarting, nil) {
		pp.stream.Start()
	}
}

// Stop initiates shutdown of the publisher. All pending messages are flushed.
func (pp *singlePartitionPublisher) Stop() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.unsafeInitiateShutdown(serviceTerminating, nil)
}

// Publish a pub/sub message.
func (pp *singlePartitionPublisher) Publish(msg *pb.PubSubMessage, onResult PublishResultFunc) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	processMessage := func() error {
		if err := pp.unsafeCheckServiceStatus(); err != nil {
			return err
		}
		if err := pp.batcher.AddMessage(msg, onResult); err != nil {
			return err
		}
		return nil
	}

	// If the new message cannot be published, flush pending messages and then
	// terminate the stream once results are received.
	if err := processMessage(); err != nil {
		pp.unsafeInitiateShutdown(serviceTerminating, err)
		onResult(nil, err)
	}
}

func (pp *singlePartitionPublisher) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return pp.pubClient.Publish(addTopicRoutingMetadata(ctx, pp.topic))
}

func (pp *singlePartitionPublisher) initialRequest() (interface{}, bool) {
	return pp.initialReq, true
}

func (pp *singlePartitionPublisher) validateInitialResponse(response interface{}) error {
	pubResponse, _ := response.(*pb.PublishResponse)
	if pubResponse.GetInitialResponse() == nil {
		return errInvalidInitialPubResponse
	}
	return nil
}

func (pp *singlePartitionPublisher) onStreamStatusChange(status streamStatus) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	switch status {
	case streamReconnecting:
		// Prevent onNewBatch() from sending any new batches to the stream.
		pp.enableSendToStream = false

	case streamConnected:
		pp.unsafeUpdateStatus(serviceActive, nil)

		// To ensure messages are sent in order, we should resend in-flight batches
		// to the stream immediately after reconnecting, before any new batches.
		batches := pp.batcher.InFlightBatches()
		for _, batch := range batches {
			if !pp.stream.Send(batch.ToPublishRequest()) {
				return
			}
		}
		pp.enableSendToStream = true

	case streamTerminated:
		pp.unsafeInitiateShutdown(serviceTerminated, pp.stream.Error())
	}
}

func (pp *singlePartitionPublisher) onNewBatch(batch *publishBatch) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	pp.batcher.AddBatch(batch)
	if pp.enableSendToStream {
		// Note: if the underlying stream is reconnecting or Send() fails, all
		// in-flight batches will be sent to the stream once the connection has been
		// re-established. Thus the return value is ignored.
		pp.stream.Send(batch.ToPublishRequest())
	}
}

func (pp *singlePartitionPublisher) onResponse(response interface{}) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	processResponse := func() error {
		pubResponse, _ := response.(*pb.PublishResponse)
		if pubResponse.GetMessageResponse() == nil {
			return errInvalidMsgPubResponse
		}
		firstOffset := pubResponse.GetMessageResponse().GetStartCursor().GetOffset()
		if err := pp.batcher.OnPublishResponse(firstOffset); err != nil {
			return err
		}
		pp.unsafeCheckDone()
		return nil
	}
	if err := processResponse(); err != nil {
		pp.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

// unsafeInitiateShutdown must be provided a target serviceStatus, which must be
// one of:
// * serviceTerminating: attempts to successfully publish all pending messages
//   before terminating the publisher. Occurs when:
//   - The user calls Stop().
//   - A new message fails preconditions. This should block the publish of
//     subsequent messages to ensure ordering, but all pending messages should
//     be flushed.
// * serviceTerminated: immediately terminates the publisher and errors all
//   in-flight batches and pending messages in the bundler. Occurs when:
//   - The publish stream terminates with a non-retryable error.
//   - An inconsistency is detected in the server's publish responses. Assume
//     there is a bug on the server and terminate the publisher, as correct
//     processing of messages cannot be guaranteed.
//
// Expected to be called with singlePartitionPublisher.mu held.
func (pp *singlePartitionPublisher) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !pp.unsafeUpdateStatus(targetStatus, err) {
		return
	}

	// Close the stream if this is an immediate shutdown. Otherwise leave it open
	// to send pending messages.
	if targetStatus == serviceTerminated {
		pp.enableSendToStream = false
		pp.stream.Stop()
	}

	// Bundler.Flush() blocks and invokes onNewBatch(), which acquires the mutex,
	// so it cannot be held here.
	// Updating the publisher status above prevents any new messages from being
	// added to the Bundler after flush.
	pp.mu.Unlock()
	pp.batcher.Flush()
	pp.mu.Lock()

	// If flushing pending messages, close the stream if there's nothing left to
	// publish.
	if targetStatus == serviceTerminating {
		pp.unsafeCheckDone()
		return
	}

	// For immediate shutdown set the error message for all pending messages.
	pp.batcher.OnPermanentError(err)
}

// unsafeCheckDone closes the stream once all pending messages have been
// published during shutdown.
func (pp *singlePartitionPublisher) unsafeCheckDone() {
	if pp.status == serviceTerminating && pp.batcher.InFlightBatchesEmpty() {
		pp.stream.Stop()
	}
}
