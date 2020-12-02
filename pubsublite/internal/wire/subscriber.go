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
	"sync"
	"time"

	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errServerNoMessages                = errors.New("pubsublite: server delivered no messages")
	errInvalidInitialSubscribeResponse = errors.New("pubsublite: first response from server was not an initial response for subscribe")
	errInvalidSubscribeResponse        = errors.New("pubsublite: received invalid subscribe response from server")
	errNoInFlightSeek                  = errors.New("pubsublite: received seek response for no in-flight seek")
)

// ReceivedMessage stores a received Pub/Sub message and AckConsumer for
// acknowledging the message.
type ReceivedMessage struct {
	Msg *pb.SequencedMessage
	Ack AckConsumer
}

// MessageReceiverFunc receives a Pub/Sub message from a topic partition.
type MessageReceiverFunc func(*ReceivedMessage)

const maxMessagesBufferSize = 1000

// messageDeliveryQueue delivers received messages to the client-provided
// MessageReceiverFunc sequentially.
type messageDeliveryQueue struct {
	receiver  MessageReceiverFunc
	messagesC chan *ReceivedMessage
	stopC     chan struct{}
	acks      *ackTracker

	// Fields below must be guarded with mu.
	mu     sync.Mutex
	status serviceStatus
}

func newMessageDeliveryQueue(acks *ackTracker, receiver MessageReceiverFunc, bufferSize int) *messageDeliveryQueue {
	// The buffer size is based on ReceiveSettings.MaxOutstandingMessages to
	// handle the worst case of single messages. But ensure there's a reasonable
	// limit as channel buffer capacity is allocated on creation.
	if bufferSize > maxMessagesBufferSize {
		bufferSize = maxMessagesBufferSize
	}
	return &messageDeliveryQueue{
		acks:      acks,
		receiver:  receiver,
		messagesC: make(chan *ReceivedMessage, bufferSize),
		stopC:     make(chan struct{}),
	}
}

func (mq *messageDeliveryQueue) Start() {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if mq.status == serviceUninitialized {
		go mq.deliverMessages()
		mq.status = serviceActive
	}
}

func (mq *messageDeliveryQueue) Stop() {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if mq.status < serviceTerminated {
		close(mq.stopC)
		mq.status = serviceTerminated
	}
}

func (mq *messageDeliveryQueue) Add(messages []*ReceivedMessage) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if mq.status == serviceActive {
		for _, msg := range messages {
			mq.messagesC <- msg
		}
	}
}

func (mq *messageDeliveryQueue) deliverMessages() {
	for {
		// stopC has higher priority.
		select {
		case <-mq.stopC:
			return // Ends the goroutine.
		default:
		}

		select {
		case <-mq.stopC:
			return // Ends the goroutine.
		case msg := <-mq.messagesC:
			// Register outstanding acks, which are primarily handled by the
			// `committer`.
			mq.acks.Push(msg.Ack.(*ackConsumer))
			mq.receiver(msg)
		}
	}
}

// The frequency of sending batch flow control requests.
const batchFlowControlPeriod = 100 * time.Millisecond

// subscribeStream directly wraps the subscribe client stream. It passes
// messages to the message receiver and manages flow control. Flow control
// tokens are batched and sent to the stream via a periodic background task,
// although it can be expedited if the user is rapidly acking messages.
//
// Client-initiated seek unsupported.
type subscribeStream struct {
	// Immutable after creation.
	subClient    *vkit.SubscriberClient
	settings     ReceiveSettings
	subscription subscriptionPartition
	initialReq   *pb.SubscribeRequest
	messageQueue *messageDeliveryQueue

	// Fields below must be guarded with mu.
	stream          *retryableStream
	offsetTracker   subscriberOffsetTracker
	flowControl     flowControlBatcher
	pollFlowControl *periodicTask
	seekInFlight    bool

	abstractService
}

func newSubscribeStream(ctx context.Context, subClient *vkit.SubscriberClient, settings ReceiveSettings,
	receiver MessageReceiverFunc, subscription subscriptionPartition, acks *ackTracker, disableTasks bool) *subscribeStream {

	s := &subscribeStream{
		subClient:    subClient,
		settings:     settings,
		subscription: subscription,
		initialReq: &pb.SubscribeRequest{
			Request: &pb.SubscribeRequest_Initial{
				Initial: &pb.InitialSubscribeRequest{
					Subscription: subscription.Path,
					Partition:    int64(subscription.Partition),
				},
			},
		},
		messageQueue: newMessageDeliveryQueue(acks, receiver, settings.MaxOutstandingMessages),
	}
	s.stream = newRetryableStream(ctx, s, settings.Timeout, reflect.TypeOf(pb.SubscribeResponse{}))

	backgroundTask := s.sendBatchFlowControl
	if disableTasks {
		backgroundTask = func() {}
	}
	s.pollFlowControl = newPeriodicTask(batchFlowControlPeriod, backgroundTask)
	return s
}

// Start establishes a subscribe stream connection and initializes flow control
// tokens from ReceiveSettings.
func (s *subscribeStream) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsafeUpdateStatus(serviceStarting, nil) {
		s.stream.Start()
		s.pollFlowControl.Start()
		s.messageQueue.Start()

		s.flowControl.OnClientFlow(flowControlTokens{
			Bytes:    int64(s.settings.MaxOutstandingBytes),
			Messages: int64(s.settings.MaxOutstandingMessages),
		})
	}
}

// Stop immediately terminates the subscribe stream.
func (s *subscribeStream) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsafeInitiateShutdown(serviceTerminating, nil)
}

func (s *subscribeStream) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return s.subClient.Subscribe(addSubscriptionRoutingMetadata(ctx, s.subscription))
}

func (s *subscribeStream) initialRequest() (interface{}, bool) {
	return s.initialReq, true
}

func (s *subscribeStream) validateInitialResponse(response interface{}) error {
	subscribeResponse, _ := response.(*pb.SubscribeResponse)
	if subscribeResponse.GetInitial() == nil {
		return errInvalidInitialSubscribeResponse
	}
	return nil
}

func (s *subscribeStream) onStreamStatusChange(status streamStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch status {
	case streamConnected:
		s.unsafeUpdateStatus(serviceActive, nil)

		// Reinitialize the offset and flow control tokens when a new subscribe
		// stream instance is connected.
		if seekReq := s.offsetTracker.RequestForRestart(); seekReq != nil {
			if !s.stream.Send(&pb.SubscribeRequest{
				Request: &pb.SubscribeRequest_Seek{Seek: seekReq},
			}) {
				return
			}
			s.seekInFlight = true
		}
		s.unsafeSendFlowControl(s.flowControl.RequestForRestart())
		s.pollFlowControl.Start()

	case streamReconnecting:
		s.seekInFlight = false
		s.pollFlowControl.Stop()

	case streamTerminated:
		s.unsafeInitiateShutdown(serviceTerminated, s.stream.Error())
	}
}

func (s *subscribeStream) onResponse(response interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status >= serviceTerminating {
		return
	}

	var err error
	subscribeResponse, _ := response.(*pb.SubscribeResponse)
	switch {
	case subscribeResponse.GetMessages() != nil:
		err = s.unsafeOnMessageResponse(subscribeResponse.GetMessages())
	case subscribeResponse.GetSeek() != nil:
		err = s.unsafeOnSeekResponse(subscribeResponse.GetSeek())
	default:
		err = errInvalidSubscribeResponse
	}
	if err != nil {
		s.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

func (s *subscribeStream) unsafeOnSeekResponse(response *pb.SeekResponse) error {
	if !s.seekInFlight {
		return errNoInFlightSeek
	}
	s.seekInFlight = false
	return nil
}

func (s *subscribeStream) unsafeOnMessageResponse(response *pb.MessageResponse) error {
	if len(response.Messages) == 0 {
		return errServerNoMessages
	}
	if err := s.offsetTracker.OnMessages(response.Messages); err != nil {
		return err
	}
	if err := s.flowControl.OnMessages(response.Messages); err != nil {
		return err
	}

	var receivedMsgs []*ReceivedMessage
	for _, msg := range response.Messages {
		ack := newAckConsumer(msg.GetCursor().GetOffset(), msg.GetSizeBytes(), s.onAck)
		receivedMsgs = append(receivedMsgs, &ReceivedMessage{Msg: msg, Ack: ack})
	}
	s.messageQueue.Add(receivedMsgs)
	return nil
}

func (s *subscribeStream) onAck(ac *ackConsumer) {
	// Don't block the user's goroutine with potentially expensive ack processing.
	go s.onAckAsync(ac.MsgBytes)
}

func (s *subscribeStream) onAckAsync(msgBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status == serviceActive {
		s.unsafeAllowFlow(flowControlTokens{Bytes: msgBytes, Messages: 1})
	}
}

// sendBatchFlowControl is called by the periodic background task.
func (s *subscribeStream) sendBatchFlowControl() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsafeSendFlowControl(s.flowControl.ReleasePendingRequest())
}

func (s *subscribeStream) unsafeAllowFlow(allow flowControlTokens) {
	s.flowControl.OnClientFlow(allow)
	if s.flowControl.ShouldExpediteBatchRequest() {
		s.unsafeSendFlowControl(s.flowControl.ReleasePendingRequest())
	}
}

func (s *subscribeStream) unsafeSendFlowControl(req *pb.FlowControlRequest) {
	if req == nil {
		return
	}

	// Note: If Send() returns false, the stream will be reconnected and
	// flowControlBatcher.RequestForRestart() will be sent when the stream
	// reconnects. So its return value is ignored.
	s.stream.Send(&pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_FlowControl{FlowControl: req},
	})
}

func (s *subscribeStream) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !s.unsafeUpdateStatus(targetStatus, err) {
		return
	}

	// No data to send. Immediately terminate the stream.
	s.messageQueue.Stop()
	s.pollFlowControl.Stop()
	s.stream.Stop()
}

// singlePartitionSubscriber receives messages from a single topic partition.
// It requires 2 child services:
// - subscribeStream to receive messages from the subscribe stream.
// - committer to commit cursor offsets to the streaming commit cursor stream.
type singlePartitionSubscriber struct {
	compositeService
}

type singlePartitionSubscriberFactory struct {
	ctx              context.Context
	subClient        *vkit.SubscriberClient
	cursorClient     *vkit.CursorClient
	settings         ReceiveSettings
	subscriptionPath string
	receiver         MessageReceiverFunc
	disableTasks     bool
}

func (f *singlePartitionSubscriberFactory) New(partition int) *singlePartitionSubscriber {
	subscription := subscriptionPartition{Path: f.subscriptionPath, Partition: partition}
	acks := newAckTracker()
	commit := newCommitter(f.ctx, f.cursorClient, f.settings, subscription, acks, f.disableTasks)
	sub := newSubscribeStream(f.ctx, f.subClient, f.settings, f.receiver, subscription, acks, f.disableTasks)
	ps := new(singlePartitionSubscriber)
	ps.init()
	ps.unsafeAddServices(sub, commit)
	return ps
}

// multiPartitionSubscriber receives messages from a fixed set of topic
// partitions.
type multiPartitionSubscriber struct {
	compositeService
}

func newMultiPartitionSubscriber(subFactory *singlePartitionSubscriberFactory) *multiPartitionSubscriber {
	ms := new(multiPartitionSubscriber)
	ms.init()

	for _, partition := range subFactory.settings.Partitions {
		subscriber := subFactory.New(partition)
		ms.unsafeAddServices(subscriber)
	}
	return ms
}
