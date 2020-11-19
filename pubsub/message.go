// Copyright 2016 Google LLC
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

package pubsub

import (
	"fmt"
	"time"

	internalpubsub "cloud.google.com/go/internal/pubsub"
	"github.com/golang/protobuf/ptypes"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
)

// Message represents a Pub/Sub message.
type Message = internalpubsub.Message

// msgAckHandler performs a safe cast of the message's ack handler to psAckHandler.
func msgAckHandler(m *Message) (*psAckHandler, bool) {
	ackh, ok := internalpubsub.MessageAckHandler(m).(*psAckHandler)
	return ackh, ok
}

func msgAckID(m *Message) string {
	if ackh, ok := msgAckHandler(m); ok {
		return ackh.ackID
	}
	return ""
}

// The done method of the iterator that created a Message.
type iterDoneFunc func(string, bool, time.Time)

func convertMessages(rms []*pb.ReceivedMessage, receiveTime time.Time, doneFunc iterDoneFunc) ([]*Message, error) {
	msgs := make([]*Message, 0, len(rms))
	for i, m := range rms {
		msg, err := toMessage(m, receiveTime, doneFunc)
		if err != nil {
			return nil, fmt.Errorf("pubsub: cannot decode the retrieved message at index: %d, message: %+v", i, m)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func toMessage(resp *pb.ReceivedMessage, receiveTime time.Time, doneFunc iterDoneFunc) (*Message, error) {
	ackh := &psAckHandler{ackID: resp.AckId}
	msg := internalpubsub.NewMessage(ackh)
	if resp.Message == nil {
		return msg, nil
	}

	pubTime, err := ptypes.Timestamp(resp.Message.PublishTime)
	if err != nil {
		return nil, err
	}

	var deliveryAttempt *int
	if resp.DeliveryAttempt > 0 {
		da := int(resp.DeliveryAttempt)
		deliveryAttempt = &da
	}

	msg.Data = resp.Message.Data
	msg.Attributes = resp.Message.Attributes
	msg.ID = resp.Message.MessageId
	msg.PublishTime = pubTime
	msg.DeliveryAttempt = deliveryAttempt
	msg.OrderingKey = resp.Message.OrderingKey
	ackh.receiveTime = receiveTime
	ackh.doneFunc = doneFunc
	return msg, nil
}

// psAckHandler handles ack/nack for the pubsub package.
type psAckHandler struct {
	// ackID is the identifier to acknowledge this message.
	ackID string

	// receiveTime is the time the message was received by the client.
	receiveTime time.Time

	calledDone bool

	// The done method of the iterator that created this Message.
	doneFunc iterDoneFunc
}

func (ah *psAckHandler) OnAck() {
	ah.done(true)
}

func (ah *psAckHandler) OnNack() {
	ah.done(false)
}

func (ah *psAckHandler) done(ack bool) {
	if ah.calledDone {
		return
	}
	ah.calledDone = true
	if ah.doneFunc != nil {
		ah.doneFunc(ah.ackID, ack, ah.receiveTime)
	}
}
