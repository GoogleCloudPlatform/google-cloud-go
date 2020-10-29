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

package pubsublite

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"math/rand"
	"time"

	"github.com/golang/protobuf/ptypes"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// AttributeValues is a slice of strings.
type AttributeValues [][]byte

// Message represents a Pub/Sub message.
type Message struct {
	// Data is the actual data in the message.
	Data []byte

	// Attributes can be used to label the message. A key may have multiple
	// values.
	Attributes map[string]AttributeValues

	// EventTime is an optional, user-specified event time for this message.
	EventTime time.Time

	// OrderingKey identifies related messages for which publish order should
	// be respected. Messages with the same ordering key are published to the
	// same topic partition and subscribers will receive the messages in order.
	// If the ordering key is empty, the message will be sent to an arbitrary
	// partition.
	OrderingKey []byte
}

func (m *Message) toProto() (*pb.PubSubMessage, error) {
	msgpb := &pb.PubSubMessage{
		Data: m.Data,
		Key:  m.OrderingKey,
	}

	if len(m.Attributes) > 0 {
		msgpb.Attributes = make(map[string]*pb.AttributeValues)
		for key, values := range m.Attributes {
			msgpb.Attributes[key] = &pb.AttributeValues{Values: values}
		}
	}

	if !m.EventTime.IsZero() {
		ts, err := ptypes.TimestampProto(m.EventTime)
		if err != nil {
			return nil, fmt.Errorf("pubsublite: error converting message timestamp: %v", err)
		}
		msgpb.EventTime = ts
	}
	return msgpb, nil
}

// messageRouter outputs a partition number, given an ordering key. Results are
// undefined when:
// - setPartitionCount() is called with count <= 0.
// - route() is called before setPartitionCount() to initialize the router.
//
// Message routers need to accommodate topic partition resizing.
type messageRouter interface {
	SetPartitionCount(count int)
	Route(orderingKey []byte) int
}

// roundRobinMsgRouter sequentially cycles through partition numbers, starting
// from a random partition.
type roundRobinMsgRouter struct {
	rng            *rand.Rand
	partitionCount int
	nextPartition  int
}

func (r *roundRobinMsgRouter) SetPartitionCount(count int) {
	r.partitionCount = count
	r.nextPartition = int(r.rng.Int63n(int64(count)))
}

func (r *roundRobinMsgRouter) Route(orderingKey []byte) (partition int) {
	partition = r.nextPartition
	r.nextPartition = (partition + 1) % r.partitionCount
	return
}

// hashingMsgRouter hashes an ordering key using SHA256 to obtain a partition
// number. It should only be used for messages with an ordering key.
//
// Matches implementation at:
// https://github.com/googleapis/java-pubsublite/blob/master/google-cloud-pubsublite/src/main/java/com/google/cloud/pubsublite/internal/DefaultRoutingPolicy.java
type hashingMsgRouter struct {
	partitionCount *big.Int
}

func (r *hashingMsgRouter) SetPartitionCount(count int) {
	r.partitionCount = big.NewInt(int64(count))
}

func (r *hashingMsgRouter) Route(orderingKey []byte) int {
	if len(orderingKey) == 0 {
		return -1
	}
	h := sha256.Sum256(orderingKey)
	num := new(big.Int).SetBytes(h[:])
	partition := new(big.Int).Mod(num, r.partitionCount)
	return int(partition.Int64())
}

// compositeMsgRouter delegates to different message routers for messages
// with/without ordering keys.
type compositeMsgRouter struct {
	keyedRouter   messageRouter
	keylessRouter messageRouter
}

func (r *compositeMsgRouter) SetPartitionCount(count int) {
	r.keyedRouter.SetPartitionCount(count)
	r.keylessRouter.SetPartitionCount(count)
}

func (r *compositeMsgRouter) Route(orderingKey []byte) int {
	if len(orderingKey) > 0 {
		return r.keyedRouter.Route(orderingKey)
	}
	return r.keylessRouter.Route(orderingKey)
}

// defaultMessageRouter returns a compositeMsgRouter that uses hashingMsgRouter
// for messages with ordering key and roundRobinMsgRouter for messages without.
func newDefaultMessageRouter(rng *rand.Rand) messageRouter {
	return &compositeMsgRouter{
		keyedRouter:   &hashingMsgRouter{},
		keylessRouter: &roundRobinMsgRouter{rng: rng},
	}
}
