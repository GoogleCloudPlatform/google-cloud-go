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

package test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// OrderingSender generates strings containing a message index to use for
// verifying message ordering. It is used on conjunction with Publishers.
type OrderingSender struct {
	TotalMsgCount int64
}

// NewOrderingSender creats a new OrderingSender.
func NewOrderingSender() *OrderingSender {
	return new(OrderingSender)
}

// Next generates the next string to publish.
func (os *OrderingSender) Next(prefix string) string {
	os.TotalMsgCount++
	return fmt.Sprintf("%s/%d", prefix, os.TotalMsgCount)
}

// OrderingReceiver consumes a message string generated by OrderingSender and
// verifies that messages in a partition are ordered. It is used in conjunction
// with Subscribers.
type OrderingReceiver struct {
	mu sync.Mutex
	// Map of key and last received message index. Messages are only guaranteed to
	// be received in order within a partition.
	received map[string]int64
}

// NewOrderingReceiver creates a new OrderingReceiver.
func NewOrderingReceiver() *OrderingReceiver {
	return &OrderingReceiver{
		received: make(map[string]int64),
	}
}

func parseMsgIndex(msg string) int64 {
	pos := strings.LastIndex(msg, "/")
	if pos >= 0 {
		if n, err := strconv.ParseInt(msg[pos+1:], 10, 64); err == nil {
			return n
		}
	}
	return -1
}

// Receive checks the given message data and key and returns an error if
// unordered messages are detected.
//
// Note: a normal scenario resulting in unordered messages is when the Publish
// stream breaks while there are in-flight batches, which are resent upon
// stream reconnect.
func (or *OrderingReceiver) Receive(data, key string) error {
	or.mu.Lock()
	defer or.mu.Unlock()

	idx := parseMsgIndex(data)
	if idx < 0 {
		return fmt.Errorf("failed to parse index from message: %q", data)
	}

	// Verify non-decreasing ordering. Allow duplicates, which can be verified
	// with DuplicateMsgDetector.
	lastIdx, exists := or.received[key]
	if exists && idx < lastIdx {
		return fmt.Errorf("message ordering failed for key %s, expected message idx >= %d, got %d", key, lastIdx, idx)
	}
	or.received[key] = idx
	return nil
}

var void struct{}

type msgMetadata struct {
	offsets map[int64]struct{}
}

func newMsgMetadata() *msgMetadata {
	return &msgMetadata{
		offsets: make(map[int64]struct{}),
	}
}

func (mm *msgMetadata) ContainsOffset(offset int64) bool {
	_, exists := mm.offsets[offset]
	return exists
}

func (mm *msgMetadata) AddOffset(offset int64) {
	mm.offsets[offset] = void
}

// DuplicateMsgDetector can be used to detect duplicate messages, either due to
// duplicate publishes or receives.
type DuplicateMsgDetector struct {
	mu sync.Mutex
	// Map of Pub/Sub message data and associated metadata.
	msgs                  map[string]*msgMetadata
	duplicatePublishCount int64
	duplicateReceiveCount int64
}

// NewDuplicateMsgDetector creates a new DuplicateMsgDetector.
func NewDuplicateMsgDetector() *DuplicateMsgDetector {
	return &DuplicateMsgDetector{
		msgs: make(map[string]*msgMetadata),
	}
}

// Receive checks the given message data and offset.
func (dm *DuplicateMsgDetector) Receive(data string, offset int64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if metadata, exists := dm.msgs[data]; exists {
		if metadata.ContainsOffset(offset) {
			// If the message contains the same offset, it means it was received
			// multiple times. This is not expected within a single test run. But it
			// is normal when processes are stopped & restarted without committing
			// cursors.
			dm.duplicateReceiveCount++
		} else {
			// If the message contains a different offset, it means a message was
			// republished, which can occur when a publish stream reconnects with
			// in-flight published messages.
			dm.duplicatePublishCount++
			metadata.AddOffset(offset)
		}
	} else {
		metadata = newMsgMetadata()
		metadata.AddOffset(offset)
		dm.msgs[data] = metadata
	}
}

// Status returns a non-empty status string if there were duplicates detected.
func (dm *DuplicateMsgDetector) Status() string {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if (dm.duplicateReceiveCount + dm.duplicatePublishCount) == 0 {
		return ""
	}
	return fmt.Sprintf("duplicate publish count = %d, receive count = %d", dm.duplicatePublishCount, dm.duplicateReceiveCount)
}

// HasPublishDuplicates returns true if duplicate published messages were
// detected.
func (dm *DuplicateMsgDetector) HasPublishDuplicates() bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.duplicatePublishCount > 0
}

// HasReceiveDuplicates returns true if duplicate received messages were
// detected.
func (dm *DuplicateMsgDetector) HasReceiveDuplicates() bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.duplicateReceiveCount > 0
}
