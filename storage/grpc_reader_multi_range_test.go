// Copyright 2026 Google LLC
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

package storage

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage/experimental"
	"cloud.google.com/go/storage/internal/apiv2/storagepb"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func newBufconnGRPCClient(t *testing.T, srv storagepb.StorageServer, extraOpts ...option.ClientOption) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	storagepb.RegisterStorageServer(s, srv)
	go s.Serve(lis)
	t.Cleanup(s.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	opts := []option.ClientOption{
		option.WithEndpoint("passthrough:///bufconn"),
		option.WithGRPCDialOption(grpc.WithContextDialer(dialer)),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.EnableDirectPath(false),
		WithDisabledClientMetrics(),
		WithGRPCBidiReads(),
	}
	opts = append(opts, extraOpts...)
	client, err := NewGRPCClient(t.Context(), opts...)
	if err != nil {
		t.Fatalf("NewGRPCClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func sendBidiReadRanges(stream storagepb.Storage_BidiReadObjectServer, data []byte, ranges []*storagepb.ReadRange) error {
	for _, rr := range ranges {
		end := rr.GetReadOffset() + rr.GetReadLength()
		if rr.GetReadLength() == 0 || end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk := data[rr.GetReadOffset():end]
		crc := crc32.Checksum(chunk, crc32cTable)
		if err := stream.Send(&storagepb.BidiReadObjectResponse{
			ObjectDataRanges: []*storagepb.ObjectRangeData{{
				ChecksummedData: &storagepb.ChecksummedData{Content: chunk, Crc32C: &crc},
				ReadRange:       &storagepb.ReadRange{ReadOffset: rr.GetReadOffset(), ReadLength: int64(len(chunk)), ReadId: rr.GetReadId()},
				RangeEnd:        true,
			}},
		}); err != nil {
			return err
		}
	}
	return nil
}

type mrdStallOnOpenServer struct {
	storagepb.UnimplementedStorageServer
	data         []byte
	stallStreams int32
	streams      atomic.Int32
	mu           sync.Mutex
	xGoogHeaders []string
}

func (s *mrdStallOnOpenServer) BidiReadObject(stream storagepb.Storage_BidiReadObjectServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	xGoog := strings.Join(md.Get("x-goog-api-client"), " ")
	s.mu.Lock()
	s.xGoogHeaders = append(s.xGoogHeaders, xGoog)
	s.mu.Unlock()

	attempt := s.streams.Add(1)
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if attempt <= s.stallStreams {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	if err := stream.Send(&storagepb.BidiReadObjectResponse{
		Metadata:   &storagepb.Object{Size: int64(len(s.data)), Generation: 1},
		ReadHandle: &storagepb.BidiReadHandle{Handle: []byte("handle-1")},
	}); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := sendBidiReadRanges(stream, s.data, req.GetReadRanges()); err != nil {
			return err
		}
	}
}

func TestMultiRangeDownloaderReadStallTimeout(t *testing.T) {
	data := []byte("0123456789abcdef0123456789abcdef")
	srv := &mrdStallOnOpenServer{data: data, stallStreams: 1}
	client := newBufconnGRPCClient(t, srv, experimental.WithReadStallTimeout(&experimental.ReadStallTimeoutConfig{
		TargetPercentile: 0.99,
		Min:              100 * time.Millisecond,
	}))
	client.SetRetry(WithBackoff(gax.Backoff{Initial: 10 * time.Millisecond}))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	start := time.Now()
	mrd, err := client.Bucket("test-bucket").Object("test-object").NewMultiRangeDownloader(ctx)
	if err != nil {
		t.Fatalf("NewMultiRangeDownloader: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed >= 2*time.Second {
		t.Errorf("NewMultiRangeDownloader elapsed = %v, want < 2s", elapsed)
	}
	if got := srv.streams.Load(); got != 2 {
		t.Errorf("srv.streams.Load() = %d, want 2", got)
	}

	srv.mu.Lock()
	headers := append([]string(nil), srv.xGoogHeaders...)
	srv.mu.Unlock()
	if len(headers) != 2 {
		t.Fatalf("len(srv.xGoogHeaders) = %d, want 2", len(headers))
	}
	if !strings.Contains(headers[0], "gccl-invocation-id/") || !strings.Contains(headers[0], "gccl-attempt-count/1") {
		t.Errorf("headers[0] = %q, want gccl-invocation-id and gccl-attempt-count/1", headers[0])
	}
	if !strings.Contains(headers[1], "gccl-attempt-count/2") {
		t.Errorf("headers[1] = %q, want gccl-attempt-count/2", headers[1])
	}

	var buf bytes.Buffer
	var rangeErr error
	mrd.Add(&buf, 0, int64(len(data)), func(_, _ int64, err error) {
		rangeErr = err
	})
	mrd.Wait()
	if err := mrd.Close(); err != nil {
		t.Errorf("mrd.Close() = %v, want nil", err)
	}
	if rangeErr != nil {
		t.Fatalf("mrd.Add callback err = %v, want nil", rangeErr)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Errorf("mrd.Add data = %q, want %q", buf.Bytes(), data)
	}
}

func TestMultiRangeDownloaderReadStallExhaustsMaxAttempts(t *testing.T) {
	data := []byte("0123456789abcdef0123456789abcdef")
	srv := &mrdStallOnOpenServer{data: data, stallStreams: 5}
	client := newBufconnGRPCClient(t, srv, experimental.WithReadStallTimeout(&experimental.ReadStallTimeoutConfig{
		TargetPercentile: 0.99,
		Min:              50 * time.Millisecond,
	}))
	client.SetRetry(
		WithBackoff(gax.Backoff{Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2}),
		WithMaxAttempts(2),
	)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := client.Bucket("test-bucket").Object("test-object").NewMultiRangeDownloader(ctx)
	if err == nil {
		t.Fatalf("NewMultiRangeDownloader() = nil, want error after exhausting MaxAttempts(2)")
	}
	wantSubstr := "retry failed after 2 attempts"
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("NewMultiRangeDownloader() err = %q, want it to contain %q", err.Error(), wantSubstr)
	}
	if got := srv.streams.Load(); got != 2 {
		t.Errorf("srv.streams.Load() = %d, want 2", got)
	}
}
