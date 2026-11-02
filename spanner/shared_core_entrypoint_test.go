/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type mockSharedCoreSpannerServer struct {
	sppb.UnimplementedSpannerServer
	mu      sync.Mutex
	handler func(req *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error
}

func (s *mockSharedCoreSpannerServer) ExecuteStreamingSql(req *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
	s.mu.Lock()
	h := s.handler
	s.mu.Unlock()
	if h != nil {
		return h(req, stream)
	}
	return status.Error(codes.Unimplemented, "not configured")
}

func startMockSharedCoreServer(t *testing.T) (*mockSharedCoreSpannerServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	srv := grpc.NewServer()
	mock := &mockSharedCoreSpannerServer{}
	sppb.RegisterSpannerServer(srv, mock)
	go func() {
		_ = srv.Serve(lis)
	}()
	return mock, lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func TestSharedCoreEntrypoint_MultiChannelRoundRobin(t *testing.T) {
	t.Setenv("SPANNER_EMULATOR_HOST", "")
	_, addr, cleanup := startMockSharedCoreServer(t)
	defer cleanup()

	const numCh = 4
	transport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint:    addr,
		NumChannels: numCh,
		PlainText:   true,
	})
	if err != nil {
		t.Fatalf("NewSharedCoreTransport failed: %v", err)
	}
	defer transport.Close()

	if transport.sc == nil || transport.sc.dynamicPool != nil {
		t.Fatalf("expected default DCPEnabled=false to use static channel pool (dynamicPool == nil)")
	}
	if len(transport.clients) != numCh {
		t.Fatalf("expected %d static spannerClients, got %d", numCh, len(transport.clients))
	}

	seen := make(map[spannerClient]bool, numCh)
	for i := 0; i < numCh*2; i++ {
		c, err := transport.pickClient()
		if err != nil {
			t.Fatalf("pickClient(%d) failed: %v", i, err)
		}
		seen[c] = true
	}
	if len(seen) != numCh {
		t.Fatalf("expected %d distinct spannerClients under round-robin, got %d", numCh, len(seen))
	}
}

func TestSharedCoreEntrypoint_DynamicChannelPoolOptIn(t *testing.T) {
	t.Setenv("SPANNER_EMULATOR_HOST", "")
	_, addr, cleanup := startMockSharedCoreServer(t)
	defer cleanup()

	transport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint:  addr,
		PlainText: true,
		DynamicChannelPoolConfig: DynamicChannelPoolConfig{
			DCPEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("NewSharedCoreTransport with DCPEnabled=true failed: %v", err)
	}
	defer transport.Close()

	if transport.sc == nil || transport.sc.dynamicPool == nil {
		t.Fatalf("expected transport.sc.dynamicPool to be initialized when DCPEnabled=true")
	}
	wantInitial := DefaultDynamicChannelPoolConfig().DCPInitialChannels
	if got := transport.sc.dynamicPool.Num(); got != wantInitial {
		t.Fatalf("expected %d initial channels in dynamicChannelPool, got %d", wantInitial, got)
	}
	if gotMax := transport.sc.dynamicPool.cfg.DCPMaxChannels; gotMax != DefaultDynamicChannelPoolConfig().DCPMaxChannels {
		t.Fatalf("expected DCPMaxChannels=%d from DefaultDynamicChannelPoolConfig, got %d", DefaultDynamicChannelPoolConfig().DCPMaxChannels, gotMax)
	}

	seen := make(map[*dcpEntry]bool, wantInitial)
	for i := 0; i < wantInitial*8 && len(seen) < wantInitial; i++ {
		c, err := transport.pickClient()
		if err != nil {
			t.Fatalf("pickClient(%d) failed: %v", i, err)
		}
		dcpClient, ok := c.(*dcpSpannerClient)
		if !ok || dcpClient.entry == nil {
			t.Fatalf("expected *dcpSpannerClient with non-nil entry, got %T", c)
		}
		seen[dcpClient.entry] = true
		// Simulate in-flight stream load on picked channel so DCPPowerOfTwoLeastBusy
		// routes subsequent picks to the remaining idle channels.
		dcpClient.entry.streamLoad.Add(10)
		defer dcpClient.entry.streamLoad.Add(-10)
	}
	if len(seen) != wantInitial {
		t.Fatalf("expected %d distinct dcpEntry channels under DCPPowerOfTwoLeastBusy, got %d", wantInitial, len(seen))
	}
}

func TestIsLoopbackEndpoint(t *testing.T) {
	cases := []struct {
		endpoint string
		want     bool
	}{
		{"localhost:8080", true},
		{"127.0.0.1:9010", true},
		{"[::1]:9010", true},
		{"localhost.example.com:443", false},
		{"127.0.0.1.nip.io:443", false},
		{"spanner.googleapis.com:443", false},
		{"localhost", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isLoopbackEndpoint(tc.endpoint); got != tc.want {
			t.Errorf("isLoopbackEndpoint(%q) = %v, want %v", tc.endpoint, got, tc.want)
		}
	}
}

func TestSharedCoreEntrypoint_DCPPrimingUnaryInterceptorHeaders(t *testing.T) {
	baseMD := metadata.Pairs(
		"authorization", "Bearer secret-token",
		"x-goog-spanner-request-id", "stale-req-id",
		"x-goog-request-params", "session=stale-session",
		"google-cloud-resource-prefix", "projects/old/instances/old/databases/old",
	)
	req := &sppb.ExecuteSqlRequest{
		Session: "projects/p/instances/i/databases/db2/sessions/s2",
		Sql:     "SELECT 1",
	}
	uCtx := applyDCPPrimeMetadata(context.Background(), req, baseMD)
	outMD, ok := metadata.FromOutgoingContext(uCtx)
	if !ok {
		t.Fatal("expected outgoing metadata in context")
	}
	if got := outMD.Get("google-cloud-resource-prefix"); len(got) != 1 || got[0] != "projects/p/instances/i/databases/db2" {
		t.Fatalf("unexpected google-cloud-resource-prefix: %v", got)
	}
	if got := outMD.Get("authorization"); len(got) != 1 || got[0] != "Bearer secret-token" {
		t.Fatalf("expected authorization header preserved, got %v", got)
	}
	if got := outMD.Get("x-goog-spanner-request-id"); len(got) != 0 {
		t.Fatalf("expected stale x-goog-spanner-request-id stripped, got %v", got)
	}
	if got := outMD.Get("x-goog-request-params"); len(got) != 0 {
		t.Fatalf("expected stale x-goog-request-params stripped, got %v", got)
	}
	if orig := baseMD.Get("google-cloud-resource-prefix"); len(orig) != 1 || orig[0] != "projects/old/instances/old/databases/old" {
		t.Fatalf("expected baseMD not to be mutated, got %v", orig)
	}
}

func TestResolveInMemoryFallbackRootCAs_EmptySystemPool(t *testing.T) {
	if os.Getenv("SPANNER_TEST_EMPTY_SYS_CERT_POOL") == "1" {
		priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("GenerateKey failed: %v", err)
		}
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{CommonName: "SpannerSharedCoreTestCA"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			KeyUsage:              x509.KeyUsageCertSign,
			BasicConstraintsValid: true,
			IsCA:                  true,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
		if err != nil {
			t.Fatalf("CreateCertificate failed: %v", err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

		SetSharedCoreFallbackRootCAPEM(string(pemBytes))
		defer SetSharedCoreFallbackRootCAPEM("")

		pool := resolveInMemoryFallbackRootCAs("")
		if pool == nil || len(pool.Subjects()) == 0 {
			t.Fatal("expected non-empty fallback CertPool when system cert pool is unavailable")
		}
		if badPool := resolveInMemoryFallbackRootCAs("not-valid-pem"); badPool != nil {
			t.Fatal("expected nil CertPool for invalid PEM")
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestResolveInMemoryFallbackRootCAs_EmptySystemPool$")
	cmd.Env = append(os.Environ(),
		"SPANNER_TEST_EMPTY_SYS_CERT_POOL=1",
		"SSL_CERT_FILE=/nonexistent/spanner_test_cert.pem",
		"SSL_CERT_DIR=/nonexistent/spanner_test_certs",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, string(out))
	}
}
