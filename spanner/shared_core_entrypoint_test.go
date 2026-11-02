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
	"strings"
	"sync"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/option"
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
	mock, addr, cleanup := startMockSharedCoreServer(t)
	defer cleanup()

	var (
		mu     sync.Mutex
		reqIDs []string
	)
	mock.handler = func(req *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
		if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
			if vals := md.Get(xSpannerRequestIDHeader); len(vals) > 0 {
				mu.Lock()
				reqIDs = append(reqIDs, vals[0])
				mu.Unlock()
			}
		}
		return stream.Send(&sppb.PartialResultSet{})
	}

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

	// Interleave pickClient() with actual streaming RPCs (which increment sc.nthRequest)
	// to verify that round-robin uses an independent counter and hits all 4 channels.
	seen := make(map[spannerClient]bool, numCh)
	for i := 0; i < numCh*2; i++ {
		c, err := transport.pickClient()
		if err != nil {
			t.Fatalf("pickClient(%d) failed: %v", i, err)
		}
		seen[c] = true
		riw, err := c.(requestIDHeaderProvider).requestIDHeaderInjector(context.Background())
		if err != nil {
			t.Fatalf("requestIDHeaderInjector(%d) failed: %v", i, err)
		}
		stream, err := c.ExecuteStreamingSql(context.Background(), &sppb.ExecuteSqlRequest{
			Session: "projects/p/instances/i/databases/d/sessions/s",
			Sql:     "SELECT 1",
		}, riw.withNextRetryAttempt(1))
		if err != nil {
			t.Fatalf("ExecuteStreamingSql(%d) failed: %v", i, err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("stream.Recv(%d) failed: %v", i, err)
		}
	}
	if len(seen) != numCh {
		t.Fatalf("expected %d distinct spannerClients under interleaved RPC round-robin, got %d", numCh, len(seen))
	}
	mu.Lock()
	gotReqIDs := append([]string(nil), reqIDs...)
	mu.Unlock()
	if len(gotReqIDs) != numCh*2 {
		t.Fatalf("expected %d x-goog-spanner-request-id headers, got %d", numCh*2, len(gotReqIDs))
	}
	for i, id := range gotReqIDs {
		wantSuffix := "." + string(rune('1'+i)) + ".1"
		if !strings.HasSuffix(id, wantSuffix) {
			t.Fatalf("request %d: expected x-goog-spanner-request-id ending with %q, got %q", i, wantSuffix, id)
		}
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

	// Verify per-call headers already present on uCtx are never overwritten by baseMD.
	callerCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer fresh-caller-token",
		"x-custom-caller", "keep-me",
		"google-cloud-resource-prefix", "projects/caller/instances/i/databases/caller-db",
	))
	callerOutCtx := applyDCPPrimeMetadata(callerCtx, req, baseMD)
	callerOutMD, _ := metadata.FromOutgoingContext(callerOutCtx)
	if got := callerOutMD.Get("authorization"); len(got) != 1 || got[0] != "Bearer fresh-caller-token" {
		t.Fatalf("expected caller authorization header not to be overwritten by baseMD, got %v", got)
	}
	if got := callerOutMD.Get("x-custom-caller"); len(got) != 1 || got[0] != "keep-me" {
		t.Fatalf("expected caller x-custom-caller header preserved, got %v", got)
	}
	if got := callerOutMD.Get("google-cloud-resource-prefix"); len(got) != 1 || got[0] != "projects/caller/instances/i/databases/caller-db" {
		t.Fatalf("expected caller google-cloud-resource-prefix not to be overwritten, got %v", got)
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
		if pool == nil || pool.Equal(x509.NewCertPool()) {
			t.Fatal("expected non-empty fallback CertPool when system cert pool is unavailable")
		}
		if badPool := resolveInMemoryFallbackRootCAs("not-valid-pem"); badPool != nil {
			t.Fatal("expected nil CertPool for invalid PEM")
		}
		tr := &SharedCoreTransport{}
		optsDirectPathBuiltInAuth := buildSharedCoreClientOptions(
			SharedCoreTransportConfig{DisableBuiltInAuth: false},
			tr,
			"spanner.googleapis.com:443",
			false,
			true,
			false,
		)
		optsNonDirectPath := buildSharedCoreClientOptions(
			SharedCoreTransportConfig{DisableBuiltInAuth: false},
			tr,
			"spanner.googleapis.com:443",
			false,
			false,
			false,
		)
		if len(optsDirectPathBuiltInAuth) != len(optsNonDirectPath)-1 {
			t.Fatalf("expected DirectPath with built-in auth to skip fallback WithTransportCredentials (got %d opts vs %d)", len(optsDirectPathBuiltInAuth), len(optsNonDirectPath))
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

func TestResolveSharedCoreUniverseDomain(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", "")
	cases := []struct {
		name              string
		cfgUniverseDomain string
		envUniverseDomain string
		schemeRemoved     string
		want              string
	}{
		{
			name:          "default googleapis.com endpoint returns empty",
			schemeRemoved: "spanner.googleapis.com:443",
			want:          "",
		},
		{
			name:          "mTLS googleapis.com endpoint is not misclassified as custom universe",
			schemeRemoved: "spanner.mtls.googleapis.com:443",
			want:          "",
		},
		{
			name:          "regional REP googleapis.com endpoint is not misclassified as custom universe",
			schemeRemoved: "spanner.us-east1.rep.googleapis.com:443",
			want:          "",
		},
		{
			name:          "regional mTLS REP googleapis.com endpoint is not misclassified as custom universe",
			schemeRemoved: "spanner.mtls.us-east1.rep.googleapis.com:443",
			want:          "",
		},
		{
			name:          "TPC endpoint infers custom universe domain",
			schemeRemoved: "spanner.apis-tpclp.goog:443",
			want:          "apis-tpclp.goog",
		},
		{
			name:              "explicit cfg.UniverseDomain takes precedence",
			cfgUniverseDomain: "my-universe.goog",
			schemeRemoved:     "spanner.googleapis.com:443",
			want:              "my-universe.goog",
		},
		{
			name:              "GOOGLE_CLOUD_UNIVERSE_DOMAIN env var used when cfg.UniverseDomain is empty",
			envUniverseDomain: "env-universe.goog",
			schemeRemoved:     "spanner.googleapis.com:443",
			want:              "env-universe.goog",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", tc.envUniverseDomain)
			if got := resolveSharedCoreUniverseDomain(tc.cfgUniverseDomain, tc.schemeRemoved); got != tc.want {
				t.Fatalf("resolveSharedCoreUniverseDomain(%q, %q) = %q, want %q", tc.cfgUniverseDomain, tc.schemeRemoved, got, tc.want)
			}
		})
	}
}

func TestSharedCoreEntrypoint_EmulatorEnvDoesNotDowngradeExplicitEndpoint(t *testing.T) {
	_, addr, cleanup := startMockSharedCoreServer(t)
	defer cleanup()

	// Set SPANNER_EMULATOR_HOST to an unrelated host while passing an explicit endpoint.
	t.Setenv("SPANNER_EMULATOR_HOST", "emulator.internal:9010")

	transport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint:  addr,
		PlainText: true,
		DynamicChannelPoolConfig: DynamicChannelPoolConfig{
			DCPEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("NewSharedCoreTransport failed: %v", err)
	}
	defer transport.Close()

	if transport.sc == nil || transport.sc.dynamicPool == nil {
		t.Fatal("expected DCP to remain enabled when explicit cfg.Endpoint differs from SPANNER_EMULATOR_HOST")
	}

	// Conversely, when cfg.Endpoint matches SPANNER_EMULATOR_HOST (as when Node.js passes emulatorHost),
	// useEmulator is true and DCP is bypassed.
	t.Setenv("SPANNER_EMULATOR_HOST", addr)
	emuTransport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint: "http://" + addr,
		DynamicChannelPoolConfig: DynamicChannelPoolConfig{
			DCPEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("NewSharedCoreTransport(emulator) failed: %v", err)
	}
	defer emuTransport.Close()
	if emuTransport.sc.dynamicPool != nil {
		t.Fatal("expected DCP to be bypassed when connecting to SPANNER_EMULATOR_HOST")
	}
}

func TestSharedCoreEntrypoint_StaticPoolClientOptionsAndFallbackDefault(t *testing.T) {
	t.Setenv("SPANNER_EMULATOR_HOST", "")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "")
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "invalid-bool")

	_, addr, cleanup := startMockSharedCoreServer(t)
	defer cleanup()

	// 1. When cfg.NumChannels == 0, option.WithGRPCConnectionPool in ClientOptions is respected.
	transport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint:           addr,
		PlainText:          true,
		EnableDirectAccess: true,
		Compression:        "gzip",
		ClientOptions: []option.ClientOption{
			option.WithGRPCConnectionPool(6),
		},
	})
	if err != nil {
		t.Fatalf("NewSharedCoreTransport failed: %v", err)
	}
	defer transport.Close()

	if len(transport.clients) != 6 {
		t.Fatalf("expected 6 clients from option.WithGRPCConnectionPool(6) when NumChannels==0, got %d", len(transport.clients))
	}
	if _, isFallback := transport.sc.connPool.(*fallbackWrapper); isFallback {
		t.Fatal("expected isFallbackEnabled to default to false")
	}
	if len(transport.sc.channelIDMap) != 6 {
		t.Fatalf("expected 6 distinct channels in sc.channelIDMap, got %d", len(transport.sc.channelIDMap))
	}
	if got := transport.sc.md.Get(requestsCompressionHeader); len(got) != 1 || got[0] != "gzip" {
		t.Fatalf("expected %s=gzip in sc.md, got %v", requestsCompressionHeader, got)
	}

	// 2. PlainText: true with Endpoint: "" still configures insecure credentials and WithoutAuthentication.
	emptyEpTransport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint:  "",
		PlainText: true,
		ClientOptions: []option.ClientOption{
			option.WithEndpoint("passthrough:///" + addr),
		},
	})
	if err != nil {
		t.Fatalf("NewSharedCoreTransport(Endpoint=\"\", PlainText=true) failed: %v", err)
	}
	defer emptyEpTransport.Close()
}

func TestStripEndpointSchemeAndExtractTLSServerName(t *testing.T) {
	schemeCases := []struct {
		in   string
		want string
	}{
		{"http://localhost:9010", "localhost:9010"},
		{"https://spanner.googleapis.com:443", "spanner.googleapis.com:443"},
		{"passthrough:///localhost:9010", "localhost:9010"},
		{"passthrough:///http://localhost:9010", "localhost:9010"},
		{"passthrough:///https://spanner.googleapis.com:443", "spanner.googleapis.com:443"},
		{"http://passthrough:///localhost:9010", "localhost:9010"},
	}
	for _, tc := range schemeCases {
		if got := stripEndpointScheme(tc.in); got != tc.want {
			t.Errorf("stripEndpointScheme(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	serverNameCases := []struct {
		in   string
		want string
	}{
		{"spanner.googleapis.com:443", "spanner.googleapis.com"},
		{"spanner.googleapis.com", "spanner.googleapis.com"},
		{"[2001:db8::1]:443", "2001:db8::1"},
		{"[2001:db8::1]", "2001:db8::1"},
	}
	for _, tc := range serverNameCases {
		if got := extractTLSServerName(tc.in); got != tc.want {
			t.Errorf("extractTLSServerName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
