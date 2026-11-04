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
	mu           sync.Mutex
	handler      func(req *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error
	unaryHandler func(ctx context.Context, req *sppb.ExecuteSqlRequest) (*sppb.ResultSet, error)
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

func (s *mockSharedCoreSpannerServer) ExecuteSql(ctx context.Context, req *sppb.ExecuteSqlRequest) (*sppb.ResultSet, error) {
	s.mu.Lock()
	uh := s.unaryHandler
	s.mu.Unlock()
	if uh != nil {
		return uh(ctx, req)
	}
	return &sppb.ResultSet{}, nil
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

	// 1. Pure pickClient() round-robin without any intervening RPCs or sc.nthRequest increments.
	pureSeen := make(map[spannerClient]bool, numCh)
	var prevClient spannerClient
	for i := 0; i < numCh*2; i++ {
		c, err := transport.pickClient()
		if err != nil {
			t.Fatalf("pure pickClient(%d) failed: %v", i, err)
		}
		if c == prevClient {
			t.Fatalf("pure pickClient(%d) repeated previous client without advancing round-robin", i)
		}
		prevClient = c
		pureSeen[c] = true
	}
	if len(pureSeen) != numCh {
		t.Fatalf("expected %d distinct spannerClients from pure pickClient() round-robin, got %d", numCh, len(pureSeen))
	}

	// 2. Interleave pickClient() with actual streaming RPCs (which increment sc.nthRequest)
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

func TestSharedCoreEntrypoint_DynamicChannelPoolOptInAndScaleUp(t *testing.T) {
	t.Setenv("SPANNER_EMULATOR_HOST", "")
	mock, addr, cleanup := startMockSharedCoreServer(t)
	defer cleanup()

	var (
		primeMu       sync.Mutex
		primePrefixes [][]string
		primeEncs     [][]string
	)
	mock.unaryHandler = func(ctx context.Context, req *sppb.ExecuteSqlRequest) (*sppb.ResultSet, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			primeMu.Lock()
			primePrefixes = append(primePrefixes, append([]string(nil), md.Get(resourcePrefixHeader)...))
			primeEncs = append(primeEncs, append([]string(nil), md.Get(requestsCompressionHeader)...))
			primeMu.Unlock()
		}
		return &sppb.ResultSet{}, nil
	}

	transport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint:    addr,
		PlainText:   true,
		Compression: "gzip",
		DynamicChannelPoolConfig: DynamicChannelPoolConfig{
			DCPEnabled:          true,
			DCPInitialChannels:  2,
			DCPMinChannels:      2,
			DCPMaxChannels:      4,
			DCPMinRPCPerChannel: 1,
			DCPMaxRPCPerChannel: 2,
		},
	})
	if err != nil {
		t.Fatalf("NewSharedCoreTransport with DCPEnabled=true failed: %v", err)
	}
	defer transport.Close()

	if transport.sc == nil || transport.sc.dynamicPool == nil {
		t.Fatalf("expected transport.sc.dynamicPool to be initialized when DCPEnabled=true")
	}
	if got := transport.sc.dynamicPool.Num(); got != 2 {
		t.Fatalf("expected 2 initial channels in dynamicChannelPool, got %d", got)
	}

	seen := make(map[*dcpEntry]bool, 2)
	for i := 0; i < 16 && len(seen) < 2; i++ {
		c, err := transport.pickClient()
		if err != nil {
			t.Fatalf("pickClient(%d) failed: %v", i, err)
		}
		dcpClient, ok := c.(*dcpSpannerClient)
		if !ok || dcpClient.entry == nil {
			t.Fatalf("expected *dcpSpannerClient with non-nil entry, got %T", c)
		}
		seen[dcpClient.entry] = true
		dcpClient.entry.streamLoad.Add(10)
		defer dcpClient.entry.streamLoad.Add(-10)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 distinct dcpEntry channels under DCPPowerOfTwoLeastBusy, got %d", len(seen))
	}

	// Wire the prime session via updateDCPPrimeSession, which calls setPrimeSession and wakes scaleUpSignal.
	const primeSession = "projects/p/instances/i/databases/db-prime/sessions/s1"
	transport.updateDCPPrimeSession(primeSession)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && transport.sc.dynamicPool.Num() <= 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := transport.sc.dynamicPool.Num(); got <= 2 {
		t.Fatalf("expected dynamicChannelPool to scale up above 2 channels after updateDCPPrimeSession under load, got %d", got)
	}

	primeMu.Lock()
	gotPrefixes := append([][]string(nil), primePrefixes...)
	gotEncs := append([][]string(nil), primeEncs...)
	primeMu.Unlock()
	if len(gotPrefixes) == 0 {
		t.Fatal("expected at least one DCP prime ExecuteSql RPC")
	}
	for i, p := range gotPrefixes {
		if len(p) != 1 || p[0] != "projects/p/instances/i/databases/db-prime" {
			t.Fatalf("prime RPC %d: expected %s=[projects/p/instances/i/databases/db-prime], got %v", i, resourcePrefixHeader, p)
		}
	}
	for i, enc := range gotEncs {
		if len(enc) != 1 || enc[0] != "gzip" {
			t.Fatalf("prime RPC %d: expected single %s=[gzip] without duplication, got %v", i, requestsCompressionHeader, enc)
		}
	}
}

func TestSharedCoreEntrypoint_DCPPrimingUnaryInterceptorHeaders(t *testing.T) {
	req := &sppb.ExecuteSqlRequest{
		Session: "projects/p/instances/i/databases/db2/sessions/s2",
		Sql:     "SELECT 1",
	}
	uCtx := applyDCPPrimeMetadata(context.Background(), req)
	outMD, ok := metadata.FromOutgoingContext(uCtx)
	if !ok {
		t.Fatal("expected outgoing metadata in context")
	}
	if got := outMD.Get(resourcePrefixHeader); len(got) != 1 || got[0] != "projects/p/instances/i/databases/db2" {
		t.Fatalf("unexpected %s: %v", resourcePrefixHeader, got)
	}

	// Verify per-call headers already present on uCtx are never overwritten.
	callerCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer fresh-caller-token",
		"x-custom-caller", "keep-me",
		resourcePrefixHeader, "projects/caller/instances/i/databases/caller-db",
	))
	callerOutCtx := applyDCPPrimeMetadata(callerCtx, req)
	callerOutMD, _ := metadata.FromOutgoingContext(callerOutCtx)
	if got := callerOutMD.Get("authorization"); len(got) != 1 || got[0] != "Bearer fresh-caller-token" {
		t.Fatalf("expected caller authorization header preserved, got %v", got)
	}
	if got := callerOutMD.Get("x-custom-caller"); len(got) != 1 || got[0] != "keep-me" {
		t.Fatalf("expected caller x-custom-caller header preserved, got %v", got)
	}
	if got := callerOutMD.Get(resourcePrefixHeader); len(got) != 1 || got[0] != "projects/caller/instances/i/databases/caller-db" {
		t.Fatalf("expected caller %s not to be overwritten, got %v", resourcePrefixHeader, got)
	}

	// Non-ExecuteSqlRequest and invalid session strings are no-ops.
	if gotCtx := applyDCPPrimeMetadata(context.Background(), "not-a-proto"); gotCtx != context.Background() {
		t.Fatal("expected non-ExecuteSqlRequest to return context unchanged")
	}
	if gotCtx := applyDCPPrimeMetadata(context.Background(), &sppb.ExecuteSqlRequest{Session: "invalid-session"}); gotCtx != context.Background() {
		t.Fatal("expected ExecuteSqlRequest without /sessions/ to return context unchanged")
	}
}

func TestResolveInMemoryFallbackRootCAs_EmptySystemPool(t *testing.T) {
	origSysCertPool := systemCertPool
	origSetFallbackRoots := setFallbackRoots
	var fallbackSetCount int
	systemCertPool = func() (*x509.CertPool, error) {
		return x509.NewCertPool(), nil
	}
	setFallbackRoots = func(roots *x509.CertPool) {
		if roots != nil {
			fallbackSetCount++
		}
	}
	t.Cleanup(func() {
		systemCertPool = origSysCertPool
		setFallbackRoots = origSetFallbackRoots
		SetSharedCoreFallbackRootCAPEM("")
	})

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
	if fallbackSetCount != 1 {
		t.Fatalf("expected SetSharedCoreFallbackRootCAPEM to register fallback roots when system pool is empty, got count=%d", fallbackSetCount)
	}

	pool := resolveInMemoryFallbackRootCAs("")
	if pool == nil || pool.Equal(x509.NewCertPool()) {
		t.Fatal("expected non-empty fallback CertPool when system cert pool is unavailable")
	}
	if badPool := resolveInMemoryFallbackRootCAs("not-valid-pem"); badPool != nil {
		t.Fatal("expected nil CertPool for invalid PEM")
	}

	countBeforeDirectPath := fallbackSetCount
	optsDirectPathBuiltInAuth := buildSharedCoreClientOptions(
		SharedCoreTransportConfig{DisableBuiltInAuth: false},
		sharedCoreSettings{
			schemeRemoved:       "spanner.googleapis.com:443",
			isPlain:             false,
			isDirectPathEnabled: true,
			useDCP:              false,
		},
	)
	if fallbackSetCount <= countBeforeDirectPath {
		t.Fatal("expected buildSharedCoreClientOptions with DirectPath enabled to still invoke resolveInMemoryFallbackRootCAs")
	}
	optsNonDirectPath := buildSharedCoreClientOptions(
		SharedCoreTransportConfig{DisableBuiltInAuth: false},
		sharedCoreSettings{
			schemeRemoved:       "spanner.googleapis.com:443",
			isPlain:             false,
			isDirectPathEnabled: false,
			useDCP:              false,
		},
	)
	if len(optsDirectPathBuiltInAuth) != len(optsNonDirectPath)-1 {
		t.Fatalf("expected DirectPath with built-in auth to skip per-dial WithTransportCredentials (got %d opts vs %d)", len(optsDirectPathBuiltInAuth), len(optsNonDirectPath))
	}

	// Verify safeSetFallbackRoots recovers if setFallbackRoots panics (e.g. if called twice in process).
	setFallbackRoots = func(*x509.CertPool) {
		panic("SetFallbackRoots has already been called")
	}
	if poolAfterPanic := resolveInMemoryFallbackRootCAs(""); poolAfterPanic == nil {
		t.Fatal("expected resolveInMemoryFallbackRootCAs to recover from setFallbackRoots panic and return pool")
	}
}

func TestResolveSharedCoreSettings(t *testing.T) {
	t.Run("defaults and GCP fallback env", func(t *testing.T) {
		t.Setenv("SPANNER_EMULATOR_HOST", "")
		t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "")
		t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "")

		s := resolveSharedCoreSettings(SharedCoreTransportConfig{
			Endpoint:           "https://spanner.googleapis.com:443",
			EnableDirectAccess: true,
		})
		if s.numCh != numChannels || s.hasNumChannelsConfig {
			t.Fatalf("unexpected numCh=%d hasNumChannelsConfig=%v", s.numCh, s.hasNumChannelsConfig)
		}
		if s.schemeRemoved != "spanner.googleapis.com:443" || s.isPlain || s.useEmulator {
			t.Fatalf("unexpected endpoint resolution: %+v", s)
		}
		if !s.isDirectPathEnabled {
			t.Fatal("expected isDirectPathEnabled=true from cfg.EnableDirectAccess")
		}
		if s.isFallbackEnabled {
			t.Fatal("expected isFallbackEnabled to default to false")
		}

		t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")
		if s2 := resolveSharedCoreSettings(SharedCoreTransportConfig{}); !s2.isFallbackEnabled {
			t.Fatal("expected isFallbackEnabled=true when GOOGLE_SPANNER_ENABLE_GCP_FALLBACK=true")
		}
	})

	t.Run("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS env overrides config and invalid-bool resolves to false", func(t *testing.T) {
		t.Setenv("SPANNER_EMULATOR_HOST", "")
		t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "invalid-bool")
		s := resolveSharedCoreSettings(SharedCoreTransportConfig{
			EnableDirectAccess: true,
		})
		if s.isDirectPathEnabled {
			t.Fatal("expected invalid GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS to resolve isDirectPathEnabled=false (matching client.go)")
		}

		t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
		s2 := resolveSharedCoreSettings(SharedCoreTransportConfig{
			EnableDirectAccess: false,
		})
		if !s2.isDirectPathEnabled {
			t.Fatal("expected GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS=true to override EnableDirectAccess=false")
		}
	})

	t.Run("unrelated SPANNER_EMULATOR_HOST does not downgrade remote TLS endpoint", func(t *testing.T) {
		t.Setenv("SPANNER_EMULATOR_HOST", "emulator.internal:9010")
		s := resolveSharedCoreSettings(SharedCoreTransportConfig{
			Endpoint:  "spanner.googleapis.com:443",
			PlainText: false,
			DynamicChannelPoolConfig: DynamicChannelPoolConfig{
				DCPEnabled: true,
			},
		})
		if s.useEmulator || s.isPlain || !s.useDCP {
			t.Fatalf("expected unrelated SPANNER_EMULATOR_HOST not to set useEmulator or isPlain or disable DCP, got %+v", s)
		}

		sMatch := resolveSharedCoreSettings(SharedCoreTransportConfig{
			Endpoint:  "http://emulator.internal:9010",
			PlainText: false,
			DynamicChannelPoolConfig: DynamicChannelPoolConfig{
				DCPEnabled: true,
			},
		})
		if !sMatch.useEmulator || !sMatch.isPlain || sMatch.useDCP {
			t.Fatalf("expected matching SPANNER_EMULATOR_HOST to set useEmulator=true, isPlain=true, useDCP=false, got %+v", sMatch)
		}
	})
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

	mock, addr, cleanup := startMockSharedCoreServer(t)
	defer cleanup()

	var (
		mu       sync.Mutex
		streamMD []metadata.MD
	)
	mock.handler = func(req *sppb.ExecuteSqlRequest, stream sppb.Spanner_ExecuteStreamingSqlServer) error {
		if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
			mu.Lock()
			streamMD = append(streamMD, md.Copy())
			mu.Unlock()
		}
		return stream.Send(&sppb.PartialResultSet{})
	}

	// 1. Conflicting NumChannels and option.WithGRPCConnectionPool returns InvalidArgument (matching client.go).
	if badTransport, err := NewSharedCoreTransport(context.Background(), SharedCoreTransportConfig{
		Endpoint:    addr,
		PlainText:   true,
		NumChannels: 4,
		ClientOptions: []option.ClientOption{
			option.WithGRPCConnectionPool(6),
		},
	}); err == nil {
		_ = badTransport.Close()
		t.Fatal("expected InvalidArgument when both NumChannels=4 and WithGRPCConnectionPool(6) are specified")
	} else if ErrCode(err) != codes.InvalidArgument {
		t.Fatalf("expected codes.InvalidArgument, got %v", err)
	}

	// 2. When cfg.NumChannels == 0, option.WithGRPCConnectionPool in ClientOptions is respected,
	// and Compression: "gzip" attaches x-response-encoding=gzip on the wire.
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
	if len(transport.sc.channelIDMap) != 6 {
		t.Fatalf("expected 6 distinct channels in sc.channelIDMap, got %d", len(transport.sc.channelIDMap))
	}
	c, err := transport.pickClient()
	if err != nil {
		t.Fatalf("pickClient failed: %v", err)
	}
	riw, err := c.(requestIDHeaderProvider).requestIDHeaderInjector(context.Background())
	if err != nil {
		t.Fatalf("requestIDHeaderInjector failed: %v", err)
	}
	stream, err := c.ExecuteStreamingSql(context.Background(), &sppb.ExecuteSqlRequest{
		Session: "projects/p/instances/i/databases/d/sessions/s",
		Sql:     "SELECT 1",
	}, riw.withNextRetryAttempt(1))
	if err != nil {
		t.Fatalf("ExecuteStreamingSql failed: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("stream.Recv failed: %v", err)
	}
	mu.Lock()
	if len(streamMD) != 1 {
		mu.Unlock()
		t.Fatalf("expected 1 recorded stream metadata, got %d", len(streamMD))
	}
	gotEnc := streamMD[0].Get(requestsCompressionHeader)
	mu.Unlock()
	if len(gotEnc) != 1 || gotEnc[0] != "gzip" {
		t.Fatalf("expected %s=[gzip] on the wire, got %v", requestsCompressionHeader, gotEnc)
	}

	// 3. PlainText: true with Endpoint: "" still configures insecure credentials and WithoutAuthentication
	// and can execute an RPC when WithEndpoint is supplied via ClientOptions.
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

	c2, err := emptyEpTransport.pickClient()
	if err != nil {
		t.Fatalf("emptyEpTransport.pickClient failed: %v", err)
	}
	riw2, err := c2.(requestIDHeaderProvider).requestIDHeaderInjector(context.Background())
	if err != nil {
		t.Fatalf("requestIDHeaderInjector failed: %v", err)
	}
	stream2, err := c2.ExecuteStreamingSql(context.Background(), &sppb.ExecuteSqlRequest{
		Session: "projects/p/instances/i/databases/d/sessions/s",
		Sql:     "SELECT 1",
	}, riw2.withNextRetryAttempt(1))
	if err != nil {
		t.Fatalf("emptyEpTransport ExecuteStreamingSql failed: %v", err)
	}
	if _, err := stream2.Recv(); err != nil {
		t.Fatalf("emptyEpTransport stream.Recv failed: %v", err)
	}
}

func TestStripEndpointScheme(t *testing.T) {
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
}
