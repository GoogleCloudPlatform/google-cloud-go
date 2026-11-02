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
	"crypto/tls"
	"crypto/x509"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
)

const sharedCorePlaceholderDB = "projects/shared-core/instances/shared-core/databases/shared-core"

var sharedCoreFallbackRootCAPEM atomic.Value

// SetSharedCoreFallbackRootCAPEM registers an in-memory PEM bundle of root CA
// certificates (e.g. from the host language runtime) used as a fallback when x509.SystemCertPool()
// contains no certificates (such as minimal container images without /etc/ssl/certs).
//
// Internal Use Only.
func SetSharedCoreFallbackRootCAPEM(pem string) {
	sharedCoreFallbackRootCAPEM.Store(pem)
}

// SharedCoreLogFunc receives log messages emitted by the Go Spanner transport layer
// so that host language bindings (e.g. Node.js) can route them into their unified logger.
//
// Internal Use Only.
type SharedCoreLogFunc func(level string, message string)

// sharedCoreLogWriter adapts SharedCoreLogFunc to an io.Writer for *log.Logger.
type sharedCoreLogWriter struct {
	logFn SharedCoreLogFunc
}

func (w *sharedCoreLogWriter) Write(p []byte) (int, error) {
	if w.logFn != nil {
		msg := strings.TrimSpace(string(p))
		if msg != "" {
			w.logFn("DEBUG", msg)
		}
	}
	return len(p), nil
}

// SharedCoreTransportConfig configures a SharedCoreTransport instance that reuses
// the Go Spanner client library's gRPC connection pool, interceptors, keepalive,
// DirectPath configuration, and resumable stream decoding infrastructure.
//
// Internal Use Only.
type SharedCoreTransportConfig struct {
	// Endpoint is the target Spanner endpoint (e.g. "spanner.googleapis.com:443" or emulator host:port).
	Endpoint string
	// UniverseDomain is the optional Google Cloud universe domain (e.g. "googleapis.com" or TPC domain).
	UniverseDomain string
	// RootCAPEM is an optional PEM-encoded root CA bundle used in memory when the OS system cert pool is empty.
	RootCAPEM string
	// NumChannels is the number of gRPC channels in the connection pool (defaults to 4).
	NumChannels int
	// DynamicChannelPoolConfig is the opt-in configuration for dynamic gRPC channel pooling,
	// matching ClientConfig.DynamicChannelPoolConfig (DCPEnabled defaults to false).
	DynamicChannelPoolConfig DynamicChannelPoolConfig
	// Compression specifies the gRPC compressor ("gzip" or "identity").
	Compression string
	// EnableDirectAccess enables DirectPath XDS and ALTS bound tokens.
	EnableDirectAccess bool
	// UserAgent is an optional user-agent suffix (e.g. "gl-node/22.23.3").
	UserAgent string
	// PlainText forces insecure transport credentials (e.g. for local mock servers or emulator).
	PlainText bool
	// DisableBuiltInAuth skips default Google ADC authentication when credentials
	// are supplied per-request via outgoing gRPC metadata from the host language.
	DisableBuiltInAuth bool
	// LogFunc is an optional callback for transport log messages.
	LogFunc SharedCoreLogFunc
	// ClientOptions allows passing additional option.ClientOption values (e.g. in tests).
	ClientOptions []option.ClientOption
}

// SharedCoreTransport wraps Go Spanner's sessionClient and dynamicChannelPool to provide
// a high-performance entry point for the shared native core without modifying existing Go client flows.
//
// Internal Use Only.
type SharedCoreTransport struct {
	closed       atomic.Bool
	sc           *sessionClient
	clients      []spannerClient
	logger       *log.Logger
	rrCounter    atomic.Uint32
	lastPrimedMD atomic.Pointer[metadata.MD]
}

func stripEndpointScheme(ep string) string {
	ep = strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(ep, "passthrough:///"), "https://"), "http://")
	return strings.TrimPrefix(ep, "passthrough:///")
}

func extractTLSServerName(schemeRemoved string) string {
	if host, _, err := net.SplitHostPort(schemeRemoved); err == nil && host != "" {
		return host
	}
	return strings.TrimSuffix(strings.TrimPrefix(schemeRemoved, "["), "]")
}

func buildSharedCoreSessionMetadata(compression string) metadata.MD {
	md := metadata.MD{}
	if compression == gzip.Name {
		md.Append(requestsCompressionHeader, gzip.Name)
	}
	return md
}

func resolveSharedCoreUniverseDomain(cfgUniverseDomain, schemeRemoved string) string {
	universeDomain := strings.TrimSpace(cfgUniverseDomain)
	if universeDomain == "" {
		universeDomain = strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN"))
	}
	if universeDomain == "" && strings.HasPrefix(schemeRemoved, "spanner.") && strings.HasSuffix(schemeRemoved, ":443") {
		candidate := strings.TrimSuffix(strings.TrimPrefix(schemeRemoved, "spanner."), ":443")
		if candidate != "" &&
			candidate != "googleapis.com" &&
			!strings.HasSuffix(candidate, ".googleapis.com") &&
			!strings.HasPrefix(candidate, "mtls.") &&
			!strings.Contains(candidate, ".rep.") {
			universeDomain = candidate
		}
	}
	if universeDomain == "googleapis.com" {
		return ""
	}
	return universeDomain
}

func resolveInMemoryFallbackRootCAs(explicitPEM string) *x509.CertPool {
	if sysPool, err := x509.SystemCertPool(); err == nil && sysPool != nil && !sysPool.Equal(x509.NewCertPool()) {
		return nil
	}
	pemData := explicitPEM
	if pemData == "" {
		if v, ok := sharedCoreFallbackRootCAPEM.Load().(string); ok {
			pemData = v
		}
	}
	if pemData == "" {
		return nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(pemData)) {
		return nil
	}
	return pool
}

func applyDCPPrimeMetadata(uCtx context.Context, rawReq any, baseMD metadata.MD) context.Context {
	req, ok := rawReq.(*sppb.ExecuteSqlRequest)
	if !ok {
		return uCtx
	}
	sess := req.GetSession()
	idx := strings.Index(sess, "/sessions/")
	if idx <= 0 {
		return uCtx
	}
	dbPrefix := sess[:idx]
	md, ok := metadata.FromOutgoingContext(uCtx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.MD{}
	}
	for k, vals := range baseMD {
		if k != "google-cloud-resource-prefix" && k != "x-goog-request-params" && k != xSpannerRequestIDHeader {
			if len(md.Get(k)) == 0 {
				md[k] = vals
			}
		}
	}
	if len(md.Get("google-cloud-resource-prefix")) == 0 {
		md.Set("google-cloud-resource-prefix", dbPrefix)
	}
	return metadata.NewOutgoingContext(uCtx, md)
}

func buildSharedCoreClientOptions(
	cfg SharedCoreTransportConfig,
	transport *SharedCoreTransport,
	schemeRemoved string,
	isPlainOrLoopback bool,
	isDirectPathEnabled bool,
	useDCP bool,
) []option.ClientOption {
	var opts []option.ClientOption
	if useDCP {
		// Unary interceptor for DCP priming (ExecuteSql "SELECT 1"): derives the
		// google-cloud-resource-prefix header directly from the ExecuteSqlRequest.Session
		// being primed and attaches any non-prefix metadata without mutating sc.md.
		primeUnaryInterceptor := func(
			uCtx context.Context,
			method string,
			rawReq, rawReply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			callOpts ...grpc.CallOption,
		) error {
			var baseMD metadata.MD
			if loaded := transport.lastPrimedMD.Load(); loaded != nil {
				baseMD = *loaded
			}
			uCtx = applyDCPPrimeMetadata(uCtx, rawReq, baseMD)
			return invoker(uCtx, method, rawReq, rawReply, cc, callOpts...)
		}
		opts = append(opts, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(primeUnaryInterceptor)))
	}

	opts = append(opts,
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		)),
	)

	if isPlainOrLoopback {
		if schemeRemoved != "" {
			opts = append(opts, option.WithEndpoint("passthrough:///"+schemeRemoved))
		}
		opts = append(opts,
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
			internaloption.SkipDialSettingsValidation(),
		)
	} else if schemeRemoved != "" {
		opts = append(opts, option.WithEndpoint(schemeRemoved))
	}

	if !isPlainOrLoopback {
		if universeDomain := resolveSharedCoreUniverseDomain(cfg.UniverseDomain, schemeRemoved); universeDomain != "" {
			opts = append(opts, option.WithUniverseDomain(universeDomain))
		}
	}

	if cfg.DisableBuiltInAuth && !isPlainOrLoopback {
		opts = append(opts,
			option.WithoutAuthentication(),
			internaloption.SkipDialSettingsValidation(),
		)
	}

	if !isPlainOrLoopback && (!isDirectPathEnabled || cfg.DisableBuiltInAuth) {
		if fallbackPool := resolveInMemoryFallbackRootCAs(cfg.RootCAPEM); fallbackPool != nil {
			opts = append(opts,
				option.WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
					RootCAs:    fallbackPool,
					ServerName: extractTLSServerName(schemeRemoved),
				}))),
			)
		}
	}

	return append(opts, cfg.ClientOptions...)
}

func initSharedCoreDCPTransport(
	ctx context.Context,
	cfg SharedCoreTransportConfig,
	transport *SharedCoreTransport,
	logger *log.Logger,
	metricsTracerFactory *builtinMetricsTracerFactory,
	isDirectPathEnabled bool,
	isPlainOrLoopback bool,
	opts []option.ClientOption,
) (*SharedCoreTransport, error) {
	dcpOpts := append([]option.ClientOption{}, opts...)
	dcpOpts = append(dcpOpts, option.WithGRPCConnectionPool(1))
	_, sc, err := createDCPConnPool(
		ctx,
		sharedCorePlaceholderDB,
		ClientConfig{
			UserAgent:                  cfg.UserAgent,
			Compression:                cfg.Compression,
			EnableDirectAccess:         isDirectPathEnabled,
			DynamicChannelPoolConfig:   cfg.DynamicChannelPoolConfig,
			BatchTimeout:               time.Minute,
			Logger:                     logger,
			OpenTelemetryMeterProvider: noop.NewMeterProvider(),
		},
		nil,
		buildSharedCoreSessionMetadata(cfg.Compression),
		metricsTracerFactory,
		dcpOpts...,
	)
	if err != nil {
		return nil, ToSpannerError(err)
	}

	// Kick off non-blocking background TCP/TLS/ALPN handshakes across all initial
	// DCP entries when connecting to a remote non-plaintext endpoint.
	if !isPlainOrLoopback && sc.dynamicPool != nil {
		for _, e := range sc.dynamicPool.getEntries() {
			if e.pool != nil {
				if conn := e.pool.Conn(); conn != nil {
					conn.Connect()
				}
			}
		}
	}

	transport.sc = sc
	return transport, nil
}

func initSharedCoreStaticTransport(
	ctx context.Context,
	cfg SharedCoreTransportConfig,
	transport *SharedCoreTransport,
	logger *log.Logger,
	metricsTracerFactory *builtinMetricsTracerFactory,
	numCh int,
	hasNumChannelsConfig bool,
	isDirectPathEnabled bool,
	isFallbackEnabled bool,
	isPlainOrLoopback bool,
	opts []option.ClientOption,
) (*SharedCoreTransport, error) {
	var (
		connPool gtransport.ConnPool
		err      error
	)
	if isFallbackEnabled && isDirectPathEnabled && !isPlainOrLoopback && !cfg.DisableBuiltInAuth {
		// createFallbackConnPool adds its own requestIDHeaderInjector stream and unary interceptors.
		connPool, _, err = createFallbackConnPool(
			ctx,
			ClientConfig{
				NumChannels:        numCh,
				Compression:        cfg.Compression,
				EnableDirectAccess: isDirectPathEnabled,
			},
			hasNumChannelsConfig,
			metricsTracerFactory,
			opts...,
		)
		if err != nil {
			return nil, ToSpannerError(err)
		}
	} else {
		reqIDInjector := new(requestIDHeaderInjector)
		staticOpts := append([]option.ClientOption{}, opts...)
		staticOpts = append(staticOpts,
			option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(reqIDInjector.interceptStream)),
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(reqIDInjector.interceptUnary)),
		)
		allOpts := allClientOpts(numCh, cfg.Compression, isDirectPathEnabled, staticOpts...)
		if hasNumChannelsConfig {
			allOpts = append(allOpts, option.WithGRPCConnectionPool(numCh))
		}

		connPool, err = gtransport.DialPool(ctx, allOpts...)
		if err != nil {
			return nil, ToSpannerError(err)
		}
	}

	sc := newSessionClient(
		connPool,
		sharedCorePlaceholderDB,
		cfg.UserAgent,
		nil,
		"",
		false,
		buildSharedCoreSessionMetadata(cfg.Compression),
		time.Minute,
		logger,
		nil,
	)
	sc.metricsTracerFactory = metricsTracerFactory

	actualPoolSize := connPool.Num()
	if actualPoolSize <= 0 {
		actualPoolSize = numCh
	}

	clients := make([]spannerClient, actualPoolSize)
	for i := 0; i < actualPoolSize; i++ {
		c, err := sc.nextClient()
		if err != nil {
			_ = sc.close()
			return nil, ToSpannerError(err)
		}
		clients[i] = c
	}

	// Kick off non-blocking background TCP/TLS/ALPN handshakes across all pooled
	// gRPC channels when connecting to a remote non-plaintext endpoint.
	if !isPlainOrLoopback {
		for conn := range sc.channelIDMap {
			if conn != nil {
				conn.Connect()
			}
		}
	}

	transport.sc = sc
	transport.clients = clients
	return transport, nil
}

// NewSharedCoreTransport initializes a new SharedCoreTransport backed by Go Spanner's
// dynamicChannelPool, allClientOpts, gtransport.DialPool, requestIDHeaderInjector, and grpcSpannerClient.
//
// Internal Use Only.
func NewSharedCoreTransport(ctx context.Context, cfg SharedCoreTransportConfig) (*SharedCoreTransport, error) {
	hasNumChannelsConfig := cfg.NumChannels > 0
	numCh := cfg.NumChannels
	if numCh <= 0 {
		numCh = numChannels
	}

	var logger *log.Logger
	if cfg.LogFunc != nil {
		logger = log.New(&sharedCoreLogWriter{logFn: cfg.LogFunc}, "", 0)
	}

	transport := &SharedCoreTransport{
		logger: logger,
	}

	emulatorAddr := os.Getenv("SPANNER_EMULATOR_HOST")
	targetEndpoint := cfg.Endpoint
	if targetEndpoint == "" && emulatorAddr != "" {
		targetEndpoint = emulatorAddr
	}
	var schemeRemoved string
	if targetEndpoint != "" {
		schemeRemoved = stripEndpointScheme(targetEndpoint)
	}
	useEmulator := emulatorAddr != "" && (cfg.Endpoint == "" || cfg.Endpoint == emulatorAddr || schemeRemoved == stripEndpointScheme(emulatorAddr))
	isPlainOrLoopback := cfg.PlainText || useEmulator || isLoopbackEndpoint(schemeRemoved)

	isDirectPathEnabled := cfg.EnableDirectAccess
	if enableDirectPathXdsString := os.Getenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS"); enableDirectPathXdsString != "" {
		if b, err := strconv.ParseBool(enableDirectPathXdsString); err == nil {
			isDirectPathEnabled = b
		}
	}
	isFallbackEnabled := false
	if val, ok := os.LookupEnv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK"); ok {
		if b, err := strconv.ParseBool(val); err == nil {
			isFallbackEnabled = b
		}
	}

	useDCP := cfg.DynamicChannelPoolConfig.DCPEnabled && !useEmulator
	opts := buildSharedCoreClientOptions(cfg, transport, schemeRemoved, isPlainOrLoopback, isDirectPathEnabled, useDCP)

	metricsTracerFactory := &builtinMetricsTracerFactory{
		enabled:  false,
		shutdown: func(context.Context) {},
	}

	if useDCP {
		return initSharedCoreDCPTransport(
			ctx,
			cfg,
			transport,
			logger,
			metricsTracerFactory,
			isDirectPathEnabled,
			isPlainOrLoopback,
			opts,
		)
	}

	return initSharedCoreStaticTransport(
		ctx,
		cfg,
		transport,
		logger,
		metricsTracerFactory,
		numCh,
		hasNumChannelsConfig,
		isDirectPathEnabled,
		isFallbackEnabled,
		isPlainOrLoopback,
		opts,
	)
}

func isLoopbackEndpoint(ep string) bool {
	return strings.HasPrefix(ep, "localhost:") ||
		strings.HasPrefix(ep, "127.0.0.1:") ||
		strings.HasPrefix(ep, "[::1]:")
}

// pickClient selects a spannerClient from the pool. When backed by dynamicChannelPool,
// it delegates to sc.nextClient() which uses lock-free DCPPowerOfTwoLeastBusy selection
// across active channels; otherwise (static pool) it uses atomic round-robin.
func (t *SharedCoreTransport) pickClient() (spannerClient, error) {
	if t.closed.Load() || t.sc == nil {
		return nil, spannerErrorf(codes.FailedPrecondition, "SharedCoreTransport is closed")
	}
	if t.sc.dynamicPool != nil {
		return t.sc.nextClient()
	}
	if len(t.clients) == 0 {
		return nil, spannerErrorf(codes.FailedPrecondition, "SharedCoreTransport is closed")
	}
	if len(t.clients) == 1 {
		return t.clients[0], nil
	}
	idx := t.rrCounter.Add(1) % uint32(len(t.clients))
	return t.clients[idx], nil
}

// Close shuts down the underlying sessionClient and gRPC connection pool.
//
// Internal Use Only.
func (t *SharedCoreTransport) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	if t.sc != nil {
		return t.sc.close()
	}
	return nil
}
