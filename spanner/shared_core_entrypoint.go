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

var (
	sharedCoreFallbackRootCAPEM atomic.Value
	systemCertPool              = x509.SystemCertPool
	setFallbackRoots            = x509.SetFallbackRoots
)

func safeSetFallbackRoots(pool *x509.CertPool) {
	defer func() {
		_ = recover()
	}()
	setFallbackRoots(pool)
}

// SetSharedCoreFallbackRootCAPEM registers an in-memory PEM bundle of root CA
// certificates (e.g. from the host language runtime) used as a fallback when x509.SystemCertPool()
// contains no certificates (such as minimal container images without /etc/ssl/certs).
//
// Internal Use Only.
func SetSharedCoreFallbackRootCAPEM(pem string) {
	sharedCoreFallbackRootCAPEM.Store(pem)
	if pem != "" {
		_ = resolveInMemoryFallbackRootCAs(pem)
	}
}

// SharedCoreLogFunc receives log messages emitted by the Go Spanner transport layer
// so that host language bindings (e.g. Node.js) can route them into their unified logger.
// Implementations must be safe for concurrent calls from multiple goroutines.
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
	// NumChannels is the number of gRPC channels in the static connection pool (defaults to 4).
	// Ignored when DynamicChannelPoolConfig.DCPEnabled is true.
	NumChannels int
	// DynamicChannelPoolConfig is the opt-in configuration for dynamic gRPC channel pooling,
	// matching ClientConfig.DynamicChannelPoolConfig (DCPEnabled defaults to false).
	DynamicChannelPoolConfig DynamicChannelPoolConfig
	// Compression specifies the gRPC compressor ("gzip" or "identity").
	Compression string
	// EnableDirectAccess enables DirectPath XDS and ALTS bound tokens.
	// When GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS is set in the process-start environment,
	// its strconv.ParseBool value overrides this field (matching ClientConfig.EnableDirectAccess).
	EnableDirectAccess bool
	// UserAgent is an optional user-agent product token in "name/version" format (e.g. "my-lib/1.0.0").
	UserAgent string
	// PlainText forces insecure transport credentials (e.g. for local mock servers or emulator).
	PlainText bool
	// DisableBuiltInAuth skips default Google ADC authentication when credentials
	// are supplied per-request via outgoing gRPC metadata from the host language.
	DisableBuiltInAuth bool
	// LogFunc is an optional callback for transport log messages. Must be safe for concurrent calls.
	LogFunc SharedCoreLogFunc
	// ClientOptions allows passing additional option.ClientOption values (e.g. in tests).
	ClientOptions []option.ClientOption
}

// SharedCoreTransport wraps Go Spanner's sessionClient and dynamicChannelPool to provide
// a high-performance entry point for the shared native core without modifying existing Go client flows.
//
// Internal Use Only.
type SharedCoreTransport struct {
	closed    atomic.Bool
	sc        *sessionClient
	clients   []spannerClient
	logger    *log.Logger
	rrCounter atomic.Uint32
}

type sharedCoreSettings struct {
	numCh                int
	hasNumChannelsConfig bool
	schemeRemoved        string
	useEmulator          bool
	isPlain              bool
	isDirectPathEnabled  bool
	isFallbackEnabled    bool
	useDCP               bool
}

func resolveSharedCoreSettings(cfg SharedCoreTransportConfig) sharedCoreSettings {
	hasNumChannelsConfig := cfg.NumChannels > 0
	numCh := cfg.NumChannels
	if numCh <= 0 {
		numCh = numChannels
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
	isPlain := cfg.PlainText || useEmulator

	isDirectPathEnabled := cfg.EnableDirectAccess
	if enableDirectPathXdsString := os.Getenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS"); enableDirectPathXdsString != "" {
		isDirectPathEnabled, _ = strconv.ParseBool(enableDirectPathXdsString)
	}
	isFallbackEnabled := false
	if val, ok := os.LookupEnv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK"); ok {
		if b, err := strconv.ParseBool(val); err == nil {
			isFallbackEnabled = b
		}
	}

	useDCP := cfg.DynamicChannelPoolConfig.DCPEnabled && !useEmulator
	return sharedCoreSettings{
		numCh:                numCh,
		hasNumChannelsConfig: hasNumChannelsConfig,
		schemeRemoved:        schemeRemoved,
		useEmulator:          useEmulator,
		isPlain:              isPlain,
		isDirectPathEnabled:  isDirectPathEnabled,
		isFallbackEnabled:    isFallbackEnabled,
		useDCP:               useDCP,
	}
}

func stripEndpointScheme(ep string) string {
	ep = strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(ep, "passthrough:///"), "https://"), "http://")
	return strings.TrimPrefix(ep, "passthrough:///")
}

func buildSharedCoreSessionMetadata(compression string) metadata.MD {
	md := metadata.MD{}
	if compression == gzip.Name {
		md.Append(requestsCompressionHeader, gzip.Name)
	}
	return md
}

func resolveInMemoryFallbackRootCAs(explicitPEM string) *x509.CertPool {
	if sysPool, err := systemCertPool(); err == nil && sysPool != nil && !sysPool.Equal(x509.NewCertPool()) {
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
	safeSetFallbackRoots(pool)
	return pool
}

func applyDCPPrimeMetadata(uCtx context.Context, rawReq any) context.Context {
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
		if len(md.Get(resourcePrefixHeader)) > 0 {
			return uCtx
		}
		md = md.Copy()
	} else {
		md = metadata.MD{}
	}
	md.Set(resourcePrefixHeader, dbPrefix)
	return metadata.NewOutgoingContext(uCtx, md)
}

func (t *SharedCoreTransport) updateDCPPrimeSession(sessionStr string) {
	if t.sc == nil || t.sc.dynamicPool == nil || !strings.Contains(sessionStr, "/sessions/") {
		return
	}
	if prev, _ := t.sc.dynamicPool.primeSession.Load().(string); prev != sessionStr {
		t.sc.dynamicPool.setPrimeSession(sessionStr)
	}
}

func buildSharedCoreClientOptions(
	cfg SharedCoreTransportConfig,
	settings sharedCoreSettings,
) []option.ClientOption {
	var opts []option.ClientOption
	if settings.useDCP {
		// Unary interceptor for DCP priming (ExecuteSql "SELECT 1"): derives the
		// google-cloud-resource-prefix header directly from the ExecuteSqlRequest.Session
		// being primed without mutating sc.md.
		primeUnaryInterceptor := func(
			uCtx context.Context,
			method string,
			rawReq, rawReply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			callOpts ...grpc.CallOption,
		) error {
			uCtx = applyDCPPrimeMetadata(uCtx, rawReq)
			return invoker(uCtx, method, rawReq, rawReply, cc, callOpts...)
		}
		opts = append(opts, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(primeUnaryInterceptor)))
	}

	if cfg.Compression == gzip.Name {
		gzipUnaryInterceptor := func(
			uCtx context.Context,
			method string,
			rawReq, rawReply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			callOpts ...grpc.CallOption,
		) error {
			if md, ok := metadata.FromOutgoingContext(uCtx); !ok || len(md.Get(requestsCompressionHeader)) == 0 {
				uCtx = metadata.AppendToOutgoingContext(uCtx, requestsCompressionHeader, gzip.Name)
			}
			return invoker(uCtx, method, rawReq, rawReply, cc, callOpts...)
		}
		gzipStreamInterceptor := func(
			sCtx context.Context,
			desc *grpc.StreamDesc,
			cc *grpc.ClientConn,
			method string,
			streamer grpc.Streamer,
			callOpts ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			if md, ok := metadata.FromOutgoingContext(sCtx); !ok || len(md.Get(requestsCompressionHeader)) == 0 {
				sCtx = metadata.AppendToOutgoingContext(sCtx, requestsCompressionHeader, gzip.Name)
			}
			return streamer(sCtx, desc, cc, method, callOpts...)
		}
		opts = append(opts,
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(gzipUnaryInterceptor)),
			option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(gzipStreamInterceptor)),
		)
	}

	opts = append(opts,
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		)),
	)

	if settings.isPlain {
		if settings.schemeRemoved != "" {
			opts = append(opts, option.WithEndpoint("passthrough:///"+settings.schemeRemoved))
		}
		opts = append(opts,
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
			internaloption.SkipDialSettingsValidation(),
		)
	} else if settings.schemeRemoved != "" {
		opts = append(opts, option.WithEndpoint(settings.schemeRemoved))
	}

	if !settings.isPlain {
		if ud := strings.TrimSpace(cfg.UniverseDomain); ud != "" {
			opts = append(opts, option.WithUniverseDomain(ud))
		}
	}

	if cfg.DisableBuiltInAuth && !settings.isPlain {
		opts = append(opts,
			option.WithoutAuthentication(),
			internaloption.SkipDialSettingsValidation(),
		)
	}

	if !settings.isPlain {
		fallbackPool := resolveInMemoryFallbackRootCAs(cfg.RootCAPEM)
		if fallbackPool != nil && (!settings.isDirectPathEnabled || cfg.DisableBuiltInAuth) {
			opts = append(opts,
				option.WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
					RootCAs: fallbackPool,
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
	settings sharedCoreSettings,
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
			EnableDirectAccess:         settings.isDirectPathEnabled,
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
	if !settings.isPlain && sc.dynamicPool != nil {
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
	settings sharedCoreSettings,
	opts []option.ClientOption,
) (*SharedCoreTransport, error) {
	var (
		connPool gtransport.ConnPool
		err      error
	)
	if settings.isFallbackEnabled && settings.isDirectPathEnabled && !settings.isPlain && !cfg.DisableBuiltInAuth {
		// createFallbackConnPool adds its own requestIDHeaderInjector stream and unary interceptors.
		connPool, _, err = createFallbackConnPool(
			ctx,
			ClientConfig{
				NumChannels:        settings.numCh,
				Compression:        cfg.Compression,
				EnableDirectAccess: settings.isDirectPathEnabled,
			},
			settings.hasNumChannelsConfig,
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
		allOpts := allClientOpts(settings.numCh, cfg.Compression, settings.isDirectPathEnabled, staticOpts...)

		connPool, err = gtransport.DialPool(ctx, allOpts...)
		if err != nil {
			return nil, ToSpannerError(err)
		}
		if settings.hasNumChannelsConfig && connPool.Num() != settings.numCh {
			_ = connPool.Close()
			return nil, spannerErrorf(codes.InvalidArgument, "Connection pool size cannot be specified by both NumChannels=%v and WithGRPCConnectionPool=%v", settings.numCh, connPool.Num())
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
		actualPoolSize = settings.numCh
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
	if !settings.isPlain {
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
// When cfg.DynamicChannelPoolConfig.DCPEnabled is true, ctx also governs the lifetime of
// background DCP channel maintenance and autoscaling goroutines until Close is called.
//
// Internal Use Only.
func NewSharedCoreTransport(ctx context.Context, cfg SharedCoreTransportConfig) (*SharedCoreTransport, error) {
	var logger *log.Logger
	if cfg.LogFunc != nil {
		logger = log.New(&sharedCoreLogWriter{logFn: cfg.LogFunc}, "", 0)
	}

	transport := &SharedCoreTransport{
		logger: logger,
	}

	settings := resolveSharedCoreSettings(cfg)
	opts := buildSharedCoreClientOptions(cfg, settings)

	metricsTracerFactory := &builtinMetricsTracerFactory{
		enabled:  false,
		shutdown: func(context.Context) {},
	}

	if settings.useDCP {
		return initSharedCoreDCPTransport(
			ctx,
			cfg,
			transport,
			logger,
			metricsTracerFactory,
			settings,
			opts,
		)
	}

	return initSharedCoreStaticTransport(
		ctx,
		cfg,
		transport,
		logger,
		metricsTracerFactory,
		settings,
		opts,
	)
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
