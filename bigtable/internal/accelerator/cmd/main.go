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

// Binary accelerator runs the in-process gRPC proxy daemon: it listens on a
// Unix domain socket, accepts standard google.bigtable.v2.Bigtable RPCs, and
// forwards each one through an Channel that rides the Jetstream
// session transport.
//
// One daemon serves one (project, instance, appProfile) tuple. Cross-language
// clients spawn the binary, wait for the UDS to become connectable, then talk
// standard Bigtable V2 protos over it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/bigtable/internal/accelerator"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// identityResolveTimeout bounds the pre-bind identity resolution so a hung
// metadata server (GCE/GKE) can never stall daemon startup. On timeout the
// principal is left unresolved and the client falls back to its native path.
const identityResolveTimeout = 15 * time.Second

// daemonConfig holds parsed command-line flags for the accelerator daemon.
type daemonConfig struct {
	udsPath        string
	project        string
	instance       string
	appProfile     string
	fallback       bool
	dataEndpoint   string
	universeDomain string
	scopesFlag     string
	quotaProject   string
	userAgent      string
}

// parseConfig parses the daemon's CLI arguments and validates required flags.
func parseConfig(args []string) (*daemonConfig, error) {
	fs := flag.NewFlagSet("accelerator", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	cfg := &daemonConfig{}
	fs.StringVar(&cfg.udsPath, "uds-path", "", "path to unix domain socket (required)")
	fs.StringVar(&cfg.project, "project", "", "GCP project ID (required)")
	fs.StringVar(&cfg.instance, "instance", "", "Bigtable instance ID (required)")
	fs.StringVar(&cfg.appProfile, "app-profile", "", "Bigtable app profile (optional)")
	// Fallback flag for client without any native implementations
	fs.BoolVar(&cfg.fallback, "fallback", false, "fall back to the classic bigtable.Client when the session backend returns Unimplemented (e.g. instance lacks session serving)")
	// Endpoint overrides. When empty, NewChannel falls back to the
	// default Bigtable data-plane endpoint (bigtable.googleapis.com:443). The
	// spawning client supplies these when it targets a non-default endpoint or a
	// non-GDU universe.
	fs.StringVar(&cfg.dataEndpoint, "data-endpoint", "", "override Bigtable data-plane endpoint, e.g. bigtable.googleapis.com:443 (optional)")
	fs.StringVar(&cfg.universeDomain, "universe-domain", "", "override the service universe domain, e.g. googleapis.com (optional)")
	fs.StringVar(&cfg.scopesFlag, "scopes", "", "comma-separated OAuth scopes to override the default data scope (optional)")
	fs.StringVar(&cfg.quotaProject, "quota-project", "", "quota/billing project override (optional)")
	// User-agent prefix. Cross-language clients (e.g. the Python SDK) pass their
	// own client user agent here so the service sees both the caller and the
	// daemon build, e.g. "python-bigtable/2.1.0 go-acc/v1.38.0".
	fs.StringVar(&cfg.userAgent, "user-agent", "", "user-agent prefix prepended to the daemon's own token (optional)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if cfg.udsPath == "" {
		return nil, errors.New("Missing required flag: --uds-path")
	}
	if cfg.project == "" {
		return nil, errors.New("Missing required flag: --project")
	}
	if cfg.instance == "" {
		return nil, errors.New("Missing required flag: --instance")
	}
	return cfg, nil
}

// buildClientOptions translates parsed CLI flags into Google API ClientOptions,
// effective OAuth scopes, and the composed user-agent string.
func buildClientOptions(cfg *daemonConfig) (opts []option.ClientOption, effectiveScopes []string, composedUA string) {
	// Only forward overrides that were actually set; leaving them unset lets
	// the default endpoint/universe-domain resolution apply.
	if cfg.dataEndpoint != "" {
		opts = append(opts, option.WithEndpoint(cfg.dataEndpoint))
	}
	if cfg.universeDomain != "" {
		opts = append(opts, option.WithUniverseDomain(cfg.universeDomain))
	}
	// Caller opts are appended after the channel's defaults, so WithScopes here
	// overrides the hardcoded data scope.
	scopes := splitScopes(cfg.scopesFlag)
	if len(scopes) > 0 {
		opts = append(opts, option.WithScopes(scopes...))
	}
	if cfg.quotaProject != "" {
		opts = append(opts, option.WithQuotaProject(cfg.quotaProject))
	}
	composedUA = accelerator.ComposeUserAgent(cfg.userAgent)
	// Prepend the caller's user-agent prefix to the daemon's own token. Appended
	// after the channel's defaults so it overrides the default WithUserAgent; a
	// blank flag leaves the default (daemon-token-only) user agent untouched.
	if strings.TrimSpace(cfg.userAgent) != "" {
		opts = append(opts, option.WithUserAgent(composedUA))
	}

	effectiveScopes = scopes
	if len(effectiveScopes) == 0 {
		effectiveScopes = []string{accelerator.DataScope}
	}
	return opts, effectiveScopes, composedUA
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		log.Fatal(err)
	}

	opts, effectiveScopes, composedUA := buildClientOptions(cfg)
	ctx := context.Background()

	// Resolve ADC once, using the effective scopes the channel will dial with,
	// then hand the resolved credentials to both the channel (via
	// WithCredentials) and the identity resolver below. This avoids a second
	// FindDefaultCredentials lookup in identity resolution and guarantees the
	// published principal matches the credentials the channel actually dials.
	creds, err := google.FindDefaultCredentials(ctx, effectiveScopes...)
	if err != nil {
		log.Fatalf("failed to resolve credentials: %v", err)
	}
	// Appended last so it overrides the channel's default credential resolution;
	// the dial can no longer re-resolve to a different principal than the one we
	// publish in identity.json.
	opts = append(opts, option.WithCredentials(creds))

	// Pass the composed user agent as the CSM client_name so client-side
	// metrics are attributed to both the calling client library and the daemon
	// build, e.g. "python-bigtable/2.1.0 go-acc/v1.38.0". A blank --user-agent
	// yields the daemon token alone ("go-acc/v1.38.0").
	channel, err := accelerator.NewChannel(ctx, cfg.project, cfg.instance, cfg.appProfile, composedUA, opts...)
	if err != nil {
		log.Fatalf("failed to construct accelerator channel: %v", err)
	}

	// Build the server with either a bare session channel or a FallbackChannel.
	// The FallbackChannel dials its own bigtable.Client (classic path) with the
	// same opts so both paths share credentials and endpoint.
	var srv *accelerator.Server
	if cfg.fallback {
		fc, err := accelerator.NewFallbackChannel(ctx, channel, cfg.project, cfg.instance, cfg.appProfile, opts...)
		if err != nil {
			log.Fatalf("failed to construct classic fallback channel: %v", err)
		}
		srv = accelerator.NewServer(cfg.udsPath, fc)
	} else {
		srv = accelerator.NewServer(cfg.udsPath, channel)
	}
	log.Printf("Starting accelerator daemon on UDS=%s project=%s instance=%s app-profile=%q data-endpoint=%q universe-domain=%q scopes=%q quota-project=%q user-agent=%q",
		cfg.udsPath, cfg.project, cfg.instance, cfg.appProfile, cfg.dataEndpoint, cfg.universeDomain, cfg.scopesFlag, cfg.quotaProject, composedUA)

	// Resolve and publish the daemon's identity BEFORE binding the socket, so
	// the file is present by the time the client detects a connectable UDS.
	// Non-fatal on failure: the client's verify step falls back to native. The
	// resolution is bound by a timeout so a hung metadata server can't stall
	// startup.
	identityCtx, cancelIdentity := context.WithTimeout(ctx, identityResolveTimeout)
	principal, err := principalFromCreds(identityCtx, creds)
	if err != nil {
		log.Printf("warning: principal resolution failed: %v", err)
	}
	cancelIdentity()
	// Record effectiveScopes (not the raw override), so identity.json reflects
	// the scope the daemon actually dials with rather than an empty list when no
	// --scopes flag was passed.
	if err := writeIdentity(cfg.udsPath, principal, effectiveScopes); err != nil {
		log.Printf("warning: failed to write identity document: %v", err)
	}

	// Register signal handling before starting the server so that in LIFO defer
	// order srv.Stop() completes before signal.Stop(sigCh) restores SIG_DFL.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start accelerator server: %v", err)
	}
	defer srv.Stop()

	select {
	case <-srv.ShutdownChan():
		log.Printf("accelerator: watchdog-initiated shutdown, exiting...")
	case s := <-sigCh:
		log.Printf("accelerator: received %s, shutting down...", s)
	}
}

// splitScopes parses a comma-separated scopes flag into a trimmed, non-empty
// slice. Returns an empty (non-nil) slice when the flag is empty or blank, so
// callers get a consistent slice value and it marshals to [] rather than null.
func splitScopes(raw string) []string {
	scopes := []string{}
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			scopes = append(scopes, s)
		}
	}
	return scopes
}
