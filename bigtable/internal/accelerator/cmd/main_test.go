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

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	v2pb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/internal/accelerator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func TestSplitScopes(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want []string
	}{
		{"empty", "", []string{}},
		{"whitespace_only", "  ,  , ", []string{}},
		{"single", "https://www.googleapis.com/auth/bigtable.data", []string{"https://www.googleapis.com/auth/bigtable.data"}},
		{
			"multiple_with_whitespace",
			" https://www.googleapis.com/auth/bigtable.data , , https://www.googleapis.com/auth/cloud-platform ",
			[]string{
				"https://www.googleapis.com/auth/bigtable.data",
				"https://www.googleapis.com/auth/cloud-platform",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := splitScopes(tc.raw)
			if got == nil {
				t.Fatal("splitScopes returned nil; want non-nil empty slice")
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitScopes(%q) = %#v, want %#v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParseConfig_AllFlags(t *testing.T) {
	args := []string{
		"--uds-path", "/tmp/bt-accel.sock",
		"--project", "my-project",
		"--instance", "my-instance",
		"--app-profile", "my-profile",
		"--fallback=true",
		"--data-endpoint", "staging-bigtable.sandbox.googleapis.com:443",
		"--universe-domain", "apis-tpclp.goog",
		"--scopes", "scope-a, scope-b",
		"--quota-project", "my-quota-proj",
		"--user-agent", "python-bigtable/3.0.0",
	}
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.udsPath != "/tmp/bt-accel.sock" || cfg.project != "my-project" || cfg.instance != "my-instance" {
		t.Errorf("unexpected required flags: %+v", cfg)
	}
	if cfg.appProfile != "my-profile" || !cfg.fallback {
		t.Errorf("unexpected appProfile/fallback: %+v", cfg)
	}
	if cfg.dataEndpoint != "staging-bigtable.sandbox.googleapis.com:443" || cfg.universeDomain != "apis-tpclp.goog" {
		t.Errorf("unexpected endpoint/universeDomain: %+v", cfg)
	}
	if cfg.quotaProject != "my-quota-proj" || cfg.userAgent != "python-bigtable/3.0.0" {
		t.Errorf("unexpected quotaProject/userAgent: %+v", cfg)
	}

	opts, effectiveScopes, composedUA := buildClientOptions(cfg)
	if len(opts) != 5 {
		t.Errorf("len(opts) = %d; want 5 (endpoint, universeDomain, scopes, quotaProject, userAgent)", len(opts))
	}
	wantScopes := []string{"scope-a", "scope-b"}
	if !reflect.DeepEqual(effectiveScopes, wantScopes) {
		t.Errorf("effectiveScopes = %v, want %v", effectiveScopes, wantScopes)
	}
	if !strings.HasPrefix(composedUA, "python-bigtable/3.0.0 ") {
		t.Errorf("composedUA = %q; want prefix 'python-bigtable/3.0.0 '", composedUA)
	}
}

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--uds-path", "/tmp/bt.sock",
		"--project", "p",
		"--instance", "i",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.fallback {
		t.Error("cfg.fallback = true; want false by default")
	}
	opts, effectiveScopes, composedUA := buildClientOptions(cfg)
	if len(opts) != 0 {
		t.Errorf("len(opts) = %d; want 0 when optional flags are unset", len(opts))
	}
	wantScopes := []string{accelerator.DataScope}
	if !reflect.DeepEqual(effectiveScopes, wantScopes) {
		t.Errorf("effectiveScopes = %v, want %v", effectiveScopes, wantScopes)
	}
	if composedUA != accelerator.ComposeUserAgent("") {
		t.Errorf("composedUA = %q, want %q", composedUA, accelerator.ComposeUserAgent(""))
	}
}

func TestParseConfig_MissingRequiredAndUnknownFlags(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantSub string
	}{
		{
			name:    "missing_uds_path",
			args:    []string{"--project", "p", "--instance", "i"},
			wantSub: "Missing required flag: --uds-path",
		},
		{
			name:    "missing_project",
			args:    []string{"--uds-path", "/tmp/s", "--instance", "i"},
			wantSub: "Missing required flag: --project",
		},
		{
			name:    "missing_instance",
			args:    []string{"--uds-path", "/tmp/s", "--project", "p"},
			wantSub: "Missing required flag: --instance",
		},
		{
			name:    "unknown_flag_caller_user_agent",
			args:    []string{"--uds-path", "/tmp/s", "--project", "p", "--instance", "i", "--caller-user-agent", "python"},
			wantSub: "flag provided but not defined: -caller-user-agent",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(tc.args)
			if err == nil {
				t.Fatalf("parseConfig(%v) succeeded; want error containing %q", tc.args, tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("parseConfig(%v) error = %q; want substring %q", tc.args, err.Error(), tc.wantSub)
			}
		})
	}
}

func writeTestServiceAccountJSON(t *testing.T, email string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("x509.MarshalPKCS8PrivateKey: %v", err)
	}
	pemBlock := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8})
	doc := map[string]string{
		"type":         "service_account",
		"project_id":   "test-proj",
		"private_key":  string(pemBlock),
		"client_email": email,
		"token_uri":    "https://oauth2.googleapis.com/token",
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatalf("os.WriteFile: %v", err)
	}
	return path
}

func TestMainSubprocess_SignalAndStdinShutdown(t *testing.T) {
	if os.Getenv("GO_ACCELERATOR_TEST_MAIN") == "1" {
		// Strip the test runner flags before invoking main() with the arguments
		// after "--".
		for i, a := range os.Args {
			if a == "--" {
				os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
				break
			}
		}
		main()
		os.Exit(0)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	v2pb.RegisterBigtableServer(grpcSrv, &v2pb.UnimplementedBigtableServer{})
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	const wantEmail = "daemon-test@test-proj.iam.gserviceaccount.com"
	saPath := writeTestServiceAccountJSON(t, wantEmail)

	for _, mode := range []string{"stdin_eof", "sigterm"} {
		t.Run(mode, func(t *testing.T) {
			tmpDir := t.TempDir()
			udsPath := filepath.Join(tmpDir, "daemon.sock")

			cmd := exec.Command(
				os.Args[0],
				"-test.run=^TestMainSubprocess_SignalAndStdinShutdown$",
				"--",
				"--uds-path", udsPath,
				"--project", "test-proj",
				"--instance", "test-inst",
				"--app-profile", "test-app",
				"--data-endpoint", lis.Addr().String(),
				"--universe-domain", "googleapis.com",
				"--scopes", "https://www.googleapis.com/auth/bigtable.data",
				"--quota-project", "quota-proj",
				"--user-agent", "python-bigtable/3.0.0",
			)
			cmd.Env = append(
				os.Environ(),
				"GO_ACCELERATOR_TEST_MAIN=1",
				"GOOGLE_APPLICATION_CREDENTIALS="+saPath,
			)
			stdinPipe, err := cmd.StdinPipe()
			if err != nil {
				t.Fatalf("StdinPipe: %v", err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatalf("cmd.Start: %v", err)
			}
			defer func() {
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
			}()

			const secret = "test-subprocess-secret"
			if _, err := io.WriteString(stdinPipe, secret+"\n"); err != nil {
				t.Fatalf("WriteString secret: %v", err)
			}

			// Wait for the UDS socket file to appear.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(udsPath); err == nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if _, err := os.Stat(udsPath); err != nil {
				t.Fatalf("daemon did not bind UDS within 5s: %v", err)
			}

			// Verify identity.json was written next to the socket.
			idBytes, err := os.ReadFile(filepath.Join(tmpDir, "identity.json"))
			if err != nil {
				t.Fatalf("ReadFile(identity.json): %v", err)
			}
			var doc identityDoc
			if err := json.Unmarshal(idBytes, &doc); err != nil {
				t.Fatalf("Unmarshal(identity.json): %v", err)
			}
			if doc.Principal != wantEmail {
				t.Errorf("identity.json principal = %q, want %q", doc.Principal, wantEmail)
			}
			if !reflect.DeepEqual(doc.Scopes, []string{"https://www.googleapis.com/auth/bigtable.data"}) {
				t.Errorf("identity.json scopes = %v", doc.Scopes)
			}

			// Verify Health/Check succeeds with the handshake secret.
			conn, err := grpc.NewClient("unix:"+udsPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("grpc.NewClient: %v", err)
			}
			defer conn.Close()
			hc := healthpb.NewHealthClient(conn)
			ctx, cancel := context.WithTimeout(
				metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-accelerator-token", secret)),
				2*time.Second,
			)
			resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
			cancel()
			if err != nil {
				t.Fatalf("Health.Check: %v", err)
			}
			if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
				t.Errorf("Health.Check status = %v, want SERVING", resp.GetStatus())
			}

			// Trigger shutdown via either stdin EOF or SIGTERM.
			if mode == "stdin_eof" {
				_ = stdinPipe.Close()
			} else {
				defer stdinPipe.Close()
				if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatalf("Signal(SIGTERM): %v", err)
				}
			}

			waitCh := make(chan error, 1)
			go func() { waitCh <- cmd.Wait() }()
			select {
			case err := <-waitCh:
				if err != nil {
					t.Errorf("daemon exited with non-zero status on %s: %v", mode, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("daemon did not exit within 5s after %s", mode)
			}
		})
	}
}
