// Copyright 2016 Google LLC
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

package metadata

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
)

func TestOnGCE_Stress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	var last bool
	for i := 0; i < 100; i++ {
		onGCEOnce = sync.Once{}

		now := OnGCE()
		if i > 0 && now != last {
			t.Errorf("%d. changed from %v to %v", i, last, now)
		}
		last = now
	}
	t.Logf("OnGCE() = %v", last)
}

func TestOnGCE_Force(t *testing.T) {
	onGCEOnce = sync.Once{}
	old := os.Getenv(metadataHostEnv)
	defer os.Setenv(metadataHostEnv, old)
	os.Setenv(metadataHostEnv, "127.0.0.1")
	if !OnGCE() {
		t.Error("OnGCE() = false; want true")
	}
}

func TestOverrideUserAgent(t *testing.T) {
	const userAgent = "my-user-agent"
	rt := &rrt{}
	c := NewClient(&http.Client{Transport: userAgentTransport{userAgent, rt}})
	c.Get("foo")
	if got, want := rt.gotUserAgent, userAgent; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestGetFailsOnBadURL(t *testing.T) {
	c := NewClient(http.DefaultClient)
	old := os.Getenv(metadataHostEnv)
	defer os.Setenv(metadataHostEnv, old)
	os.Setenv(metadataHostEnv, "host:-1")
	_, err := c.Get("suffix")
	log.Printf("%v", err)
	if err == nil {
		t.Errorf("got %v, want non-nil error", err)
	}
}

func TestGet_LeadingSlash(t *testing.T) {
	v := url.Values{}
	v.Add("audience", "http://example.com")
	want := "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/identity?audience=http%3A%2F%2Fexample.com"
	tests := []struct {
		name   string
		suffix string
	}{
		{
			name:   "without leading slash",
			suffix: "instance/service-accounts/default/identity?" + v.Encode(),
		},
		{
			name:   "with leading slash",
			suffix: "/instance/service-accounts/default/identity?" + v.Encode(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ct := &captureTransport{}
			c := NewClient(&http.Client{Transport: ct})
			c.Get(tc.suffix)
			if ct.url != want {
				t.Fatalf("got %v, want %v", ct.url, want)
			}
		})
	}
}

type captureTransport struct {
	url string
}

func (ct *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ct.url = req.URL.String()
	return &http.Response{Body: ioutil.NopCloser(bytes.NewReader(nil))}, nil
}

type userAgentTransport struct {
	userAgent string
	base      http.RoundTripper
}

func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}

type rrt struct {
	gotUserAgent string
}

func (r *rrt) RoundTrip(req *http.Request) (*http.Response, error) {
	r.gotUserAgent = req.Header.Get("User-Agent")
	return &http.Response{Body: ioutil.NopCloser(bytes.NewReader(nil))}, nil
}
