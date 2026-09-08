// Copyright 2017 HootSuite Media Inc.
// SPDX-License-Identifier: Apache-2.0
// Modified hereafter by contributors to runatlantis/atlantis.

package etcd_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/runatlantis/atlantis/server/core/etcd"
	. "github.com/runatlantis/atlantis/testing"
)

// TestCoordinator_ResolveOwner proves the first replica to resolve a pull claims
// it (local), and a second replica resolves to the first's advertise URL.
func TestCoordinator_ResolveOwner(t *testing.T) {
	backend := startEmbeddedEtcd(t)
	ctx := context.Background()

	rtA, err := etcd.NewRuntime(ctx, runtimeConfig(t, backend, "A", "https://replica-a:4142"))
	Ok(t, err)
	t.Cleanup(func() { _ = rtA.Close() })
	rtB, err := etcd.NewRuntime(ctx, runtimeConfig(t, backend, "B", "https://replica-b:4142"))
	Ok(t, err)
	t.Cleanup(func() { _ = rtB.Close() })

	coordA := etcd.NewRuntimeCoordinator(rtA)
	coordB := etcd.NewRuntimeCoordinator(rtB)

	local, advertise, err := coordA.ResolveOwner(ctx, "github.com", "o/r", 1)
	Ok(t, err)
	Assert(t, local, "A should own the pull it resolved first")
	Equals(t, "", advertise)

	local, advertise, err = coordB.ResolveOwner(ctx, "github.com", "o/r", 1)
	Ok(t, err)
	Assert(t, !local, "B must not own A's pull")
	Equals(t, "https://replica-a:4142", advertise)
}

// TestCoordinator_ForwardAPIRequest proves the forwarder posts the body to
// advertiseURL+path with the API token and loop-guard header, and returns the
// owner's status and body.
func TestCoordinator_ForwardAPIRequest(t *testing.T) {
	backend := startEmbeddedEtcd(t)
	ctx := context.Background()

	var gotToken, gotProxied, gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Atlantis-Token")
		gotProxied = r.Header.Get(etcd.InternalProxiedHeader)
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	rt, err := etcd.NewRuntime(ctx, runtimeConfig(t, backend, "A", "http://127.0.0.1:4142"))
	Ok(t, err)
	t.Cleanup(func() { _ = rt.Close() })
	coord := etcd.NewRuntimeCoordinator(rt)

	status, body, err := coord.ForwardAPIRequest(ctx, srv.URL, "/api/plan", "sekret", []byte(`{"Repository":"o/r"}`))
	Ok(t, err)
	Equals(t, http.StatusOK, status)
	Equals(t, `{"ok":true}`, string(body))
	Equals(t, "sekret", gotToken)
	Equals(t, "1", gotProxied)
	Equals(t, "/api/plan", gotPath)
	Equals(t, `{"Repository":"o/r"}`, gotBody)
}

// TestCoordinator_ForwardAPIRequest_AllowlistRejects proves a non-allowlisted
// destination host is refused before dialing.
func TestCoordinator_ForwardAPIRequest_AllowlistRejects(t *testing.T) {
	backend := startEmbeddedEtcd(t)
	ctx := context.Background()
	rt, err := etcd.NewRuntime(ctx, runtimeConfig(t, backend, "A", "http://127.0.0.1:4142"))
	Ok(t, err)
	t.Cleanup(func() { _ = rt.Close() })
	coord := etcd.NewRuntimeCoordinator(rt)

	_, _, err = coord.ForwardAPIRequest(ctx, "https://evil.example.com:4142", "/api/plan", "sekret", []byte(`{}`))
	Assert(t, err != nil, "a non-allowlisted destination must be refused")
}
