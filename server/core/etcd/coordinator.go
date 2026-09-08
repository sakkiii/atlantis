// Copyright 2017 HootSuite Media Inc.
// SPDX-License-Identifier: Apache-2.0
// Modified hereafter by contributors to runatlantis/atlantis.
//
// This file implements RuntimeCoordinator, the owner-routing seam the Atlantis
// command pipeline uses (design §"Command dispatch and local fencing", §533).
// It exposes exactly two operations to the runner integration: Route resolves
// PR ownership and admits or forwards a command (ingress), and Execute fences an
// admitted command with a persistent execution barrier, advances its admission
// record, and runs it on the owning process (owner side). All etcd-internal
// coordination — barrier transactions, admission-lifecycle CAS, and the
// blocked-by-older-generation takeover rule — stays behind this boundary so the
// runner never depends on unexported coordination types.
package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// maxProxiedAPIResponseBytes bounds a synchronously proxied API response. API
// plan/apply output can be large, so this is far higher than the internal command
// envelope limit, while still capping a misbehaving peer.
const maxProxiedAPIResponseBytes = 64 << 20 // 64 MiB

// InternalProxiedHeader marks a synchronously proxied API request so the owning
// replica handles it locally instead of forwarding again (loop prevention).
const InternalProxiedHeader = "X-Atlantis-Internal-Proxied"

// ExecuteOutcome reports why Execute did or did not run an admitted command. The
// runner uses it to decide whether to surface a fail-closed message to the user.
type ExecuteOutcome int

const (
	// ExecuteRan means the barrier was established and the run function was
	// invoked. Its boolean result is reflected in the admission terminal state.
	ExecuteRan ExecuteOutcome = iota
	// ExecuteBlocked means an unresolved execution barrier from an older owner
	// generation exists for this pull; the command was not run and the PR requires
	// explicit resolution (design §558).
	ExecuteBlocked
	// ExecuteClaimLost means the owning claim was no longer at the admitted
	// generation when the barrier transaction ran; the command was not run.
	ExecuteClaimLost
	// ExecuteUnavailable means a coordination backend error prevented fencing; the
	// command was not run and the caller must fail closed.
	ExecuteUnavailable
)

// RuntimeCoordinator adapts a *Runtime to the narrow ingress/owner-side contract
// the command runner depends on. It is created after the runtime's adapters are
// built and used for the life of the process.
type RuntimeCoordinator struct {
	rt *Runtime
}

// NewRuntimeCoordinator wraps a serving runtime. It panics if the runtime has no
// coordination adapters (maintenance mode), which never reaches the command
// pipeline.
func NewRuntimeCoordinator(rt *Runtime) *RuntimeCoordinator {
	return &RuntimeCoordinator{rt: rt}
}

// Route resolves or creates the PR owner claim and admits the command locally or
// forwards it to the owning replica (design §490). It returns the dispatch
// Result whose Status the caller maps to proceed/fail-closed.
func (c *RuntimeCoordinator) Route(ctx context.Context, cmd Command) (Result, error) {
	return c.rt.Route(ctx, cmd)
}

// ResolveOwner resolves (claiming if unowned) the owner of a pull for
// synchronous API proxying. It returns local=true when this replica should
// handle the request itself; otherwise it returns the owning replica's internal
// advertise URL to proxy to.
func (c *RuntimeCoordinator) ResolveOwner(ctx context.Context, vcsHostname, repoFullName string, pullNum int) (bool, string, error) {
	scope := PullScope{VCSHostname: vcsHostname, Repository: repoFullName, PullNum: pullNum}
	claim, won, err := c.rt.ownership.Claim(ctx, scope)
	if err != nil {
		return false, "", err
	}
	if won || claim.Record.InstanceID == c.rt.ownership.InstanceID() {
		return true, "", nil
	}
	return false, claim.AdvertiseURL(), nil
}

// ForwardAPIRequest proxies a synchronous API request (path is /api/plan or
// /api/apply) to the owning replica's advertise URL and returns the owner's
// status and response body. The destination host must be allowlisted; the API
// secret is forwarded so the owner authenticates the call, and a proxied marker
// header prevents the owner from forwarding again. It uses the internal TLS
// configuration (nil only in insecure development).
func (c *RuntimeCoordinator) ForwardAPIRequest(ctx context.Context, advertiseURL, path, apiToken string, body []byte) (int, []byte, error) {
	u, err := url.Parse(advertiseURL)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing advertise url: %w", err)
	}
	if c.rt.allowlist != nil && !c.rt.allowlist.Allows(u.Hostname()) {
		return 0, nil, fmt.Errorf("forwarding destination %q is not allowlisted", u.Hostname())
	}

	endpoint := strings.TrimRight(advertiseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(atlantisAPITokenHeader, apiToken)
	req.Header.Set(InternalProxiedHeader, "1")

	client := &http.Client{
		Timeout: c.rt.cfg.StartupTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("internal transport does not follow redirects")
		},
		Transport: &http.Transport{TLSClientConfig: c.rt.internalTLS},
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("forwarding API request: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxProxiedAPIResponseBytes))
	if err != nil {
		return 0, nil, fmt.Errorf("reading proxied API response: %w", err)
	}
	return resp.StatusCode, respBody, nil
}

// atlantisAPITokenHeader is the header carrying the API secret, mirrored from the
// controllers package so the proxied request authenticates at the owner.
const atlantisAPITokenHeader = "X-Atlantis-Token"

// Execute is the owner-side entry point invoked from the local executor once a
// command has been admitted (locally or via forwarding). It establishes an
// execution barrier bound to the exact owner generation, advances the admission
// record to running, invokes run, and then clears the barrier and records the
// terminal result. A barrier blocked by an older generation is left in place and
// the admission record is marked uncertain, never auto-replayed (design §546,
// §558). run reports whether the command completed without an internal failure;
// it is not a Terraform success signal.
func (c *RuntimeCoordinator) Execute(cmd Command, run func() bool) ExecuteOutcome {
	claim := ClaimFromGeneration(cmd.Scope, cmd.Generation)
	execID := cmd.Identity.encode()

	sctx, cancel := context.WithTimeout(context.Background(), c.rt.RequestTimeout())
	barrier, err := c.rt.Barriers().StartStep(sctx, claim, execID)
	cancel()
	if err != nil {
		if errors.Is(err, errBlockedByOlderGeneration) {
			c.markUncertain(cmd.Identity)
			return ExecuteBlocked
		}
		if errors.Is(err, errClaimLostAtBarrier) {
			// A newer owner generation exists; do not run and do not replay. The
			// admission record stays for reconciliation.
			c.markUncertain(cmd.Identity)
			return ExecuteClaimLost
		}
		return ExecuteUnavailable
	}

	c.advanceRunning(cmd.Identity)

	success := run()

	cctx, ccancel := context.WithTimeout(context.Background(), c.rt.RequestTimeout())
	// Clearing the barrier is the authenticated completion acknowledgement: this
	// process ran the step to completion, so the cross-generation fence is
	// released (design §558). A crash before this leaves the barrier active,
	// blocking a new owner until explicit resolution — the intended fail-safe.
	_ = c.rt.Barriers().CompleteStep(cctx, barrier)
	ccancel()

	c.complete(cmd.Identity, success)
	return ExecuteRan
}

// advanceRunning best-effort moves the admission record scheduled -> running. The
// router reserves and schedules the record on the ingress path; this runs in the
// executor goroutine, which may briefly observe the record still reserved, so it
// polls a bounded number of times. Admission bookkeeping is an audit/idempotency
// record, not the safety fence (the barrier is), so failures are tolerated.
func (c *RuntimeCoordinator) advanceRunning(id CommandIdentity) {
	adm := c.rt.Admission()
	deadline := time.Now().Add(2 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), c.rt.RequestTimeout())
		rec, err := adm.Get(ctx, id)
		cancel()
		if err != nil || rec == nil {
			return
		}
		if rec.State() == AdmissionScheduled {
			tctx, tcancel := context.WithTimeout(context.Background(), c.rt.RequestTimeout())
			_, _ = adm.Transition(tctx, *rec, AdmissionRunning)
			tcancel()
			return
		}
		if rec.State() != AdmissionReserved || time.Now().After(deadline) {
			// Already running/terminal, or the scheduled transition never landed.
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// complete best-effort writes the terminal admission result for a running record.
func (c *RuntimeCoordinator) complete(id CommandIdentity, success bool) {
	adm := c.rt.Admission()
	ctx, cancel := context.WithTimeout(context.Background(), c.rt.RequestTimeout())
	defer cancel()
	rec, err := adm.Get(ctx, id)
	if err != nil || rec == nil {
		return
	}
	if rec.State() != AdmissionRunning {
		return
	}
	_, _ = adm.Complete(ctx, *rec, success, "", "")
}

// markUncertain best-effort forces a non-terminal admission record to uncertain,
// so a blocked or claim-lost command is never automatically replayed.
func (c *RuntimeCoordinator) markUncertain(id CommandIdentity) {
	adm := c.rt.Admission()
	ctx, cancel := context.WithTimeout(context.Background(), c.rt.RequestTimeout())
	defer cancel()
	rec, err := adm.Get(ctx, id)
	if err != nil || rec == nil {
		return
	}
	switch rec.State() {
	case AdmissionSucceeded, AdmissionFailed, AdmissionUncertain:
		return
	}
	_, _ = adm.MarkUncertain(ctx, *rec)
}
