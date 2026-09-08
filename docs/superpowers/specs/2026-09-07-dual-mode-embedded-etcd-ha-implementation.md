# Dual-Mode Embedded etcd HA — Implementation Plan

Companion to [`2026-09-07-dual-mode-embedded-etcd-ha-design.md`](./2026-09-07-dual-mode-embedded-etcd-ha-design.md).
Section references (§) point into that design document.

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done

## Sequencing

The safety surface splits into two halves:

- **Half A — storage backend** (`db.Database` + a new scoped-lock interface). Self-contained;
  drops in behind `server/server.go:497` with no call-site churn.
- **Half B — active-active HA** (ownership, routing, fencing, entry-point coverage). Reaches into
  `command_runner.go`, `project_command_runner.go`, `api_controller.go`, `locks_controller.go`
  because every executable entry point must pass through owner resolution (§574 is a release gate).

External-first (matches §945): prove consistency semantics before embedded lifecycle risk.

```
Phase 0  Foundation (keyspace, envelopes, config, deps)
Phase 1  External backend + scoped locks   (WS1)   proves consistency
Phase 2  Ownership + transport             (WS3)   proves routing
Phase 3  Execution fencing + plan takeover (WS4)   proves safety
Phase 4  Entry-point coverage             (WS5)   RELEASE GATE
Phase 5  Embedded runtime                 (WS2)   parallelizable after P0
Phase 6  Migration + operations           (WS6)
Phase 7  Deployment integration (Helm)    (WS7)
```

Phases 1→4 are a hard chain. Phase 5 depends only on Phase 0 and may run in parallel.

## Key code touch points (from codebase mapping)

- `db.Database` interface: `server/core/db/db.go:18` — conflates project locks, pull-status, and
  the global apply lock; an etcd backend must implement **all three**.
- Backend wiring switch: `server/server.go:497` (no default/validation today).
- Flags: `cmd/server.go` (constants ~117, defaults ~189, registration ~421); config struct
  `server/user_config.go:91`.
- Lock key: `models.GenerateLockKey` (`server/events/models/models.go:338`) uses `RepoFullName`
  only — **drops VCS hostname**. The etcd path must use a separate host-scoped store, never the
  legacy `keyRegex` parser (`server/core/locking/locking.go:51`). Do not retrofit hostname into
  `GenerateLockKey` (would ripple into BoltDB/Redis keys).
- Existing backend templates: `server/core/boltdb/boltdb.go`, `server/core/redis/redis.go`.
- Command dispatch: `server/events/command_runner.go` (`RunAutoplanCommand:145`,
  `RunCommentCommand:464`); project exec `server/events/project_command_runner.go`
  (`Plan:363`, `Apply:383`, `ensurePlanLoaded:934`).
- API endpoints: `server/controllers/api_controller.go` (`Plan:282`, `Apply:315`).
- Lock UI: `server/controllers/locks_controller.go` (`GetLock:62`, `DeleteLock:105`).
- go.mod: no `go.etcd.io/etcd` client yet; only `go.etcd.io/bbolt`.

---

## Phase 0 — Foundation (`server/core/etcd/`)

- [x] 0.1 Added `go.etcd.io/etcd/client/v3 v3.6.5` to go.mod. Verified build. (server/v3 embed
      deferred to Phase 5.)
- [~] 0.2 Config surface: etcd `Config` types done (`config.go`). Flag wiring in `cmd/server.go` +
      `user_config.go` + `FromUserConfig` mapper deferred to Phase 1.4 (ties to the `server.go`
      switch case, which needs `EtcdDatabase`).
- [x] 0.3 Config validation (`config.go` + `config_test.go`): mode gating, TLS-mandatory-in-prod,
      insecure-dev loopback-only, voter-count {3,5,7}, lifecycle×purpose matrix (§251), join/restore
      field gating. Table-tested.
- [x] 0.4 Keyspace + envelopes (`keys.go`, `envelope.go` + tests): namespace normalization, versioned
      prefix, length-prefixed injective `ProjectScope`/`PullScope` encoding + unpadded URL-safe base64
      (§598), strict versioned JSON envelopes. Hostname-disambiguation + injectivity proven.
- [x] 0.5 `Backend` interface (`backend.go`): shared `sync.Once` close guard, bounded linearizable
      `Ready` probe, external `NewExternal` client construction with mandatory-TLS gate.

**Build note:** Go is not on PATH in this env; toolchain runs via
`docker run golang:1.26` (see `scratchpad/gorun.sh`). `go test ./server/core/etcd/` green.

## Phase 1 — External backend + scoped locks (WS1)

- [x] 1.1 `EtcdDatabase` (`database.go`): status via revision-CAS retry loops with jittered backoff
      (§438), global apply lock via create-only txn, `Ping` (linearizable via `Backend.Ready`),
      `Close` (delegates to backend). No leases (§456). Merge logic mirrors BoltDB exactly.
- [x] 1.2 `ScopedProjectLockStore` (`scoped_lock.go`): host-aware acquire (`CreateRevision==0`), get,
      conditional unlock (exact ModRevision), versioned opaque UI lock ID (no legacy fallback),
      `UnlockByPullScope` with cleaning-key guard + revision-pinned scan (`range.go`). Cleaning is
      modeled as key **presence** (`CreateRevision==0`), not a `Value` compare — the latter is
      unreliable on absent keys (bug caught by tests).
      **Remaining:** close→reopen generation CAS advance (§450) and acquisition recording+comparing
      that generation. Lifecycle open/closed record is written; generation bump is a stub (gen=0).
- [~] 1.3 Legacy `db.Database` project-lock methods delegate to scoped store; `TryLock` carries host
      exactly. Host-less methods (`Unlock`/`GetLock`/`UnlockByPull`) resolve scope by scan — exact for
      single-host, and etcd call sites should use `Scoped()` directly for multi-host.
      **Remaining:** route `locking.Client`/call sites through `Scoped()` in etcd mode.
- [x] 1.4 **External etcd is selectable end-to-end.** Wired `case "etcd":` at `server/server.go`
      (external → `NewExternal`+`NewDatabase`; embedded → explicit not-implemented error; unknown
      db-type → error). Added 13 common+external flags (`cmd/server.go` consts/stringFlags/boolFlags/
      setDefaults), `user_config.go` fields, `etcd.BuildConfig(Settings)` mapper, and 13 sorted doc
      sections in `runatlantis.io/docs/server-configuration.md`. `Config.Validate` split into
      `ValidateDatabase` (client/db, no ownership) + full `Validate`; `NewExternal` uses the former so
      single-replica external etcd works without ownership config (mirrors Redis wiring). All cmd
      coupling tests green (`TestExecute_Flags/Defaults`, `TestUserConfigAllTested`, `TestAllFlagsDocumented`).
      **Deferred to their phases:** embedded flags (Phase 5), ownership/routing flags (Phase 2).
- [x] 1.5 Consistency suite (`integration_test.go` + in-process etcd harness `testetcd_test.go`):
      single-winner contention, two-host isolation, conditional-unlock safety, cleaning-blocks-acquire,
      global-lock single-winner, no-lost-concurrent-status. **Green.**
      **Remaining:** revision-pinned multi-page pagination test, close/reopen race test (need gen work).

## Phase 2 — Ownership + transport (WS3)

- [x] **Namespace init/epoch** (`deployment.go` + tests): atomic empty-namespace init writing schema +
      deployment record with a coordination epoch UUID; validates existing (schema version, deployment
      ID); refuses data-without-schema (§635, §708 step 4). Prereq for ownership fencing.
- [x] 2.1 `OwnershipStore` (`ownership.go` + tests): renewable `concurrency.Session` lease; PR-claim
      `{host,repo,pull}` attached to lease; record with replica/instance/advertise/claimID/epoch (§463);
      key-absent claim txn (single winner); linearizable `Get`/`OwnedLocally`; `Release` (exact
      create-rev); `Close` revokes lease. Fencing generation = createRevision + epoch.
- [x] 2.2 Per-process instance ID (uuid) so a reused replica ID can't adopt an older claim; `Done()`
      channel surfaces lease loss for `/readyz`. **Remaining:** wire `Done()` into the readiness probe.
- [x] 2.5 Durable admission records (`admission.go` + tests): `reserved→scheduled→running→
      succeeded|failed`, plus forced `uncertain`; `Reserve` binds to the exact owner generation
      (rejects stale claim); CAS transitions reject double-advance; duplicate reserve reconciles to the
      existing record (§512–531). **Remaining:** 24h dedup-window cleaner; local drainer registration.
- [x] 2.3 Internal transport (`transport.go` + tests): credential-free `Command` envelope with
      immutable identity; `InternalServer` validates a bearer token in constant time (`subtle`) with
      body-size limits; `InternalClient` blocks redirects, enforces the advertise `Allowlist` (host/CIDR
      SSRF guard), and limits response size. TLS config plumbed (nil only in insecure-dev).
- [x] 2.4 Local claim guard + dispatch (`router.go` + tests): `Router.Route` (ingress) resolves/creates
      the claim → admits locally or forwards; `HandleForwarded` (receiver) validates exact ownership →
      409 on stale; 409 refresh-and-reroute-once; 503 on backend/exec unavailability; duplicates return
      stored state. Decoupled from Atlantis via an `Executor` interface.
      **Remaining (Phase 4/WS5):** wire `Router.Route` into the real `command_runner.go` entry points
      (the route-inventory gate) + load token/CA/TLS files in `server.go` + `/readyz` from `Done()`.
- [x] 2.6 HTTP tests (`router_test.go`): two nodes wired over real `httptest` servers assert 202 local
      admit, 202 forward-to-owner (owner executes, non-owner doesn't), 202 duplicate stored-state, 409
      stale claim, 401 unauthorized token, allowlist rejection. **Green.**

## Phase 3 — Execution fencing + plan takeover (WS4)

- [x] 3.2 Persistent execution barriers (`barrier.go` + tests): `ExecutionBarrierStore` keyed
      `{pull scope, generation, execution-id}` (§546); `StartStep` creates a barrier in one txn that
      compares the exact owning claim (rejects claim-lost); parallel same-generation barriers proceed.
- [x] 3.4 Older-generation takeover barrier (§558): `blockedByOlderGeneration` refuses a new owner
      generation's work while an active **or uncertain** older-gen barrier exists; `MarkUncertain`
      persists a blocking barrier; `CompleteStep` clears on exact revision. Tested.
- [ ] 3.1 Claim-generation propagation + admission checks at every §534 boundary in
      `project_command_runner.go`. **(Runner integration — with Phase 4.)**
- [ ] 3.3 Lease-loss cancellation: reap subprocesses, mark unready, unwind newly-acquired lock.
      **(Runner integration; store primitives + `OwnershipStore.Done()` ready.)**
- [ ] 3.5 Plan takeover (§688): clone + external-PlanStore restore w/ head-commit validation.
      **(Integration into `ensurePlanLoaded`.)**

## Phase 4 — Entry-point coverage (WS5) — RELEASE GATE  *(comment/autoplan wired)*

The dispatch primitives (`Router`, transport) are built and tested (Phase 2). The command-pipeline
integration seam is now wired for the asynchronous comment/autoplan path:
- [x] **Command ingress + owner-side executor** (`server/events/etcd_routing.go` + `server/core/etcd/coordinator.go`):
      `EtcdCommandRouter` decorates the `CommandRunner` used by the events controller. Every comment
      command and autoplan is serialized into a credential-free `etcd.Command` and dispatched through
      `RuntimeCoordinator.Route` (ownership resolve → local admit or forward). The router also implements
      `etcd.Executor`: on the owning replica it fences the run with a generation-bound execution barrier
      (`RuntimeCoordinator.Execute`), advances the admission record `scheduled→running→terminal`, and runs
      the wrapped runner. Non-admitted results and blocked/claim-lost fenced executions fail closed with a
      user-visible comment. Tests: `etcd_routing_test.go` (ingress + executor, fake coordinator),
      `coordinator_test.go` (run-to-terminal + older-generation block, embedded etcd).
- [x] **Server wiring** (`server/server.go`): the runtime is retained; `AttachExecutor` mounts the
      internal command handler at `etcd.InternalCommandPath`; `/readyz` reports `runtime.Ready` (backend
      authority + live ownership session); shutdown calls `runtime.Close`, releasing ownership before the
      client closes.
- [x] 4.4 Autoplan / review-triggered plan work is owner-routed via the autoplan ingress path.
- [x] 4.5 Reject drift + active-active etcd in config validation (§594) — `cmd/server.go` +
      `TestExecute_ValidateEtcdDriftDetection`.
- [ ] 4.1 Positive-PR API synchronous proxying (`api_controller.go`) — needs request/response proxying,
      not the async command envelope. **Not owner-routed yet.**
- [ ] 4.2 Pull close/reopen lifecycle via owner-routed path (still runs on the receiving replica).
- [ ] 4.3 Lock-UI routing (`locks_controller.go`) when deletion affects owner-local plan state.
- [ ] 4.6 Route-inventory test proving *every* inventory endpoint refuses to execute on a non-owner
      (the async comment/autoplan path is covered; API/lock-UI paths are pending 4.1–4.3).

### Fencing granularity note (3.1/3.3/3.5)

Execution barriers are established at whole-command granularity in `RuntimeCoordinator.Execute`, not at
each §534 project-workflow boundary inside `project_command_runner.go`. This is sufficient for the
cross-generation takeover invariant (a new owner generation is blocked while an older generation's
command barrier is unresolved), and parallel same-generation commands proceed. Finer per-project-step
barriers, lease-loss subprocess reaping, and plan takeover (3.1/3.3/3.5) require threading the claim
generation into the project contexts and a cancellable exec path, and remain follow-ups.

## Phase 5 — Embedded runtime (WS2)

- [x] 5.1 Embedded config parsing (`embedded_config.go` + tests): JSON parse w/ no process-terminating
      helpers → validated `embed.Config`; hard-rejects unsafe options (force-new-cluster, no-fsync,
      disabled strict reconfig, discovery); https/client-cert-auth required in prod; insecure-dev
      loopback-only; bootstrap peer-set count == voter count (§280, §276).
- [x] 5.4 `NewEmbedded` (`embedded.go`): data-dir state validation per lifecycle (empty for
      bootstrap/join, non-empty for restart/restore); `StartEtcd` + `ReadyNotify`/`Err`/`ctx`/timeout;
      shared client + linearizable probe; backend `embeddedClose` waits for `StopNotify` (§716, §751).
      Cluster state derived from lifecycle, strict-reconfig + corrupt-check forced on.
- [~] 5.2/5.3/5.6 Lifecycle data-dir preconditions + voter-count + peer-set validation done. **Remaining:**
      identity-manifest read/validate + activation, join membership-ticket consumption, full multi-node
      bootstrap→restart flow (needs multi-process infra; single-node `embed` path proven by test harness).

## Phase 6 — Migration + operations (WS6)

- [x] 6.1 Offline migration (`migration.go` + tests): `Migrator` — in-progress manifest (create-only,
      empty-target check), create-only record import (project locks/statuses/global locks), running
      count + order-independent checksum, atomic `Complete` writing schema+deployment/epoch under the
      manifest revision. Interrupted migration → normal startup refuses (proven). (§879)
- [x] 6.2 Recovery quarantine (`quarantine.go` + tests): `QuarantineStore` set (create-only) / IsActive
      / Clear (exact-recovery-ID CAS); every executable admission checks it. Fresh epoch on migrate. (§852)
- [ ] 6.3 Schema expand/migrate/contract + leased capability records (§651). **(Later release.)**
- [ ] 6.4 Runbooks: voter-count change, backup/defrag/compaction, one-member upgrades (§378). **(Docs.)**

## Phase 7 — Deployment integration (WS7)

- [x] 7.1 Reference K8s manifests (`docs/superpowers/examples/etcd-ha/`): member StatefulSet
      (Parallel/OnDelete/per-member PVC/anti-affinity/topology-spread), headless peer service
      (`publishNotReadyAddresses`), client Deployment + HPA (external mode, scale 0..N), client service
      selecting both roles, PDB `maxUnavailable:1`, README of invariants. (Atlantis's Helm chart lives
      in the separate `runatlantis/helm-charts` repo; these are the reference for it to adopt.)
- [ ] 7.2 Package into the actual Helm chart (separate repo) with TLS secret / identity-ticket wiring.

## Config surface — complete
All 27 etcd flags are wired to `cmd/server.go` + `user_config.go` + docs (external, embedded, and
ownership/routing). `TestUserConfigAllTested` + `TestAllFlagsDocumented` (sorted) pass.

## Runtime assembler — done + tested (`runtime.go`)
`etcd.NewRuntime(ctx, cfg)` assembles the whole stack over one client: backend (external `NewExternal`
or embedded `NewEmbedded`) → `InitOrValidateNamespace` (epoch) → database, ownership, admission,
barrier, quarantine adapters. `AttachExecutor(executor)` late-binds the router + returns the internal
command HTTP handler; `Route` dispatches ingress; `Ready` = backend authority + live ownership session;
`Close` follows the fixed shutdown order. Maintenance embedded start returns a non-serving runtime.
`server.go` `case "etcd"` now builds the Runtime and uses `runtime.Database()` (external + embedded
serve). Tests: full-stack assembly + DB round-trip, two-runtime end-to-end forward-to-owner dispatch,
migrated-namespace validation. (Transport auth hardened: tolerant Bearer parse for the empty dev token.)

**Command-pipeline integration seam — wired for comment/autoplan (Phase 4).** `server.go` now
decorates the events-controller `CommandRunner` with `EtcdCommandRouter`, calls
`runtime.AttachExecutor` with it, mounts the internal handler at `etcd.InternalCommandPath`, drives
`/readyz` from `runtime.Ready`, and calls `runtime.Close` on shutdown (releasing ownership first).
Comment commands and autoplan flow through `runtime.Route` → owner admit/forward → fenced
`RuntimeCoordinator.Execute`.

**Still pending:** positive-PR `/api/plan` and `/api/apply` synchronous proxying (4.1); pull
close/reopen and lock-UI owner routing (4.2/4.3); finer per-project-step barriers, lease-loss
subprocess reaping, and plan takeover (3.1/3.3/3.5 — currently fenced at whole-command granularity);
provider-delivery-ID admission dedup (each ingress is currently a distinct admission); the 24h
admission dedup-window cleaner; and embedded identity-manifest activation + join-ticket flow (5.2/5.3).
