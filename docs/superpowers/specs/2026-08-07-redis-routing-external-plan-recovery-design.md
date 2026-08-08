# Redis Replica Routing with Optional External Plan Recovery

Status: Approved design with configuration cleanup

Date: 2026-08-07

## Context

Redis replica routing assigns an entire pull request to one Atlantis replica. The owner keeps its checkout and plan files locally, and other replicas forward actionable webhook commands to it. The initial implementation deliberately rejected `--enable-external-stores` because owner loss was defined to require a new plan.

The revised contract makes ownership and plan durability independent:

- Replica routing controls command placement and pull request affinity.
- The configured PlanStore controls whether plans are local-only or portable.

This permits transparent plan restoration after owner loss when an external PlanStore is enabled, while retaining the existing re-plan behavior for local plans.

## Goals

- Preserve one owner for all projects and workspaces in a pull request.
- Let newly added replicas accept new pull requests without moving existing ownership.
- Reassign ownership only after graceful release or lease expiry.
- Restore a valid external plan after takeover when external storage is enabled.
- Require re-plan after takeover when plans are local-only.
- Preserve existing head-commit validation and external plan cleanup behavior.
- Remove the dedicated replica-routing enable flag and infer routing intent from its configuration group.
- Default replica identity to the pod or container hostname while retaining an optional override.
- Introduce no new server flags or concurrency controls.

## Non-Goals

- Proactive ownership rebalancing during scale-up.
- Copying working directories between replicas.
- Automatically replaying a webhook or command that failed during takeover.
- Exactly-once command execution.
- Canceling Terraform already running on an owner that becomes partitioned from Redis.
- Adding another ownership or PlanStore backend.

## Invariants

1. The ownership key is `{VCS hostname, repository full name, pull number}`. Workspace and project are intentionally excluded.
2. An ownership record belongs to one exact Atlantis process claim, not only a stable pod name.
3. Existing ownership is never moved merely because another replica becomes available.
4. The first command accepted under a new process claim clears that replica's local pull request directory exactly once.
5. External plan data is not deleted as part of owner takeover.
6. A restored plan may be applied only when its stored head commit matches the current pull request head commit.
7. Redis or ownership uncertainty fails closed; Atlantis never executes locally as a routing fallback.

## Ownership Lifecycle

| Event | Ownership behavior |
| --- | --- |
| New pull request | The first replica receiving actionable work atomically claims it. |
| Scale up | Existing claims remain unchanged. New replicas can claim new pull requests. |
| Non-owner receives work | It forwards the command to the current owner. |
| Graceful scale down or restart | The owner drains, stops accepting work, waits for accepted commands, and releases exact claims. |
| Failed owner | Its claims remain until their Redis TTL expires. |
| Missing or expired claim | The next replica receiving actionable work creates a new process claim. |
| Pull request closes | The owner performs local and external cleanup, then releases the claim. |

Ownership reassignment is lazy. There is no background scheduler that transfers claims to another replica. If graceful HTTP shutdown times out, claims are not released early and instead expire by TTL.

## Command Flow

1. Any ready replica receives and authenticates the VCS webhook.
2. Atlantis parses an actionable comment, autoplan, or pull-close event into a credential-free envelope.
3. The receiving replica checks ownership readiness and atomically resolves the pull request owner.
4. If the exact local process owns the claim, it executes locally. Otherwise it forwards to the owner's authenticated internal endpoint.
5. Before the first local command under a new claim, the owner removes its local pull request directory.
6. Atlantis then follows the configured plan-storage mode.

The local reset always occurs before clone or plan restoration. It prevents a reused persistent volume from supplying a plan created under an older process claim.

## Plan Storage Modes

### Local Plans

Configuration:

```text
--locking-db-type=redis
--replica-advertise-url=http://atlantis-0.atlantis-headless:4141
ATLANTIS_INTERNAL_COMMAND_TOKEN=<shared-secret>
--enable-external-stores=false
```

- Planning writes `.tfplan` files only beneath the owner's data directory.
- Commands continue to use owner-local plans while the ownership claim remains live.
- After takeover, the new owner starts with an empty local pull directory.
- Apply reports the normal missing working directory or missing plan error.
- The user must run plan again before apply.

### External Plans

Configuration:

```text
--locking-db-type=redis
--replica-advertise-url=http://atlantis-0.atlantis-headless:4141
ATLANTIS_INTERNAL_COMMAND_TOKEN=<shared-secret>
--enable-external-stores=true
```

- Planning writes locally and saves the plan through the existing external PlanStore.
- Owner takeover clears only local state; remote plans remain available.
- A targeted apply reclones the required workspace and loads its plan through the PlanStore.
- Apply-all lists stored workspaces, clones them before restoration, and restores their plans for discovery.
- The apply runner loads each plan and validates its stored head commit before Terraform executes.
- A missing plan, absent commit metadata, or commit mismatch fails the command and requires re-plan.
- External-store unavailability fails the command; Atlantis does not apply an unvalidated local fallback.
- Successful apply and pull-close cleanup retain the existing best-effort removal behavior for external objects.

External storage does not remove the need for ownership. Ownership still provides pull request command ordering, stable local runtime state, forwarding, log locality, and cancellation affinity.

## Configuration Contract

The public `--enable-replica-routing` flag and `ATLANTIS_ENABLE_REPLICA_ROUTING` environment variable are removed. Replica routing is activated by configuration presence instead of a separate Boolean.

Routing intent is present when any of these values is configured:

- `--replica-id`, which is an optional identity override.
- `--replica-advertise-url`.
- `--internal-command-token`.

`--ownership-ttl-seconds` does not activate routing because it has a default value.

When no routing-intent value is configured, replica routing remains disabled. This preserves existing Redis-only and external-store-only deployments. When any routing-intent value is configured, startup requires:

- Redis locking through `--locking-db-type=redis`.
- A valid `--replica-advertise-url`.
- A non-empty `--internal-command-token`.
- An ownership TTL of at least 10 seconds.
- A non-empty resolved replica ID.

The replica ID resolves in this order:

1. Use the explicit `--replica-id` value when configured.
2. Otherwise use `os.Hostname()` from the Atlantis process.

Failure to read a non-empty hostname is a startup error when routing is configured. In Kubernetes, this hostname must be the pod hostname, normally the stable StatefulSet pod name such as `atlantis-0`. The Kubernetes worker-node hostname must not be used because multiple Atlantis replicas can share a node. A process-specific instance and claim ID continue to fence restarts that reuse the same pod hostname.

`--enable-external-stores` is independently optional and becomes compatible with inferred replica routing. The startup validation rejecting their combination is removed.

No other validation changes:

- External storage still requires a valid server-side `external_stores.plan_store` configuration.
- External storage without replica routing retains its existing behavior.
- Redis locking without replica routing retains its existing behavior.

The server may retain an internal derived Boolean to control ownership wiring and internal routes, but it is not exposed as user configuration.

## Failure Semantics

- Redis unavailable: the ingress request returns HTTP 503 before local execution.
- Owner unreachable while its lease is live: forwarding fails closed; ownership is not stolen early.
- Owner gone: the next actionable request can claim the pull request after lease expiry.
- External plan valid: the new owner restores it and may continue apply.
- External plan stale or missing: apply fails and requires re-plan.
- Local-only plan: takeover always requires re-plan.
- Forwarding timeout after acceptance: duplicate delivery remains possible; exactly-once execution is not claimed.
- No command arrives after owner loss: no reassignment occurs, because takeover is lazy.

Redis leases fence new command admission. They do not stop a Terraform process already running when its owner loses Redis connectivity. Redis asynchronous failover and network partitions therefore do not provide strict execution fencing.

## Security

- Forwarded envelopes contain no VCS clone credentials.
- The new owner rehydrates credentials from its local configuration.
- Internal endpoints continue to require the shared command token and exact ownership claim.
- PlanStore credentials remain local to each replica and are not stored in Redis or forwarded commands.
- Internal command paths must remain private at the network and gateway layers.

## Testing

The implementation must add or update tests for:

1. Redis configuration alone does not activate replica routing.
2. Complete routing settings activate routing without an enable flag.
3. Partial routing settings fail startup with a precise validation error.
4. Replica ID defaults to the process hostname and supports an explicit override.
5. Startup accepts replica routing with external storage configured.
6. Scale-up leaves an existing pull request with its current owner.
7. Graceful release permits lazy takeover by another replica.
8. Lease expiry permits lazy takeover after owner failure.
9. Local-plan takeover deletes stale local state and requires re-plan.
10. External-plan takeover deletes stale local state, restores a matching plan, and permits apply.
11. External-plan takeover rejects a plan from a different head commit.
12. Missing or unavailable external plans fail without executing Terraform.
13. Pull-close cleanup attempts external plan deletion before releasing ownership.
14. Existing single-replica, Redis-only, and external-store-only modes remain unchanged.

## Cleanup Scope

- Remove the `enable-replica-routing` CLI flag, environment binding, defaults, generated configuration entries, and documentation section.
- Remove the explicit `ATLANTIS_REPLICA_ID` entry from the Kubernetes example; keep `POD_NAME` only where it is needed to construct the advertise URL.
- Replace user-config Boolean checks with one derived routing-configuration method.
- Remove validation and documentation that describe replica routing and external storage as incompatible.
- Remove obsolete tests that exist only for the deleted Boolean while retaining behavioral routing tests.
- Leave the optional `replica-id` override documented for non-Kubernetes or customized-hostname deployments.

## Documentation Changes

- Describe routing as activated by advertise URL and internal token configuration rather than an enable flag.
- Document pod-hostname identity and the optional replica ID override.
- Describe external plan recovery as an optional routing mode rather than an incompatible architecture.
- Update the deployment guide and FAQ mode matrix.
- Retain the explicit warning that local-plan takeover requires re-plan.
- Retain Redis partition and in-flight execution limitations.
