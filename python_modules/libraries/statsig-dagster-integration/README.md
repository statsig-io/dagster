# Statsig Dagster integration

Private, explicit integration for official Dagster 1.13.25, webserver 1.13.25 and
Postgres 0.29.25. Distribution version 1.13.25.1 identifies this integration
revision; it does not change SDK distribution versions. Importing the package
never activates adapters. No console scripts shadow upstream entrypoints.

`statsig_dagster_integration.cli.main(arguments, pool_settings=...)` owns the
version-guarded daemon/webserver lifecycle. Kelp supplies explicit PoolSettings
and owns image aliases, coordinator, metrics and company policy. The underlying
Click commands, run-only telemetry, configured RPC timeouts and webserver storage
options retain their tested contracts. Environment opt-outs isolate fixtures.

Framework modules/tests originate from the committed Kelp checkpoint recorded in
source-origin.json. Existing immutable configuration dataclasses are preserved as
part of that tested public constructor/validation contract; this port does not
introduce a record conversion or alter caller behavior.

Build a pure Python wheel from a committed Git archive using the hash-locked
poetry-core 2.2.1 in build-requirements.txt, with no runtime dependency resolution
or build isolation. Record the exact source commit/tree, module hashes, backend
requirements/image identity and wheel SHA256. Repeat the build from the same
archive and compare wheel bytes. Kelp consumes that exact local wheel through its
root lock and checks its hash in both images; no Git installs or package publishing
are necessary. The SDK wheel graph remains official and unchanged.

Run tests/run_fixture.py with a retained immutable target test image and fresh
output directory. It uses bounded network-none containers, a unique disposable
Postgres volume/socket, synthetic credentials and explicit cleanup. It does not
contact configuration services, submit cloud jobs or start live telemetry.

## Recovery and validation scope

Keep two workspace attempts and three ListRepositories attempts per workspace
attempt, with at most 22s of backoff; only UNAVAILABLE is retried, not
DEADLINE_EXCEEDED or CANCELLED. ListRepositories uses the general
`DAGSTER_GRPC_TIMEOUT_SECONDS`, captured at client-module import (default 60s).
Its request/backoff allowance is `6*T + 22`: 382s by default, or 21622s with the
tracked Kelp production daemon/webserver timeout of 3600s. This is not an overall
load or recovery deadline. GetServerId, optional GetCurrentImage, serial repository
snapshots and optional deferred-job batches add RPC work, followed by parsing and
cleanup. Repository snapshots use `DAGSTER_REPOSITORY_GRPC_TIMEOUT_SECONDS`,
defaulting to `max(180,T)`. These additional calls are outside the ListRepositories
allowance; no retry increase or broad RPC retry is added.

Upstream daemon workspace refresh is attempted after 60s; its 300s freshness
tolerance handles raised refresh failures and does not interrupt blocked RPCs.
Location errors can be serialized into a completed refresh. The code-server
watcher polls identity every second with an explicit 2s timeout, continues
reconnecting after its initial error transition, and checks reachable-but-errored
locations every 10s. Synchronous refresh callbacks can delay further checks;
these intervals do not guarantee recovery within 60s. Relevant upstream paths
are `_grpc/client.py`, `_grpc/server_watcher.py`, `_daemon/controller.py` and
`_api/snapshot_repository.py` under `python_modules/dagster/dagster`.

The September 7–October 6, 2026 production summary from StatQL `#logs`
(cluster `prod-gke-us-west1`), supplied for review, reports 585 warnings
(136 daemon, 449 webserver), 23 daemon episodes and 176 webserver episodes across
94 webserver pods, maximum durations of 457s/551s, and classifications of
576 Unreachable, eight
UNAVAILABLE and one CANCELLED. Its prediction leaves 20/23 daemon and 118/176
webserver episodes errored under the unchanged bounded policy; that residual is
accepted. These are supplied historical counts and a prediction, not newly measured
target recovery or a fix for the outage cause.

The review's Kelp inventory has 19 schedules at intervals of five minutes or less
(13 every five minutes, four every minute, two every two minutes), including
`load_user_store.py`, `create_fsms.py` and `retry_failed_to_start_jobs.py`. Target
`_scheduler/scheduler.py` retains only the latest missed tick for nonpartitioned
schedules, as the checked-in 1.4.16 overlay already did. A 7–9 minute outage can
therefore drop intermediate ticks: this is existing behavior, not an upgrade
regression. Recovery does not guarantee lossless tick catch-up.

Focused validation passed eight tests (one warning), including four real
code-server stop/restart cases in `tests/test_recovery.py` for daemon and webserver.
Two use real backoff: both exhaust two workspace attempts/six ListRepositories
calls with timeout 3600s and 22s of backoff, then recover automatically through the
watcher within the fixture's 30s post-restart deadline. Two virtualize only adapter
backoff sleep and exercise upstream `attempt_error_recovery` when the server is
reachable but its workspace entry remains errored. Recovery took 11.573s/11.560s
for daemon/webserver without LOCATION_UPDATED or LOCATION_ERROR recovery events.
No manual reload triggers recovery. Additional serialized-error tests cover both
60s and 3600s timeout allowances. These local synthetic results use fast-failing
RPCs; they do not establish production recovery latency or exercise calls lasting
3600s. Test/documentation revisions are separate from Kelp's existing immutable
wheel source; runtime-module hashes remain unchanged, and the original 52-case
artifact receipt does not include these new recovery tests.
The complete synthetic PostgreSQL/framework/recovery fixture passed 58 tests
(one warning); exit status was 0 and cleanup reported no errors.
