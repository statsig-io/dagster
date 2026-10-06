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
