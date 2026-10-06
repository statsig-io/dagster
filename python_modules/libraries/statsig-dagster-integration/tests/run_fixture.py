"""New bounded offline fixture; never resumes or modifies the expired October 2 runners."""

import argparse
import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

TARGET_IMAGE = "sha256:40d7c2eaf411c47c29b78713acc59a3883431d505a99ecd23c28c1886ff2b1ea"
POSTGRES_IMAGE = "sha256:649df4d4c0779f61e1bf3913add94e3026f6758a7349dafbf26dfef54b00415c"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image", default=TARGET_IMAGE)
    parser.add_argument("--system-python", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    unique = f"dagster-daemon-20261006-{uuid.uuid4().hex[:10]}"
    deadline = time.monotonic() + 300
    commands: list[dict[str, object]] = []
    env = dict(os.environ)
    source = Path(__file__).resolve().parents[1]
    ledger = {
        str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(source.rglob("*.py"))
        if "__pycache__" not in path.parts
    }
    (args.output / "source-hashes.json").write_text(
        json.dumps(ledger, indent=2) + "\n", encoding="utf-8"
    )

    def execute(arguments: list[str], *, cleanup: bool = False) -> subprocess.CompletedProcess[str]:
        command = ["docker", *arguments]
        started = time.monotonic()
        remaining = max(1.0, deadline - time.monotonic())
        if not cleanup and remaining <= 1:
            raise TimeoutError("STADI-677 fixture exceeded its 300-second deadline")
        result = subprocess.run(
            command,
            env=env,
            text=True,
            capture_output=True,
            check=False,
            timeout=min(240.0, remaining) if not cleanup else 30,
        )
        with (args.output / "output.log").open("a", encoding="utf-8") as output:
            output.write(result.stdout + result.stderr)
        commands.append(
            {
                "command": command,
                "returncode": result.returncode,
                "elapsed_seconds": time.monotonic() - started,
                "cleanup": cleanup,
            }
        )
        if result.returncode and not cleanup:
            raise RuntimeError(
                f"Fixture command failed ({result.returncode}): {arguments[:3]}\n{result.stdout}{result.stderr}"
            )
        return result

    passed = False
    cleanup_errors: list[str] = []
    try:
        execute(["volume", "create", unique])
        execute(
            [
                "run",
                "--detach",
                "--name",
                unique,
                "--network",
                "none",
                "--read-only",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "256",
                "--memory",
                "512m",
                "--tmpfs",
                "/tmp:rw,nosuid,size=128m",
                "--tmpfs",
                "/var/run/postgresql:rw,nosuid,size=16m",
                "--mount",
                f"type=volume,source={unique},target=/pgfixture",
                "--env",
                "PGDATA=/pgfixture/data",
                "--env",
                "POSTGRES_USER=daemon_fixture",
                "--env",
                "POSTGRES_PASSWORD=synthetic-fixture-password",
                "--env",
                "POSTGRES_DB=daemon_fixture",
                "--env",
                "PGHOST=/pgfixture/socket",
                "--env",
                "POSTGRES_INITDB_ARGS=--auth-local=md5 --auth-host=reject",
                "--entrypoint",
                "sh",
                POSTGRES_IMAGE,
                "-c",
                "mkdir -p /pgfixture/socket && chown postgres:postgres /pgfixture/socket && exec docker-entrypoint.sh postgres -c listen_addresses='' -c unix_socket_directories=/var/run/postgresql,/pgfixture/socket",
            ]
        )
        for _ in range(40):
            startup = execute(["logs", unique], cleanup=True)
            ready = execute(
                [
                    "exec",
                    unique,
                    "pg_isready",
                    "-h",
                    "/pgfixture/socket",
                    "-U",
                    "daemon_fixture",
                ],
                cleanup=True,
            )
            if (
                ready.returncode == 0
                and "PostgreSQL init process complete" in startup.stdout + startup.stderr
            ):
                break
            time.sleep(0.25)
        else:
            raise TimeoutError("Postgres did not become ready within 10 seconds")
        execute(
            [
                "run",
                "--rm",
                "--name",
                f"{unique}-tests",
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "256",
                "--memory",
                "2g",
                "--cpus",
                "2",
                "--tmpfs",
                "/tmp:rw,nosuid,size=512m",
                "--env",
                "HOME=/tmp",
                "--env",
                "PYTHONDONTWRITEBYTECODE=1",
                "--env",
                "PYTHONPATH=/daemon",
                "--env",
                "DAEMON_FIXTURE_POSTGRES_URL=postgresql://daemon_fixture:synthetic-fixture-password@/daemon_fixture?host=/pgfixture/socket",
                "--mount",
                f"type=volume,source={unique},target=/pgfixture",
                "--mount",
                f"type=bind,source={source},target=/daemon,readonly",
                "--entrypoint",
                "uv",
                args.image,
                "run",
                "--no-project" if args.system_python else "--no-sync",
                "python",
                "-m",
                "pytest",
                "-q",
                "-p",
                "no:cacheprovider",
                "/daemon/tests/test_runtime.py",
                "/daemon/tests/test_telemetry.py",
                "/daemon/tests/test_postgres.py",
                "/daemon/tests/test_cli.py",
                "/daemon/tests/test_scheduler.py",
                "/daemon/tests/test_webserver.py",
                "/daemon/tests/test_recovery.py",
            ]
        )
        passed = True
    finally:
        for arguments in (
            ["logs", unique],
            ["rm", "--force", f"{unique}-tests"],
            ["rm", "--force", unique],
            ["volume", "rm", unique],
        ):
            try:
                result = execute(arguments, cleanup=True)
                absent = arguments[0] in {"logs", "rm"} and (
                    f"No such container: {arguments[-1]}" in result.stdout + result.stderr
                )
                if result.returncode and not absent:
                    cleanup_errors.append(f"{arguments}: exit {result.returncode}")
                commands[-1]["cleanup_outcome"] = (
                    "already_absent"
                    if absent
                    else "completed"
                    if not result.returncode
                    else "failed"
                )
            except (OSError, subprocess.TimeoutExpired) as error:
                cleanup_errors.append(f"{arguments}: {type(error).__name__}: {error}")
        (args.output / "commands.json").write_text(
            json.dumps(
                {
                    "owner": "STADI-677",
                    "max_seconds": 300,
                    "target_image": args.image,
                    "postgres_image": POSTGRES_IMAGE,
                    "commands": commands,
                    "status": "passed" if passed and not cleanup_errors else "failed",
                    "cleanup_errors": cleanup_errors,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    if cleanup_errors:
        raise RuntimeError(f"Fixture cleanup failed: {cleanup_errors}")
    print(f"PASS bounded STADI-677 fixture; evidence: {args.output}")  # noqa: T201 -- test-driver receipt.


if __name__ == "__main__":
    main()
