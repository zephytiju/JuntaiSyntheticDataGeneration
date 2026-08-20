"""Service, worker, and service-owned migration entry point."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import threading


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="juntai-synthetic-data")
    commands = parser.add_subparsers(dest="mode", required=True)
    commands.add_parser("serve", help="run the API service")
    commands.add_parser("worker", help="run the background worker")
    relay = commands.add_parser("relay", help="run the service-owned SWP queue relay")
    relay.add_argument("--once", action="store_true", help="process one bounded relay batch")
    relay.add_argument("--poll-seconds", type=float, default=1.0)
    migrate = commands.add_parser("migrate", help="apply the ordered KingbaseES migration set")
    migrate.add_argument("--dsn-file", help="absolute path to the KES DSN secret file")
    migrate.add_argument(
        "--check", action="store_true", help="verify compatibility without changing the database"
    )
    migrate.add_argument(
        "--print-manifest", action="store_true", help="print the packaged migration-set manifest"
    )
    return parser


def _run_migration(args: argparse.Namespace) -> int:
    from juntai_synthetic_data.migration import (
        MigrationConfigurationError,
        MigrationDatabaseError,
        MigrationSafetyError,
        apply_migrations,
        binding_from_environment,
        manifest,
        read_dsn_file,
    )

    if args.print_manifest:
        print(json.dumps(manifest(), indent=2, sort_keys=True))
        return 0
    try:
        binding = binding_from_environment()
        dsn = read_dsn_file(args.dsn_file)
        result = apply_migrations(dsn, binding, check=args.check)
    except MigrationConfigurationError as error:
        print(f"migration configuration error: {error}", file=sys.stderr)
        return 2
    except MigrationSafetyError as error:
        print(f"migration safety error: {error}", file=sys.stderr)
        return 3
    except MigrationDatabaseError as error:
        print(f"migration database error: {error}", file=sys.stderr)
        return 4
    print(json.dumps(result.as_dict(), sort_keys=True))
    if args.check and result.pending:
        return 5
    return 0


def main() -> int | None:
    args = _parser().parse_args()
    if args.mode == "migrate":
        return _run_migration(args)
    if args.mode == "serve":
        from juntai_synthetic_data.api import build_server
        from juntai_synthetic_data.api.openapi import IAM_AUDIENCE
        from juntai_synthetic_data.runtime import build_runtime_service
        from juntai_synthetic_data.runtime_auth import build_runtime_authorizer

        service = build_runtime_service()
        configured_audience = os.getenv("JUNTAI_IAM_AUDIENCE", IAM_AUDIENCE)
        if configured_audience != IAM_AUDIENCE:
            raise RuntimeError(f"JUNTAI_IAM_AUDIENCE must be exactly {IAM_AUDIENCE}")
        authorizer = build_runtime_authorizer(
            issuer=os.environ["JUNTAI_IAM_ISSUER"],
            audiences=(IAM_AUDIENCE,),
            policy_snapshot_path=os.environ["JUNTAI_IAM_POLICY_SNAPSHOT"],
            discovery_url=os.getenv("JUNTAI_IAM_DISCOVERY_URL"),
        )
        server = build_server(service, authorizer=authorizer)
        asyncio.run(server.serve(host=os.getenv("HOST", "0.0.0.0")))
    elif args.mode == "relay":
        from juntai_synthetic_data.relay_runtime import build_runtime_relay

        relay = build_runtime_relay()
        if args.once:
            print(json.dumps(relay.run_once().__dict__, sort_keys=True))
            return 0
        stop = threading.Event()

        def request_stop(_signum: int, _frame: object) -> None:
            stop.set()

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        relay.run_forever(stop, poll_seconds=args.poll_seconds)
        return 0
    else:
        from juntai_synthetic_data.worker_runtime import build_worker
        from juntai_synthetic_data.worker_stream_runtime import run_production_worker

        run_production_worker(build_worker)


if __name__ == "__main__":
    raise SystemExit(main())
