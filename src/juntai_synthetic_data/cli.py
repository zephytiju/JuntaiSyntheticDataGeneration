"""Service, worker, and service-owned migration entry point."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="juntai-synthetic-data")
    commands = parser.add_subparsers(dest="mode", required=True)
    commands.add_parser("serve", help="run the API service")
    commands.add_parser("worker", help="run the background worker")
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
    from juntai_synthetic_data.runtime import build_runtime_service

    service = build_runtime_service()
    if args.mode == "serve":
        from juntai_synthetic_data.api import build_server
        from juntai_synthetic_data.runtime_auth import build_runtime_authorizer

        authorizer = build_runtime_authorizer(
            issuer=os.environ["JUNTAI_IAM_ISSUER"],
            audiences=tuple(
                item.strip()
                for item in os.environ["JUNTAI_IAM_AUDIENCE"].split(",")
                if item.strip()
            ),
            policy_snapshot_path=os.environ["JUNTAI_IAM_POLICY_SNAPSHOT"],
            discovery_url=os.getenv("JUNTAI_IAM_DISCOVERY_URL"),
        )
        server = build_server(service, authorizer=authorizer)
        asyncio.run(server.serve(host=os.getenv("HOST", "0.0.0.0")))
    else:
        from juntai_synthetic_data.scheduling import JobScheduler

        scheduler = JobScheduler(service)

        async def run() -> None:
            await scheduler.validate()
            await scheduler.materialize()
            await scheduler.start()
            try:
                await asyncio.Event().wait()
            finally:
                await scheduler.remove_readiness()
                await scheduler.drain()
                await scheduler.stop()

        asyncio.run(run())


if __name__ == "__main__":
    raise SystemExit(main())
