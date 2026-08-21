"""Synchronous API service and service-owned migration entry point."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

TEST_FLEET_ENV = "JUNTAI_SYNTHETIC_DATA_TEST_FLEET"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="juntai-synthetic-data")
    commands = parser.add_subparsers(dest="mode", required=True)
    commands.add_parser("serve", help="run the API service")
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
        _run_server()


def _run_server() -> None:
    if os.getenv(TEST_FLEET_ENV) != "true":
        raise RuntimeError(f"{TEST_FLEET_ENV} must be exactly lowercase true")

    from juntai_synthetic_data.api import build_server
    from juntai_synthetic_data.api.openapi import IAM_AUDIENCE
    from juntai_synthetic_data.migration import read_dsn_file
    from juntai_synthetic_data.runtime import build_runtime_service, psycopg_connector
    from juntai_synthetic_data.runtime_auth import build_runtime_authorizer

    dsn = read_dsn_file()
    configured_audience = os.getenv("JUNTAI_IAM_AUDIENCE", IAM_AUDIENCE)
    if configured_audience != IAM_AUDIENCE:
        raise RuntimeError(f"JUNTAI_IAM_AUDIENCE must be exactly {IAM_AUDIENCE}")
    authorizer = build_runtime_authorizer(
        issuer=os.environ["JUNTAI_IAM_ISSUER"],
        audiences=(IAM_AUDIENCE,),
        policy_snapshot_path=os.environ["JUNTAI_IAM_POLICY_SNAPSHOT"],
        discovery_url=os.getenv("JUNTAI_IAM_DISCOVERY_URL"),
    )
    service = build_runtime_service(
        connector=psycopg_connector(dsn),
        test_fleet=True,
        service_image_digest=os.getenv("JUNTAI_SERVICE_IMAGE_DIGEST"),
        otlp_endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
    )
    server = build_server(service, authorizer=authorizer)
    asyncio.run(server.serve(host=os.getenv("HOST", "0.0.0.0")))


if __name__ == "__main__":
    raise SystemExit(main())
