"""Synchronous API service and service-owned migration entry point."""

from __future__ import annotations

import argparse
import json
import sys


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
    raise RuntimeError(
        "serve binding is intentionally disabled until the exact destination-allowlist "
        "configuration name and test-fleet marker are approved"
    )


if __name__ == "__main__":
    raise SystemExit(main())
