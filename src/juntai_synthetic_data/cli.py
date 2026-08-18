"""Service and worker entry point."""

from __future__ import annotations

import argparse
import asyncio
import os


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="juntai-synthetic-data")
    parser.add_argument("mode", choices=("serve", "worker"))
    return parser


def main() -> None:
    args = _parser().parse_args()
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
    main()
