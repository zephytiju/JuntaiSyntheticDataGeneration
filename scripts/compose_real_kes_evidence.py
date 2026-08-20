from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

EXPECTED_CHECKS = {
    "empty-database",
    "repeat-idempotence",
    "concurrency-lock",
    "transactional-partial-failure",
    "transactional-failure-recovery",
    "released-1.1.0-and-1.2.0-upgrade",
    "tenant-rls-isolation-all-swp-tables",
    "atomic-outbox-result-replay",
    "relay-lease-expiry-recovery",
    "dead-letter-state-idempotency",
    "result-and-dead-letter-conflict-rejection",
    "post-migration-api-startup",
    "post-migration-worker-startup-no-kes",
    "worker-kes-network-denied",
    "database-restart",
    "ledger-current",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary", required=True)
    parser.add_argument("--post-restart", required=True)
    parser.add_argument("--worker-isolation", required=True)
    parser.add_argument("--kes-image", required=True)
    parser.add_argument("--out")
    return parser


def _canonical(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def main() -> int:
    args = _parser().parse_args()
    primary = json.loads(args.primary)
    restarted = json.loads(args.post_restart)
    worker_isolation = json.loads(args.worker_isolation)
    if primary["phase"] != "primary" or restarted["phase"] != "post-restart":
        raise SystemExit("real-KES evidence phases are invalid")
    for field in ("sourceRevision", "serviceImageDigest", "migrationIds", "databaseVersion"):
        if primary[field] != restarted[field]:
            raise SystemExit(f"real-KES evidence differs across restart: {field}")
    checks = sorted(set(primary["checks"]) | set(restarted["checks"]))
    if worker_isolation != {"check": "worker-kes-network-denied", "result": "passed"}:
        raise SystemExit("worker KES network isolation evidence is invalid")
    checks.append(worker_isolation["check"])
    checks.sort()
    if set(checks) != EXPECTED_CHECKS:
        raise SystemExit("real-KES evidence does not contain the complete required matrix")
    if "@sha256:" not in args.kes_image:
        raise SystemExit("KingbaseES acceptance image must be digest-pinned")
    evidence = {
        "schemaVersion": "juntai.synthetic-data.real-kes-acceptance-result/v1",
        "sourceRevision": primary["sourceRevision"],
        "serviceImageDigest": primary["serviceImageDigest"],
        "serviceVersion": "1.3.0",
        "kingbaseImage": args.kes_image,
        "kingbaseVersion": primary["databaseVersion"],
        "migrationIds": primary["migrationIds"],
        "checks": checks,
        "result": "passed",
        "completedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    content = _canonical(evidence) + "\n"
    if args.out:
        Path(args.out).write_text(content, encoding="utf-8", newline="\n")
    else:
        print(content, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
