from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

EXPECTED_CHECKS = {
    "concurrency-lock",
    "cross-schema-atomic-write",
    "database-restart",
    "delete-idempotence",
    "destination-conflict-rollback",
    "empty-database",
    "exact-key-delete",
    "idempotent-replay",
    "ledger-current",
    "lost-response-recovery",
    "no-platform-database-dependency",
    "released-1.2.0-baseline-upgrade",
    "repeat-idempotence",
    "tenant-rls-isolation",
    "transactional-failure-recovery",
    "transactional-partial-failure",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary", required=True)
    parser.add_argument("--post-restart", required=True)
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
    if primary["phase"] != "primary" or restarted["phase"] != "post-restart":
        raise SystemExit("real-KES evidence phases are invalid")
    for field in ("sourceRevision", "serviceImageDigest", "migrationIds", "databaseVersion"):
        if primary[field] != restarted[field]:
            raise SystemExit(f"real-KES evidence differs across restart: {field}")
    checks = sorted(set(primary["checks"]) | set(restarted["checks"]))
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
