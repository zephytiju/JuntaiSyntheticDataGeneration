"""Generate deterministic SWP/v1 schema, checksum, and optional exact release manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

from pydantic import TypeAdapter

from juntai_synthetic_data import __version__
from juntai_synthetic_data.worker_protocol import (
    EVIDENCE_MEDIA_TYPE,
    INPUT_MEDIA_TYPE,
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    SOCKET_PATH,
)
from juntai_synthetic_data.worker_protocol.models import Envelope

ROOT = Path(__file__).parents[1]
DEFAULT_OUT = ROOT / "contracts" / "worker-protocol"
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        + "\n"
    ).encode()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--source-commit")
    parser.add_argument("--image-digest")
    args = parser.parse_args()
    if (args.source_commit is None) != (args.image_digest is None):
        raise SystemExit("source commit and image digest must be supplied together")
    if args.source_commit is not None and not _COMMIT.fullmatch(args.source_commit):
        raise SystemExit("source commit must be 40 lowercase hexadecimal characters")
    if args.image_digest is not None and not _DIGEST.fullmatch(args.image_digest):
        raise SystemExit("image digest must be an immutable sha256 digest")

    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    schema = TypeAdapter(Envelope).json_schema(mode="validation")
    schema.update(
        {
            "$id": "https://contracts.juntai.example/synthetic/worker/swp.v1.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Juntai Synthetic Worker Protocol v1",
            "x-juntai-canonicalization": "RFC 8785 JCS; contentDigest omitted while hashing",
            "x-juntai-framing": {
                "socket": SOCKET_PATH,
                "lengthPrefix": "uint32-big-endian",
                "maximumFrameBytes": MAX_FRAME_BYTES,
            },
        }
    )
    schema_path = output / "swp.v1.schema.json"
    schema_path.write_bytes(_canonical(schema))
    digest = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    (output / "swp.v1.sha256").write_text(
        f"{digest}  swp.v1.schema.json\n", encoding="utf-8", newline="\n"
    )
    if args.source_commit is not None:
        manifest = {
            "schemaVersion": "juntai.synthetic.worker-protocol-release/v1",
            "protocol": PROTOCOL_VERSION,
            "serviceVersion": __version__,
            "sourceCommit": args.source_commit,
            "workerImageDigest": args.image_digest,
            "schema": {"path": "swp.v1.schema.json", "sha256": digest},
            "canonicalization": "RFC 8785",
            "contentDigest": "sha256 of canonical envelope with contentDigest omitted",
            "socket": {
                "path": SOCKET_PATH,
                "owner": "root",
                "group": "juntai-worker",
                "mode": "0660",
                "maximumFrameBytes": MAX_FRAME_BYTES,
            },
            "channels": {
                "dispatch": "synthetic.worker.dispatch.v1",
                "control": "synthetic.worker.control.v1",
                "result": "synthetic.worker.result.v1",
                "deadLetter": "synthetic.worker.dead-letter.v1",
            },
            "delivery": {
                "visibilitySeconds": 60,
                "renewEverySeconds": 20,
                "maximumDeliveries": 5,
                "retryBaseSeconds": 5,
                "retryCapSeconds": 300,
                "terminationAllowanceSeconds": 60,
                "maximumLeaseSeconds": 21600,
            },
            "capabilities": [
                "canonical-envelope-digest",
                "cancel-sequence",
                "exact-artifact-references",
                "terminal-evidence",
            ],
            "minimumExecutorBinding": "juntai.platform.synthetic-executor/v1",
            "artifactMediaTypes": [INPUT_MEDIA_TYPE, EVIDENCE_MEDIA_TYPE],
        }
        (output / "generation-manifest.json").write_bytes(_canonical(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
