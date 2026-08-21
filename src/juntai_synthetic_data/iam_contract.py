"""Immutable identity of the published IAM runtime and wire contracts."""

from __future__ import annotations

import hashlib
import json
from importlib import metadata
from pathlib import Path

IAM_DISTRIBUTION = "juntai-iam"
IAM_VERSION = "1.1.0"
IAM_SOURCE_COMMIT = "72b481ed825c00d0bd96feca67790e90dc5ace9b"
IAM_WHEEL_SHA256 = "007362537726dbd69c75952b73c62b90e4f7ea92a48ab214ba0ad3ffcb533e6c"

IAM_CONTRACTS_DISTRIBUTION = "juntai-iam-contracts"
IAM_CONTRACTS_VERSION = "1.1.1"
IAM_CONTRACTS_SOURCE_COMMIT = "a37b6d6daaba75efd8c15c19b440a3081ba761c5"
IAM_CONTRACTS_WHEEL_SHA256 = "e1daa81386669cfbf74b119c73f822d80a2f5e7a64a187538c54dcff07643cf1"
IAM_CONTRACT_MANIFEST_SHA256 = "64dafb25c54d40320347c8661960d23ba524a2d3c102d112c08c95679d12db85"
IAM_CONTRACT_MANIFEST_RELATIVE_PATH = Path("share/juntai-iam-contracts/manifest.v1.json")


def validate_iam_runtime() -> Path:
    """Fail closed unless the installed IAM packages and manifest are the reviewed tuple."""

    try:
        iam = metadata.distribution(IAM_DISTRIBUTION)
        contracts = metadata.distribution(IAM_CONTRACTS_DISTRIBUTION)
    except metadata.PackageNotFoundError as error:
        raise RuntimeError("required published Juntai IAM runtime is absent") from error
    if iam.version != IAM_VERSION or contracts.version != IAM_CONTRACTS_VERSION:
        raise RuntimeError("installed Juntai IAM tuple differs from the reviewed release")

    requirement = next(
        (
            value
            for value in (iam.requires or ())
            if value.lower().startswith(f"{IAM_CONTRACTS_DISTRIBUTION}==")
        ),
        None,
    )
    if requirement != f"{IAM_CONTRACTS_DISTRIBUTION}=={IAM_CONTRACTS_VERSION}":
        raise RuntimeError("installed Juntai IAM does not pin the reviewed contract package")

    manifest_entries = [
        entry
        for entry in (contracts.files or ())
        if Path(str(entry)).as_posix().endswith(IAM_CONTRACT_MANIFEST_RELATIVE_PATH.as_posix())
    ]
    if len(manifest_entries) != 1:
        raise RuntimeError("published Juntai IAM contract manifest is absent or ambiguous")
    manifest = contracts.locate_file(manifest_entries[0])
    if not manifest.is_file() or manifest.is_symlink():
        raise RuntimeError("published Juntai IAM contract manifest is absent")
    payload = manifest.read_bytes()
    if hashlib.sha256(payload).hexdigest() != IAM_CONTRACT_MANIFEST_SHA256:
        raise RuntimeError("published Juntai IAM contract manifest digest differs")
    try:
        document = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise RuntimeError("published Juntai IAM contract manifest is invalid") from error
    if (
        document.get("schemaVersion") != "juntai.iam/contract-manifest.v1"
        or document.get("packageVersion") != IAM_CONTRACTS_VERSION
    ):
        raise RuntimeError("published Juntai IAM contract manifest identity differs")
    return manifest
