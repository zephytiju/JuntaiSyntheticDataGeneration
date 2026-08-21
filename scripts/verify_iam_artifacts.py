"""Verify the exact published IAM wheels before installation or image composition."""

from __future__ import annotations

import argparse
import hashlib
import runpy
import zipfile
from pathlib import Path

ROOT = Path(__file__).parents[1]
_CONTRACT = runpy.run_path(str(ROOT / "src/juntai_synthetic_data/iam_contract.py"))
IAM_VERSION = _CONTRACT["IAM_VERSION"]
IAM_WHEEL_SHA256 = _CONTRACT["IAM_WHEEL_SHA256"]
IAM_CONTRACTS_VERSION = _CONTRACT["IAM_CONTRACTS_VERSION"]
IAM_CONTRACTS_WHEEL_SHA256 = _CONTRACT["IAM_CONTRACTS_WHEEL_SHA256"]
IAM_CONTRACT_MANIFEST_SHA256 = _CONTRACT["IAM_CONTRACT_MANIFEST_SHA256"]

WHEELS = {
    f"juntai_iam-{IAM_VERSION}-py3-none-any.whl": IAM_WHEEL_SHA256,
    f"juntai_iam_contracts-{IAM_CONTRACTS_VERSION}-py3-none-any.whl": (IAM_CONTRACTS_WHEEL_SHA256),
}


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    actual = {path.name: _sha256(path.read_bytes()) for path in args.directory.glob("*.whl")}
    if actual != WHEELS:
        raise SystemExit(f"published IAM wheel set differs: {actual}")

    contracts_wheel = args.directory / (
        f"juntai_iam_contracts-{IAM_CONTRACTS_VERSION}-py3-none-any.whl"
    )
    with zipfile.ZipFile(contracts_wheel) as archive:
        manifests = [name for name in archive.namelist() if name.endswith("/manifest.v1.json")]
        if len(manifests) != 1:
            raise SystemExit("IAM contracts wheel does not contain exactly one manifest")
        if _sha256(archive.read(manifests[0])) != IAM_CONTRACT_MANIFEST_SHA256:
            raise SystemExit("IAM contracts wheel manifest digest differs")

    iam_wheel = args.directory / f"juntai_iam-{IAM_VERSION}-py3-none-any.whl"
    with zipfile.ZipFile(iam_wheel) as archive:
        metadata_files = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_files) != 1:
            raise SystemExit("IAM wheel does not contain exactly one METADATA file")
        metadata = archive.read(metadata_files[0]).decode("utf-8")
    if f"Requires-Dist: juntai-iam-contracts=={IAM_CONTRACTS_VERSION}\n" not in metadata:
        raise SystemExit("IAM wheel does not pin the reviewed contract package")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
