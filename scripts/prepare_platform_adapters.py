"""Reproduce and verify the immutable Platform adapter wheels without vendoring source."""

from __future__ import annotations

import argparse
import hashlib
import os
import runpy
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).parents[1]
_CONTRACT = runpy.run_path(str(ROOT / "src/juntai_synthetic_data/platform_adapter_contract.py"))
PLATFORM_ADAPTER_CONTRACT_COMMIT = _CONTRACT["PLATFORM_ADAPTER_CONTRACT_COMMIT"]
PLATFORM_ADAPTER_CONTRACT_SHA256 = _CONTRACT["PLATFORM_ADAPTER_CONTRACT_SHA256"]
PLATFORM_ADAPTER_CONTRACT_TREE = _CONTRACT["PLATFORM_ADAPTER_CONTRACT_TREE"]
PLATFORM_ADAPTER_EVIDENCE_COMMIT = _CONTRACT["PLATFORM_ADAPTER_EVIDENCE_COMMIT"]
PLATFORM_ADAPTER_EVIDENCE_TREE = _CONTRACT["PLATFORM_ADAPTER_EVIDENCE_TREE"]
PLATFORM_ADAPTER_SOURCE_COMMIT = _CONTRACT["PLATFORM_ADAPTER_SOURCE_COMMIT"]
PLATFORM_ADAPTER_SOURCE_TREE = _CONTRACT["PLATFORM_ADAPTER_SOURCE_TREE"]
PLATFORM_QUEUE_WHEEL_SHA256 = _CONTRACT["PLATFORM_QUEUE_WHEEL_SHA256"]
PLATFORM_STREAM_WHEEL_SHA256 = _CONTRACT["PLATFORM_STREAM_WHEEL_SHA256"]

MANIFEST_PATH = "contracts/python-adapters/v1/manifest.json"
PACKAGE_PATHS = (
    "packages/queue-adapter-python",
    "packages/swp-stream-python",
)
WHEELS = {
    "juntai_platform_queue_kafka-1.0.0-py3-none-any.whl": PLATFORM_QUEUE_WHEEL_SHA256,
    "juntai_platform_swp_stream-1.0.0-py3-none-any.whl": PLATFORM_STREAM_WHEEL_SHA256,
}


def _run(repository: Path, *arguments: str, text: bool = True) -> str | bytes:
    result = subprocess.run(
        ("git", "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=text,
    )
    return result.stdout.strip() if text else result.stdout


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _verify_git_identity(repository: Path, commit: str, tree: str) -> None:
    if _run(repository, "cat-file", "-t", commit) != "commit":
        raise SystemExit(f"Platform object is not a commit: {commit}")
    if _run(repository, "rev-parse", f"{commit}^{{tree}}") != tree:
        raise SystemExit(f"Platform source tree differs for {commit}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform-repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    repository = args.platform_repository.resolve()
    if not (repository / ".git").exists() and not _run(repository, "rev-parse", "--git-dir"):
        raise SystemExit("Platform repository is not a Git worktree")
    for commit, tree in (
        (PLATFORM_ADAPTER_SOURCE_COMMIT, PLATFORM_ADAPTER_SOURCE_TREE),
        (PLATFORM_ADAPTER_CONTRACT_COMMIT, PLATFORM_ADAPTER_CONTRACT_TREE),
        (PLATFORM_ADAPTER_EVIDENCE_COMMIT, PLATFORM_ADAPTER_EVIDENCE_TREE),
    ):
        _verify_git_identity(repository, commit, tree)
    if (
        _run(
            repository,
            "merge-base",
            "--is-ancestor",
            PLATFORM_ADAPTER_SOURCE_COMMIT,
            PLATFORM_ADAPTER_EVIDENCE_COMMIT,
        )
        != ""
    ):
        raise SystemExit("Platform evidence does not descend from the adapter source")

    manifest = _run(
        repository,
        "show",
        f"{PLATFORM_ADAPTER_CONTRACT_COMMIT}:{MANIFEST_PATH}",
        text=False,
    )
    assert isinstance(manifest, bytes)
    if _sha256(manifest) != PLATFORM_ADAPTER_CONTRACT_SHA256:
        raise SystemExit("Platform contract manifest digest differs")

    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise SystemExit("adapter output directory must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)

    environment = dict(os.environ)
    environment.update(
        {
            "LC_ALL": "C",
            "PYTHONHASHSEED": "0",
            "SOURCE_DATE_EPOCH": "0",
        }
    )
    with tempfile.TemporaryDirectory(prefix="juntai-platform-adapters-") as directory:
        source = Path(directory) / "source"
        source.mkdir()
        archive = _run(
            repository,
            "archive",
            "--format=tar",
            PLATFORM_ADAPTER_SOURCE_COMMIT,
            text=False,
        )
        assert isinstance(archive, bytes)
        archive_path = Path(directory) / "source.tar"
        archive_path.write_bytes(archive)
        with tarfile.open(archive_path) as tar:
            tar.extractall(source, filter="data")
        for package in PACKAGE_PATHS:
            subprocess.run(
                (
                    sys.executable,
                    "-m",
                    "build",
                    "--wheel",
                    "--no-isolation",
                    "--outdir",
                    str(output),
                    str(source / package),
                ),
                check=True,
                env=environment,
            )

    actual = {path.name: _sha256(path.read_bytes()) for path in output.glob("*.whl")}
    if actual != WHEELS:
        raise SystemExit(f"reproduced Platform wheel set differs: {actual}")
    manifest_target = output / "manifest.json"
    manifest_target.write_bytes(manifest)
    shutil.copyfile(repository / MANIFEST_PATH, output / "live-manifest.readback.json")
    if _sha256((output / "live-manifest.readback.json").read_bytes()) != (
        PLATFORM_ADAPTER_CONTRACT_SHA256
    ):
        raise SystemExit("checked-out Platform manifest differs from the canonical contract")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
