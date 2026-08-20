from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[2]
PUBLICATION_GUARD = "scripts/require-published-platform-adapters.sh"
PRIVATE_SOURCE_MARKERS = (
    "JUNTAI_" + "UPSTREAM_AUDIT_TOKEN",
    "JUNTAI_" + "PLATFORM_REPOSITORY",
    "JuntaiPlatform" + "Infrastructure",
    "prepare-external-" + "dependencies.sh",
    "prepare_platform_" + "adapters.py",
)


def _boundary_files() -> list[Path]:
    return [
        ROOT / "README.md",
        *sorted((ROOT / ".github" / "workflows").glob("*.yml")),
        *sorted((ROOT / "scripts").glob("*")),
    ]


def test_synthetic_never_reads_or_rebuilds_private_platform_source() -> None:
    for path in _boundary_files():
        if not path.is_file():
            continue
        text = path.read_text()
        for marker in PRIVATE_SOURCE_MARKERS:
            assert marker not in text, f"{path.relative_to(ROOT)} contains {marker}"


def test_artifact_consumers_fail_closed_before_install_or_publication() -> None:
    for relative in (
        ".github/workflows/ci.yml",
        ".github/workflows/publish.yml",
        ".github/workflows/release-service.yml",
        ".github/workflows/release-documentation.yml",
    ):
        workflow = (ROOT / relative).read_text()
        assert PUBLICATION_GUARD in workflow

    publication = (ROOT / ".github" / "workflows" / "publish.yml").read_text()
    assert publication.index(PUBLICATION_GUARD) < publication.index("docker/build-push-action")


def test_unconfigured_publication_guard_is_explicit_and_fail_closed() -> None:
    result = subprocess.run(
        ("sh", str(ROOT / PUBLICATION_GUARD)),
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 78
    assert "Official Platform adapter publication is not configured" in result.stderr
    assert "manifest, checksums, signature, and provenance" in result.stderr
    assert "Private Platform source access, reconstruction, and vendoring are forbidden" in (
        result.stderr
    )


def test_image_keeps_the_external_artifact_build_context_boundary() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert "FROM adapter-artifacts AS adapter-artifacts" in dockerfile
    assert "COPY --from=adapter-artifacts / /tmp/platform-adapters/" in dockerfile
