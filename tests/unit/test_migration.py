from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

from juntai_synthetic_data import cli
from juntai_synthetic_data.migration import (
    IMAGE_DIGEST_ENV,
    SOURCE_REVISION_ENV,
    MigrationConfigurationError,
    binding_from_environment,
    load_migrations,
    manifest,
    read_dsn_file,
)


def test_manifest_is_ordered_and_binds_exact_source_checksum() -> None:
    document = manifest()
    migrations = load_migrations()

    assert document["schemaVersion"] == "juntai.synthetic-data.migration-set/v1"
    assert document["service"]["version"] == "1.3.0"
    assert [migration.migration_id for migration in migrations] == [
        "0001_jobs",
        "0002_worker_protocol",
        "0003_synchronous_generations",
    ]
    assert migrations[0].checksum == hashlib.sha256(migrations[0].sql.encode()).hexdigest()
    assert migrations[0].checksum == (
        "af29058d1ca61516415cc3b3f877987012c371fba5fdec0170bc83dc76c19822"
    )


def test_binding_requires_exact_immutable_identifiers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(SOURCE_REVISION_ENV, "a" * 40)
    monkeypatch.setenv(IMAGE_DIGEST_ENV, "sha256:" + "b" * 64)
    binding = binding_from_environment()

    assert binding.source_revision == "a" * 40
    assert binding.image_digest == "sha256:" + "b" * 64
    assert binding.service_version == "1.3.0"


@pytest.mark.parametrize(
    ("source", "image"),
    [
        ("A" * 40, "sha256:" + "b" * 64),
        ("a" * 39, "sha256:" + "b" * 64),
        ("a" * 40, "latest"),
    ],
)
def test_binding_rejects_mutable_or_malformed_identifiers(
    monkeypatch: pytest.MonkeyPatch, source: str, image: str
) -> None:
    monkeypatch.setenv(SOURCE_REVISION_ENV, source)
    monkeypatch.setenv(IMAGE_DIGEST_ENV, image)

    with pytest.raises(MigrationConfigurationError):
        binding_from_environment()


def test_dsn_is_read_only_from_an_owner_only_absolute_file(tmp_path: Path) -> None:
    path = tmp_path / "kes.dsn"
    path.write_text("host=kes dbname=jobs user=migrator password=secret\n")
    path.chmod(0o600)

    assert read_dsn_file(str(path)) == "host=kes dbname=jobs user=migrator password=secret"


def test_dsn_rejects_group_or_world_access(tmp_path: Path) -> None:
    path = tmp_path / "kes.dsn"
    path.write_text("password=secret")
    path.chmod(0o640)

    with pytest.raises(MigrationConfigurationError, match="group/world"):
        read_dsn_file(str(path))


def test_print_manifest_does_not_build_runtime(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["juntai-synthetic-data", "migrate", "--print-manifest"])

    assert cli.main() == 0
    assert "0001_jobs" in capsys.readouterr().out


def test_migrate_configuration_failure_has_stable_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv(SOURCE_REVISION_ENV, raising=False)
    monkeypatch.delenv(IMAGE_DIGEST_ENV, raising=False)
    monkeypatch.setattr(sys, "argv", ["juntai-synthetic-data", "migrate"])

    assert cli.main() == 2
    assert SOURCE_REVISION_ENV in capsys.readouterr().err
