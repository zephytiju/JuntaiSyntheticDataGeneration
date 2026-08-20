from __future__ import annotations

import hashlib
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from juntai.iam import AuthorityMode, IdentityContext, PrincipalKind

from juntai_synthetic_data.iam_contract import (
    CASDOOR_IMAGE,
    IAM_CONTRACT_MANIFEST_SHA256,
    IAM_CONTRACTS_SOURCE_COMMIT,
    IAM_CONTRACTS_VERSION,
    IAM_CONTRACTS_WHEEL_SHA256,
    IAM_SOURCE_COMMIT,
    IAM_VERSION,
    IAM_WHEEL_SHA256,
    PLATFORM_SOURCE_COMMIT,
    PLATFORM_VERSION,
    validate_iam_runtime,
)

ROOT = Path(__file__).parents[2]


def test_runtime_uses_only_the_exact_published_iam_tuple() -> None:
    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    assert f"juntai-iam=={IAM_VERSION}" in dependencies
    assert f"juntai-iam-contracts=={IAM_CONTRACTS_VERSION}" in dependencies
    manifest = validate_iam_runtime()
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == IAM_CONTRACT_MANIFEST_SHA256
    assert IAM_SOURCE_COMMIT == "72b481ed825c00d0bd96feca67790e90dc5ace9b"
    assert IAM_WHEEL_SHA256 == ("007362537726dbd69c75952b73c62b90e4f7ea92a48ab214ba0ad3ffcb533e6c")
    assert IAM_CONTRACTS_SOURCE_COMMIT == "a37b6d6daaba75efd8c15c19b440a3081ba761c5"
    assert IAM_CONTRACTS_WHEEL_SHA256 == (
        "e1daa81386669cfbf74b119c73f822d80a2f5e7a64a187538c54dcff07643cf1"
    )
    assert PLATFORM_VERSION == "1.1.0"
    assert PLATFORM_SOURCE_COMMIT == "763e126735b572e8cf334115c5fa33968326819c"
    assert CASDOOR_IMAGE.endswith(
        "@sha256:d7658640aba370495e59dc1464756d2ae7ec66576203b9de0040e9cc37793607"
    )


@pytest.mark.parametrize(
    ("kind", "mode", "fields", "message"),
    [
        (
            PrincipalKind.HUMAN,
            AuthorityMode.DELEGATED,
            {
                "delegation_grant_ref": "grant-1",
                "delegation_grant_revision": 1,
                "grantor_principal_id": "urn:juntai:human:tenant-a:user-1",
                "grantor_authority_epoch": 1,
            },
            "only AgentPrincipal",
        ),
        (
            PrincipalKind.AGENT,
            AuthorityMode.DELEGATED,
            {},
            "requires exact grant and grantor epochs",
        ),
        (
            PrincipalKind.SERVICE,
            AuthorityMode.DELEGATED,
            {
                "delegation_grant_ref": "grant-1",
                "delegation_grant_revision": 1,
                "grantor_principal_id": "urn:juntai:human:tenant-a:user-1",
                "grantor_authority_epoch": 1,
            },
            "only AgentPrincipal",
        ),
    ],
)
def test_published_peer_principal_contract_rejects_invalid_delegation_context(
    kind: PrincipalKind,
    mode: AuthorityMode,
    fields: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        IdentityContext(
            principal_kind=kind,
            subject_id=f"urn:juntai:{kind.value}:tenant-a:caller-1",
            issuer="https://iam.example",
            audience=("juntai.synthetic-data.api",),
            tenant_id="tenant-a",
            authority_mode=mode,
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
            **fields,
        )


def test_synthetic_does_not_copy_iam_schemas_or_evaluator_semantics() -> None:
    source = ROOT / "src" / "juntai_synthetic_data"
    assert not list(source.rglob("*.schema.json"))
    text = "\n".join(path.read_text() for path in source.rglob("*.py"))
    assert "class PeerPrincipalEvaluator" not in text
    assert "class CasdoorAccessTokenVerifier" not in text
