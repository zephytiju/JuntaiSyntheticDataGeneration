from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from juntai_synthetic_data.destinations import DestinationAllowlist
from juntai_synthetic_data.persistence import SqlGenerationRepository
from juntai_synthetic_data.runtime import build_runtime_service


def test_runtime_fails_closed_outside_test_fleet() -> None:
    with pytest.raises(RuntimeError, match="only in a test fleet"):
        build_runtime_service(
            connector=Mock(),
            allowlist=DestinationAllowlist(frozenset({("axiom_preview", "site")})),
            test_fleet=False,
        )


def test_runtime_builds_one_in_process_service_from_injected_bindings() -> None:
    connector = Mock()
    allowlist = DestinationAllowlist(frozenset({("axiom_preview", "site")}))

    with patch("juntai_synthetic_data.runtime.configure_observability") as configure:
        service = build_runtime_service(
            connector=connector,
            allowlist=allowlist,
            test_fleet=True,
            service_image_digest="sha256:" + "1" * 64,
        )

    assert isinstance(service.repository, SqlGenerationRepository)
    assert service.repository.connector is connector
    assert service.repository.allowlist is allowlist
    assert service.providers._providers[0].manifest.provider_id == "juntai.synthetic-data.tabular"
    assert configure.call_args.args[0].deployment_environment == "test-fleet"
