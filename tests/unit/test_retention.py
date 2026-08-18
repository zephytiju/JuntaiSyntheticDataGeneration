from __future__ import annotations

from conftest import request_data

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.dataset import BoundedDatasetSink


def test_ephemeral_workspace_is_removed_after_context_exit() -> None:
    request = CreateJobRequest.model_validate(request_data())
    with BoundedDatasetSink(request.generation_contract) as sink:
        root = sink.root
        assert root.exists()
    assert not root.exists()
