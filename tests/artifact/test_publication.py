from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest
from conftest import ARTIFACT_DIGEST, IMAGE_DIGEST, request_data
from juntai.artifact.errors import OCITransportError

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.dataset import BoundedDatasetSink
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.providers import DeterministicTabularProvider, GenerationExecutionContext
from juntai_synthetic_data.publication import ArtifactDatasetPublisher


@dataclass
class CapturingArtifactClient:
    calls: list[dict[str, Any]]

    def publish(self, **kwargs: Any):
        self.calls.append(kwargs)
        return SimpleNamespace(
            reference=SimpleNamespace(
                artifact_id="art_exact",
                version_id="artv_exact",
                manifest_digest=ARTIFACT_DIGEST,
                manifest_media_type="application/vnd.oci.image.manifest.v1+json",
            )
        )


def generated_dataset():
    request = CreateJobRequest.model_validate(request_data())
    provider = DeterministicTabularProvider(worker_image_digest=IMAGE_DIGEST)
    sink = BoundedDatasetSink(request.generation_contract)
    output = provider.generate(
        request.generation_contract,
        request.seed,
        sink,
        GenerationExecutionContext("job", "tenant", lambda: False, 30),
    )
    return request, sink, output


def test_publication_uses_high_level_artifact_client_with_manifest_and_shards() -> None:
    request, sink, output = generated_dataset()
    client = CapturingArtifactClient([])
    try:
        published = ArtifactDatasetPublisher(client).publish(  # type: ignore[arg-type]
            job_id="job_1234567890abcdef1234567890abcdef",
            tenant_id="tenant",
            idempotency_key="publish-key",
            dataset=output,
            manifest={
                "request_digest": request.digest,
                "format": "jsonl",
                "record_count": output.record_count,
            },
            source_revision="a" * 40,
            correlation_id="correlation",
        )
    finally:
        sink.cleanup()
    assert published.digest == ARTIFACT_DIGEST
    call = client.calls[0]
    assert call["kind"] == "synthetic-data.dataset"
    assert len(call["layers"]) == 1 + len(output.shards)
    assert call["layers"][0].annotations["role"] == "dataset-manifest"
    assert all(layer.annotations["role"] == "dataset-shard" for layer in call["layers"][1:])
    assert "oci" not in call and "registry" not in call


class FailingArtifactClient:
    def publish(self, **kwargs: Any):
        del kwargs
        raise OCITransportError("simulated digest mismatch")


def test_publication_failure_never_returns_partial_reference() -> None:
    request, sink, output = generated_dataset()
    try:
        with pytest.raises(SyntheticDataError) as captured:
            ArtifactDatasetPublisher(FailingArtifactClient()).publish(  # type: ignore[arg-type]
                job_id="job_1234567890abcdef1234567890abcdef",
                tenant_id="tenant",
                idempotency_key="publish-key",
                dataset=output,
                manifest={"request_digest": request.digest, "format": "jsonl"},
                source_revision="a" * 40,
                correlation_id="correlation",
            )
    finally:
        sink.cleanup()
    assert captured.value.code is ErrorCode.PUBLICATION_FAILED
    assert captured.value.retryable
