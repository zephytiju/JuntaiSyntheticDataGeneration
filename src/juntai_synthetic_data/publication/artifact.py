"""Dataset Artifact composition over the released high-level Artifact client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from juntai.artifact import ArtifactClient, ArtifactLayer, Provenance
from juntai.artifact.errors import ArtifactError

from juntai_synthetic_data.contracts.models import MANIFEST_VERSION, canonical_json
from juntai_synthetic_data.dataset import DatasetOutput
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


@dataclass(frozen=True)
class PublishedDataset:
    artifact_id: str
    version_id: str
    digest: str
    media_type: str


class DatasetPublisher(Protocol):
    def publish(
        self,
        *,
        job_id: str,
        tenant_id: str,
        idempotency_key: str,
        dataset: DatasetOutput,
        manifest: dict[str, Any],
        source_revision: str,
        correlation_id: str,
    ) -> PublishedDataset: ...


class ArtifactDatasetPublisher:
    """Pushes bytes through ArtifactClient, never through Registry metadata RPC."""

    def __init__(self, client: ArtifactClient, *, namespace: str = "synthetic-data") -> None:
        self.client = client
        self.namespace = namespace

    def publish(
        self,
        *,
        job_id: str,
        tenant_id: str,
        idempotency_key: str,
        dataset: DatasetOutput,
        manifest: dict[str, Any],
        source_revision: str,
        correlation_id: str,
    ) -> PublishedDataset:
        del tenant_id  # tenant authority is supplied by workload identity at the Registry boundary.
        manifest_bytes = canonical_json({"schema_version": MANIFEST_VERSION, **manifest})
        layers = [
            ArtifactLayer(
                media_type="application/vnd.juntai.synthetic-data.manifest.v1+json",
                data=manifest_bytes,
                annotations={"role": "dataset-manifest"},
            )
        ]
        layers.extend(
            ArtifactLayer(
                media_type=shard.media_type,
                data=shard.data,
                annotations={"role": "dataset-shard", "name": shard.name},
            )
            for shard in dataset.shards
        )
        created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        try:
            published = self.client.publish(
                namespace=self.namespace,
                name=f"dataset-{job_id.removeprefix('job_')[:24]}",
                kind="synthetic-data.dataset",
                version=f"job-{job_id.removeprefix('job_')[:32]}",
                layers=layers,
                provenance=Provenance(
                    producer_identity="juntai-synthetic-data-generation",
                    source_revision=source_revision,
                    build_id=job_id,
                    created_at=created_at,
                ),
                idempotency_key=f"synthetic-data:{idempotency_key}",
                labels={"dataset-format": str(manifest["format"])},
                annotations={"request-digest": str(manifest["request_digest"])},
                correlation_id=correlation_id,
            )
        except (ArtifactError, TimeoutError, ConnectionError, ValueError) as exc:
            raise SyntheticDataError(
                ErrorCode.PUBLICATION_FAILED,
                "dataset Artifact publication did not commit an exact reference",
                retryable=True,
            ) from exc
        reference = published.reference
        return PublishedDataset(
            artifact_id=reference.artifact_id,
            version_id=reference.version_id,
            digest=reference.manifest_digest,
            media_type=reference.manifest_media_type,
        )
