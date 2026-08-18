"""Provenance projection containing no credentials or target-store bindings."""

from __future__ import annotations

from datetime import datetime

from juntai_synthetic_data.contracts.models import PROVENANCE_VERSION
from juntai_synthetic_data.dataset import DatasetOutput
from juntai_synthetic_data.providers.base import GeneratorProviderManifest
from juntai_synthetic_data.publication import PublishedDataset
from juntai_synthetic_data.validators import ValidatorEvidence


def build_provenance(
    *,
    job_id: str,
    request_digest: str,
    contract_digest: str,
    seed: str,
    policy_digest: str,
    quota_reservation_id: str,
    provider: GeneratorProviderManifest,
    validator_reference: str | None,
    validator_evidence: ValidatorEvidence | None,
    dataset: DatasetOutput,
    artifact: PublishedDataset,
    started_at: datetime,
    completed_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": PROVENANCE_VERSION,
        "job_id": job_id,
        "request_digest": request_digest,
        "contract_digest": contract_digest,
        "provider_id": provider.provider_id,
        "provider_version": provider.version,
        "model_identity": provider.model_identity,
        "model_version": provider.model_version,
        "seed": seed,
        "policy_digest": policy_digest,
        "quota_reservation_id": quota_reservation_id,
        "worker_image_digest": provider.worker_image_digest,
        "validator_reference": validator_reference,
        "validation_digest": validator_evidence.digest if validator_evidence else None,
        "logical_dataset_digest": dataset.logical_digest,
        "artifact_digest": artifact.digest,
        "record_count": dataset.record_count,
        "byte_count": dataset.byte_count,
        "shard_count": len(dataset.shards),
        "started_at": started_at,
        "completed_at": completed_at,
    }
