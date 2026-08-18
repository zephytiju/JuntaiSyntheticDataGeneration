from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from juntai.usage import UsageReporter

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.jobs import InMemoryJobRepository
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.publication import PublishedDataset
from juntai_synthetic_data.quotas import InMemoryQuotaLedger, QuotaLimits
from juntai_synthetic_data.service import SyntheticDataService
from juntai_synthetic_data.validators import (
    SandboxExecutionRequest,
    SandboxExecutionResult,
    ValidatorSandbox,
)

IMAGE_DIGEST = "sha256:" + "1" * 64
ARTIFACT_DIGEST = "sha256:" + "2" * 64
VALIDATOR_DIGEST = "sha256:" + "3" * 64


def request_data(*, validator: bool = False, max_bytes: int = 1_000_000) -> dict[str, Any]:
    value: dict[str, Any] = {
        "contract_version": "juntai.synthetic-data.request/v1",
        "generation_contract": {
            "contract_version": "juntai.synthetic-data.contract/v1",
            "records": [
                {
                    "record_type": "site",
                    "count": {"maximum": 3},
                    "fields": {
                        "site_id": {
                            "type": "string",
                            "unique": True,
                            "distribution": {"kind": "sequence", "start": 100, "step": 1},
                        }
                    },
                },
                {
                    "record_type": "asset",
                    "count": {"maximum": 6},
                    "fields": {
                        "asset_id": {"type": "string", "unique": True},
                        "site_id": {"type": "string"},
                        "reading": {
                            "type": "number",
                            "distribution": {"kind": "normal", "mean": 65, "stddev": 12},
                        },
                    },
                },
            ],
            "relations": [{"from": "asset.site_id", "to": "site.site_id", "required": True}],
            "bounds": {"max_records": 9, "max_bytes": max_bytes, "max_shards": 4},
            "output": {"format": "jsonl", "compression": "none"},
        },
        "seed": "acceptance-seed-1",
        "provider": {"class": "tabular", "requirements": {"deterministic": True}},
        "policy": {"data_classification": "synthetic", "source_examples": "none"},
    }
    if validator:
        value["validator"] = {
            "artifact_id": "art_validator",
            "version_id": "artv_validator_1",
            "digest": VALIDATOR_DIGEST,
            "entry_point": "validator:validate_dataset",
        }
    return value


@pytest.fixture
def sample_request() -> CreateJobRequest:
    return CreateJobRequest.model_validate(request_data())


@dataclass
class FakePublisher:
    fail: bool = False
    calls: list[dict[str, Any]] = field(default_factory=list)

    def publish(self, **kwargs: Any) -> PublishedDataset:
        self.calls.append(kwargs)
        if self.fail:
            from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

            raise SyntheticDataError(ErrorCode.PUBLICATION_FAILED, "simulated", retryable=True)
        return PublishedDataset(
            artifact_id="art_dataset",
            version_id="artv_dataset_1",
            digest=ARTIFACT_DIGEST,
            media_type="application/vnd.oci.image.manifest.v1+json",
        )


class PassingExecutor:
    def __init__(self) -> None:
        self.requests: list[SandboxExecutionRequest] = []

    def execute(self, request: SandboxExecutionRequest) -> SandboxExecutionResult:
        self.requests.append(request)
        return SandboxExecutionResult(passed=True, findings=("valid",))


class ExactResolver:
    def __init__(self) -> None:
        self.descriptors = []

    def resolve_exact(self, descriptor):
        self.descriptors.append(descriptor)
        return (b"exact-validator-artifact",)


def make_service(
    *,
    publisher: FakePublisher | None = None,
    executor: PassingExecutor | None = None,
    limits: QuotaLimits | None = None,
    usage_reporter: UsageReporter | None = None,
) -> SyntheticDataService:
    provider = DeterministicTabularProvider(worker_image_digest=IMAGE_DIGEST)
    return SyntheticDataService(
        repository=InMemoryJobRepository(),
        providers=ProviderRegistry((provider,)),
        policy=DefaultPolicyEngine(),
        quotas=InMemoryQuotaLedger(limits),
        publisher=publisher or FakePublisher(),
        validator_sandbox=ValidatorSandbox(executor, ExactResolver()) if executor else None,
        usage_reporter=usage_reporter,
        source_revision="a" * 40,
    )
