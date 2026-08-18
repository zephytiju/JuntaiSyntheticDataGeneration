from __future__ import annotations

from types import SimpleNamespace

from conftest import VALIDATOR_DIGEST

from juntai_synthetic_data.contracts.models import ValidatorDescriptor
from juntai_synthetic_data.validators import ArtifactValidatorResolver


class FakeArtifactClient:
    def __init__(self) -> None:
        self.metadata = None
        self.downloaded = None

    def resolve_metadata(self, **kwargs):
        self.metadata = kwargs
        return SimpleNamespace(manifest_digest=VALIDATOR_DIGEST)

    def resolve_and_download(self, reference):
        self.downloaded = reference
        return (SimpleNamespace(data=b"validator-layer"),)


def test_validator_resolves_exact_coordinates_before_download() -> None:
    client = FakeArtifactClient()
    credentials = object()
    resolver = ArtifactValidatorResolver(client, credentials)  # type: ignore[arg-type]
    descriptor = ValidatorDescriptor(
        artifact_id="art_validator",
        version_id="artv_validator",
        digest=VALIDATOR_DIGEST,
        entry_point="validator:validate_dataset",
    )
    layers = resolver.resolve_exact(descriptor)
    assert layers == (b"validator-layer",)
    assert client.metadata == {
        "artifact_id": descriptor.artifact_id,
        "version_id": descriptor.version_id,
        "digest": descriptor.digest,
        "caller_credentials": credentials,
    }
    assert client.downloaded.manifest_digest == descriptor.digest
