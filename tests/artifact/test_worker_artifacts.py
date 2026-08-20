from __future__ import annotations

from types import SimpleNamespace

import pytest

from juntai_synthetic_data.errors import SyntheticDataError
from juntai_synthetic_data.execution import ArtifactExactReferenceVerifier
from juntai_synthetic_data.worker_protocol import EVIDENCE_MEDIA_TYPE, ExactArtifactReference


class MetadataClient:
    def __init__(self, *, tenant_id: str = "tenant-a") -> None:
        self.tenant_id = tenant_id
        self.calls = []

    def resolve_metadata(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            tenant_id=self.tenant_id,
            manifest_media_type="application/vnd.oci.image.manifest.v1+json",
            layer_media_types=(EVIDENCE_MEDIA_TYPE,),
            provenance=SimpleNamespace(build_id="a" * 40),
        )


def reference() -> ExactArtifactReference:
    return ExactArtifactReference(
        tenantId="tenant-a",
        artifactId="art-exact",
        versionId="artv-exact",
        manifestDigest="sha256:" + "1" * 64,
        mediaType=EVIDENCE_MEDIA_TYPE,
        byteLength=128,
        producerBuildId="a" * 40,
    )


def test_api_verifies_exact_registry_coordinates_before_result_commit() -> None:
    client = MetadataClient()
    verifier = ArtifactExactReferenceVerifier(client, object())  # type: ignore[arg-type]
    verifier.verify(reference(), tenant_id="tenant-a")
    assert client.calls == [
        {
            "artifact_id": "art-exact",
            "version_id": "artv-exact",
            "digest": "sha256:" + "1" * 64,
            "caller_credentials": verifier.caller_credentials,
            "correlation_id": "art-exact",
        }
    ]


def test_api_fails_closed_on_registry_tenant_or_producer_mismatch() -> None:
    client = MetadataClient(tenant_id="tenant-b")
    verifier = ArtifactExactReferenceVerifier(client, object())  # type: ignore[arg-type]
    with pytest.raises(SyntheticDataError, match="exact result Artifact verification failed"):
        verifier.verify(reference(), tenant_id="tenant-a")
