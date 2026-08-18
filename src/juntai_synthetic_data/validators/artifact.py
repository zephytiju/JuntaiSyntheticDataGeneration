"""Exact validator Artifact resolution through the released Artifact SDK."""

from __future__ import annotations

from collections.abc import Callable

import grpc
from juntai.artifact import ArtifactClient
from juntai.artifact.errors import ArtifactError

from juntai_synthetic_data.contracts.models import ValidatorDescriptor
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


class ArtifactValidatorResolver:
    def __init__(
        self,
        client: ArtifactClient,
        caller_credentials: grpc.CallCredentials | Callable[[], grpc.CallCredentials],
    ) -> None:
        self.client = client
        self.caller_credentials = caller_credentials

    def resolve_exact(self, descriptor: ValidatorDescriptor) -> tuple[bytes, ...]:
        credentials = (
            self.caller_credentials()
            if callable(self.caller_credentials)
            else self.caller_credentials
        )
        try:
            reference = self.client.resolve_metadata(
                artifact_id=descriptor.artifact_id,
                version_id=descriptor.version_id,
                digest=descriptor.digest,
                caller_credentials=credentials,
            )
            if reference.manifest_digest != descriptor.digest:
                raise ValueError("validator Artifact digest differs from descriptor")
            layers = self.client.resolve_and_download(reference)
        except (ArtifactError, TimeoutError, ConnectionError, ValueError) as exc:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "exact validator Artifact resolution failed",
                retryable=True,
            ) from exc
        return tuple(layer.data for layer in layers)
