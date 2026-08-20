"""Production composition for the service-owned SWP queue relay."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import stat
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from juntai_platform_queue_kafka import QueueTransport

from juntai_synthetic_data.execution.coordinator import (
    CONTROL_CHANNEL,
    DEAD_LETTER_CHANNEL,
    DISPATCH_CHANNEL,
    RESULT_CHANNEL,
)
from juntai_synthetic_data.platform_adapter_contract import PLATFORM_ADAPTER_CONTRACT_SHA256
from juntai_synthetic_data.relay.runner import SyntheticRelay
from juntai_synthetic_data.relay.transport import validate_binding_metadata
from juntai_synthetic_data.runtime import build_runtime_service

TRANSPORT_FACTORY_ENV = "JUNTAI_QUEUE_TRANSPORT_FACTORY"
TOKEN_FILE_ENV = "JUNTAI_QUEUE_CREDENTIAL_FILE"
CONTRACT_MANIFEST_FILE_ENV = "JUNTAI_QUEUE_CONTRACT_MANIFEST_FILE"
CONTRACT_MANIFEST_SHA256_ENV = "JUNTAI_QUEUE_CONTRACT_MANIFEST_SHA256"
TRANSPORT_FACTORY = "juntai_platform_queue_kafka:create_transport"
TRANSPORT_DISTRIBUTION = "juntai-platform-queue-kafka"
TRANSPORT_VERSION = "1.0.0"
QUEUE_CREDENTIAL_FILE = "/var/run/secrets/juntai/queue-binding/credentials.json"
QUEUE_CREDENTIAL_SCHEMA = "juntai.platform.queue-credential/v1"
QUEUE_SERVER_NAME = "swp-executor.juntai-platform.svc.cluster.local"
ENDPOINT_ENV = {
    DISPATCH_CHANNEL: "JUNTAI_QUEUE_DISPATCH_ENDPOINT",
    CONTROL_CHANNEL: "JUNTAI_QUEUE_CONTROL_ENDPOINT",
    RESULT_CHANNEL: "JUNTAI_QUEUE_RESULT_ENDPOINT",
    DEAD_LETTER_CHANNEL: "JUNTAI_QUEUE_DEAD_LETTER_ENDPOINT",
}
QUEUE_ENVIRONMENT = frozenset(
    {
        TRANSPORT_FACTORY_ENV,
        TOKEN_FILE_ENV,
        CONTRACT_MANIFEST_FILE_ENV,
        CONTRACT_MANIFEST_SHA256_ENV,
        *ENDPOINT_ENV.values(),
    }
)


@dataclass(frozen=True)
class QueueCapabilityConfiguration:
    endpoints: dict[str, str]
    credential_file: str
    contract_manifest_file: str
    contract_manifest_sha256: str


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_QUEUE_ENDPOINT = re.compile(
    r"^grpcs://\[(?P<ip>[^\]]+)\]:7444/(?P<channel>[^/?#]+)\?serverName="
    + re.escape(QUEUE_SERVER_NAME)
    + r"$"
)


def _validate_read_only_projection(
    path_value: str | None,
    *,
    environment_name: str,
    maximum_bytes: int,
    exact_mode: int | None = None,
) -> Path:
    path = Path(path_value) if path_value else None
    if path is None or not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise RuntimeError(
            f"{environment_name} must name an existing absolute non-symlink regular file"
        )
    file_stat = path.stat()
    mode = stat.S_IMODE(file_stat.st_mode)
    if exact_mode is not None and mode != exact_mode:
        raise RuntimeError(f"{environment_name} must be a mode-{exact_mode:04o} file")
    if exact_mode is None and mode & 0o222:
        raise RuntimeError(f"{environment_name} must be read-only")
    if not 1 <= file_stat.st_size <= maximum_bytes:
        raise RuntimeError(f"{environment_name} exceeds its bounded projection")
    return path


def _validate_credential_binding(path: Path) -> None:
    _validate_read_only_projection(
        str(path),
        environment_name=TOKEN_FILE_ENV,
        maximum_bytes=16_384,
        exact_mode=0o400,
    )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RuntimeError("queue credential binding is not valid UTF-8 JSON") from error
    required = {
        "schemaVersion",
        "caFile",
        "certificateFile",
        "privateKeyFile",
        "serverName",
        "notBefore",
        "notAfter",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise RuntimeError("queue credential binding fields are incompatible")
    if value["schemaVersion"] != QUEUE_CREDENTIAL_SCHEMA:
        raise RuntimeError("queue credential binding schema is incompatible")
    if value["serverName"] != QUEUE_SERVER_NAME:
        raise RuntimeError("queue credential serverName is incompatible")
    for name in ("caFile", "certificateFile", "privateKeyFile"):
        referenced = Path(value[name]) if isinstance(value[name], str) else None
        if referenced is None or not referenced.is_absolute() or not referenced.is_file():
            raise RuntimeError(f"queue credential binding {name} is not an absolute file")
    if not all(isinstance(value[name], str) and value[name] for name in ("notBefore", "notAfter")):
        raise RuntimeError("queue credential validity metadata is absent")


def _validate_endpoint(value: str | None, *, channel: str, environment_name: str) -> str:
    match = _QUEUE_ENDPOINT.fullmatch(value or "")
    if match is None or match.group("channel") != channel:
        raise RuntimeError(
            f"required exact executor QueueTransport endpoint is missing or incompatible: "
            f"{environment_name}"
        )
    try:
        address = ipaddress.ip_address(match.group("ip"))
    except ValueError as error:
        raise RuntimeError(
            f"{environment_name} host must be a literal Service ClusterIP"
        ) from error
    if (
        not address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
    ):
        raise RuntimeError(f"{environment_name} host is not an approved Service ClusterIP")
    return match.group("ip")


def _capability_configuration() -> QueueCapabilityConfiguration:
    present = {name for name in os.environ if name.startswith("JUNTAI_QUEUE_")}
    if present != QUEUE_ENVIRONMENT:
        missing = sorted(QUEUE_ENVIRONMENT - present)
        extra = sorted(present - QUEUE_ENVIRONMENT)
        raise RuntimeError(
            "queue projection is incomplete or has extra names: "
            f"missing={','.join(missing)} extra={','.join(extra)}"
        )
    endpoints: dict[str, str] = {}
    authority: str | None = None
    for channel, name in ENDPOINT_ENV.items():
        value = os.getenv(name)
        endpoint_authority = _validate_endpoint(value, channel=channel, environment_name=name)
        if authority is not None and endpoint_authority != authority:
            raise RuntimeError("all four queue endpoints must share one executor authority")
        authority = endpoint_authority
        assert value is not None
        endpoints[channel] = value
    credential_file = os.getenv(TOKEN_FILE_ENV)
    if credential_file != QUEUE_CREDENTIAL_FILE:
        raise RuntimeError(f"{TOKEN_FILE_ENV} must be exactly {QUEUE_CREDENTIAL_FILE}")
    credential_path = Path(credential_file)
    _validate_credential_binding(credential_path)
    manifest_file = os.getenv(CONTRACT_MANIFEST_FILE_ENV)
    manifest_path = _validate_read_only_projection(
        manifest_file,
        environment_name=CONTRACT_MANIFEST_FILE_ENV,
        maximum_bytes=1_048_576,
    )
    manifest_sha256 = os.getenv(CONTRACT_MANIFEST_SHA256_ENV, "")
    if not _SHA256.fullmatch(manifest_sha256):
        raise RuntimeError(f"{CONTRACT_MANIFEST_SHA256_ENV} must be 64 lowercase hexadecimal")
    if manifest_sha256 != PLATFORM_ADAPTER_CONTRACT_SHA256:
        raise RuntimeError("queue contract manifest is not the pinned Platform contract")
    actual = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    if actual != manifest_sha256:
        raise RuntimeError("queue contract manifest digest mismatch")
    return QueueCapabilityConfiguration(
        endpoints=endpoints,
        credential_file=credential_file,
        contract_manifest_file=str(manifest_path),
        contract_manifest_sha256=manifest_sha256,
    )


def _transport() -> QueueTransport:
    reference = os.getenv(TRANSPORT_FACTORY_ENV, "")
    if reference != TRANSPORT_FACTORY:
        raise RuntimeError(f"{TRANSPORT_FACTORY_ENV} must be exactly {TRANSPORT_FACTORY}")
    try:
        installed_version = version(TRANSPORT_DISTRIBUTION)
    except PackageNotFoundError as error:
        raise RuntimeError(f"required queue adapter {TRANSPORT_DISTRIBUTION} is absent") from error
    if installed_version != TRANSPORT_VERSION:
        raise RuntimeError(
            f"required queue adapter version is {TRANSPORT_VERSION}, found {installed_version}"
        )
    module_name, attribute = reference.split(":", 1)
    factory: Callable[..., Any] = getattr(import_module(module_name), attribute)
    configuration = _capability_configuration()
    transport = factory(
        endpoints=configuration.endpoints,
        credential_file=configuration.credential_file,
        contract_manifest_file=configuration.contract_manifest_file,
        contract_manifest_sha256=configuration.contract_manifest_sha256,
    )
    required = (
        "binding_metadata",
        "publish",
        "receive",
        "renew",
        "acknowledge",
        "release",
        "reject",
    )
    if any(not callable(getattr(transport, method, None)) for method in required):
        raise RuntimeError("injected queue transport does not implement the required SPI")
    validate_binding_metadata(transport.binding_metadata())
    return transport


def _bounded_integer(name: str, *, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be in [{minimum}, {maximum}]")
    return value


def build_runtime_relay() -> SyntheticRelay:
    relay_id = os.getenv("JUNTAI_RELAY_ID")
    if not relay_id:
        raise RuntimeError("required environment variable is missing: JUNTAI_RELAY_ID")
    transport = _transport()
    service = build_runtime_service()
    return SyntheticRelay(
        relay_id=relay_id,
        repository=service.repository,
        service=service,
        transport=transport,
        batch_size=_bounded_integer("JUNTAI_RELAY_BATCH_SIZE", default=25, minimum=1, maximum=100),
    )
