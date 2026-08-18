"""Bounded canonical temporary dataset and shard construction."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from juntai_synthetic_data.contracts.models import GenerationContract, canonical_json
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


@dataclass(frozen=True)
class DatasetShard:
    name: str
    media_type: str
    data: bytes
    digest: str
    record_count: int


@dataclass(frozen=True)
class DatasetOutput:
    shards: tuple[DatasetShard, ...]
    record_count: int
    byte_count: int
    logical_digest: str


class BoundedDatasetSink:
    """Writes only to a private ephemeral workspace and enforces hard output limits."""

    def __init__(self, contract: GenerationContract, *, root: str | None = None) -> None:
        self.contract = contract
        self._owned_root = root is None
        self.root = Path(root or tempfile.mkdtemp(prefix="juntai-synthetic-data-"))
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self._records: list[tuple[str, dict[str, Any]]] = []
        self._estimated_bytes = 0
        self._finalized = False

    def __enter__(self) -> BoundedDatasetSink:
        return self

    def __exit__(self, *_: object) -> None:
        self.cleanup()

    def write(self, record_type: str, record: dict[str, Any]) -> None:
        if self._finalized:
            raise RuntimeError("dataset sink is already finalized")
        if len(self._records) >= self.contract.bounds.max_records:
            raise SyntheticDataError(ErrorCode.OUTPUT_LIMIT_EXCEEDED, "record bound exceeded")
        line = canonical_json({"record_type": record_type, "record": record}) + b"\n"
        if self._estimated_bytes + len(line) > self.contract.bounds.max_bytes:
            raise SyntheticDataError(ErrorCode.OUTPUT_LIMIT_EXCEEDED, "byte bound exceeded")
        self._records.append((record_type, record))
        self._estimated_bytes += len(line)

    def _serialize(self) -> bytes:
        output = self.contract.output
        if output.format == "jsonl":
            raw = b"".join(
                canonical_json({"record_type": record_type, "record": record}) + b"\n"
                for record_type, record in self._records
            )
        else:
            fields = sorted({key for _, record in self._records for key in record})
            buffer = io.StringIO(newline="")
            writer = csv.DictWriter(
                buffer, fieldnames=["record_type", *fields], lineterminator="\n"
            )
            writer.writeheader()
            for record_type, record in self._records:
                writer.writerow({"record_type": record_type, **record})
            raw = buffer.getvalue().encode()
        if output.compression == "gzip":
            raw = gzip.compress(raw, mtime=0)
        return raw

    def finalize(self) -> DatasetOutput:
        if self._finalized:
            raise RuntimeError("dataset sink is already finalized")
        self._finalized = True
        raw = self._serialize()
        if len(raw) > self.contract.bounds.max_bytes:
            raise SyntheticDataError(
                ErrorCode.OUTPUT_LIMIT_EXCEEDED, "serialized bytes exceed bound"
            )
        shard_count = min(
            self.contract.bounds.max_shards,
            max(1, (len(raw) + 8_388_607) // 8_388_608),
        )
        chunk_size = max(1, (len(raw) + shard_count - 1) // shard_count)
        shards: list[DatasetShard] = []
        extension = self.contract.output.format
        if self.contract.output.compression == "gzip":
            extension += ".gz"
        for index, offset in enumerate(range(0, max(1, len(raw)), chunk_size)):
            data = raw[offset : offset + chunk_size]
            path = self.root / f"part-{index:05d}.{extension}"
            path.write_bytes(data)
            os.chmod(path, 0o600)
            digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
            media = f"application/vnd.juntai.synthetic-data.shard.v1+{self.contract.output.format}"
            if self.contract.output.compression != "none":
                media += f"+{self.contract.output.compression}"
            shards.append(
                DatasetShard(
                    path.name, media, data, digest, len(self._records) if index == 0 else 0
                )
            )
            if offset + chunk_size >= len(raw):
                break
        logical_document = {
            "contract_digest": self.contract.digest,
            "format": self.contract.output.format,
            "compression": self.contract.output.compression,
            "record_count": len(self._records),
            "byte_count": len(raw),
            "shards": [{"digest": shard.digest, "size": len(shard.data)} for shard in shards],
        }
        logical_digest = f"sha256:{hashlib.sha256(canonical_json(logical_document)).hexdigest()}"
        return DatasetOutput(tuple(shards), len(self._records), len(raw), logical_digest)

    def cleanup(self) -> None:
        self._records.clear()
        if self._owned_root and self.root.exists():
            shutil.rmtree(self.root)
