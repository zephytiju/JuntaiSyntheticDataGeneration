"""Validated deployment allowlist and insert/delete ordering."""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from juntai_synthetic_data.contracts.models import GenerationContract

ALLOWLIST_VERSION = "juntai.synthetic-data.destination-allowlist/v1"
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


@dataclass(frozen=True)
class DestinationPlan:
    insert_order: tuple[str, ...]
    delete_order: tuple[str, ...]


class DestinationAllowlist:
    def __init__(self, destinations: frozenset[tuple[str, str]]) -> None:
        if not destinations:
            raise ValueError("destination allowlist must not be empty")
        self.destinations = destinations

    def allows(self, schema: str, table: str) -> bool:
        return (schema, table) in self.destinations

    @classmethod
    def from_document(cls, document: object) -> DestinationAllowlist:
        if not isinstance(document, dict) or set(document) != {"schemaVersion", "destinations"}:
            raise ValueError("destination allowlist has an invalid top-level shape")
        if document["schemaVersion"] != ALLOWLIST_VERSION:
            raise ValueError("destination allowlist version is unsupported")
        raw = document["destinations"]
        if not isinstance(raw, list) or not 1 <= len(raw) <= 256:
            raise ValueError("destination allowlist must contain 1 to 256 entries")
        destinations: set[tuple[str, str]] = set()
        for item in raw:
            if not isinstance(item, dict) or set(item) != {"schema", "tables"}:
                raise ValueError("destination allowlist entry has an invalid shape")
            schema = item["schema"]
            tables = item["tables"]
            if (
                not isinstance(schema, str)
                or _IDENTIFIER.fullmatch(schema) is None
                or not isinstance(tables, list)
                or not tables
            ):
                raise ValueError("destination allowlist schema or tables are invalid")
            for table in tables:
                if not isinstance(table, str) or _IDENTIFIER.fullmatch(table) is None:
                    raise ValueError("destination allowlist table is invalid")
                destinations.add((schema, table))
        return cls(frozenset(destinations))

    @classmethod
    def from_file(cls, path_value: str) -> DestinationAllowlist:
        path = Path(path_value)
        if not path.is_absolute():
            raise ValueError("destination allowlist file path must be absolute")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("destination allowlist must be a regular file")
            if metadata.st_size > 65_536:
                raise ValueError("destination allowlist exceeds 65536 bytes")
            with os.fdopen(descriptor, encoding="utf-8") as stream:
                descriptor = -1
                document = json.load(stream)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        return cls.from_document(document)


def plan_destinations(contract: GenerationContract) -> DestinationPlan:
    order = [record.record_type for record in contract.records]
    dependencies: dict[str, set[str]] = {name: set() for name in order}
    children: dict[str, set[str]] = {name: set() for name in order}
    for relation in contract.relations:
        child = relation.from_field.split(".", 1)[0]
        parent = relation.to_field.split(".", 1)[0]
        if child == parent:
            continue
        dependencies[child].add(parent)
        children[parent].add(child)
    ready = [name for name in order if not dependencies[name]]
    insert: list[str] = []
    while ready:
        current = ready.pop(0)
        insert.append(current)
        for child in order:
            if child not in children[current]:
                continue
            dependencies[child].discard(current)
            if not dependencies[child] and child not in ready and child not in insert:
                ready.append(child)
    if len(insert) != len(order):
        raise ValueError("relation graph must be acyclic")
    return DestinationPlan(tuple(insert), tuple(reversed(insert)))
