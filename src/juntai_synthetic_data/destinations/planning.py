"""Deterministic insert/delete ordering for caller-declared destinations."""

from __future__ import annotations

from dataclasses import dataclass

from juntai_synthetic_data.contracts.models import GenerationContract


@dataclass(frozen=True)
class DestinationPlan:
    insert_order: tuple[str, ...]
    delete_order: tuple[str, ...]


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
