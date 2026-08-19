# Generation contract reference

`juntai.synthetic-data.contract/v1` describes bounded structure only.

- `records` contains 1 to 64 uniquely named record families.
- Each family declares a positive maximum count and 1 to 256 fields.
- Field types are `string`, `integer`, `number`, `boolean`, `date`, or `datetime`.
- Distributions are `constant`, `sequence`, `choice`, `uniform`, `normal`, or `uuid` with kind-specific required parameters.
- A relation uses `record.field` endpoints. Its target field must be declared unique.
- `bounds` limits total records, bytes, and shards. Declared family counts must fit `max_records`.
- Output is `jsonl` or `csv`, with `none` or `gzip` compression.
- Metadata is bounded opaque text; it is not interpreted as domain semantics.

Hard public maxima are 1,000,000 records, 1,073,741,824 bytes, 15 shards, 64 record families, 256 fields per family, and 256 relations. Tenant policy and quota may impose lower limits.

The provider request declares class, deterministic requirement, modes, and maximum runtime. The default released provider class is `tabular`. Deterministic jobs pin the seed and provider behavior; a provider that cannot satisfy determinism fails before generation.
