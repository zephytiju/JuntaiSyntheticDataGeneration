# Generation contract

The unpublished V1 request uses `juntai.synthetic-data.request/v1` and contains:

- a deterministic seed and an in-process provider selection;
- record types with positive counts and bounded fields;
- per-record logical destination `{schema, table, columns, key_fields}`;
- optional relations between generated fields; and
- maximum total records and canonical generated-byte size.

`columns` maps every generated field exactly once to a database column. `key_fields` names generated
fields whose mapped columns form a live unique or primary key. A relation target must be unique, and
the relation must match a live database foreign key. V1 rejects relation graphs that cannot be
inserted and deleted in a deterministic dependency order.

The request cannot select a database, host, port, DSN, credential, tenant, arbitrary connection
option, or SQL fragment. Unknown fields are rejected. Provider output must match the exact declared
record types, counts, field types, uniqueness constraints, relations, and bounds.
