# Destination validation

Deployment supplies one application KingbaseES connection and an exact logical destination
allowlist. The service refuses startup or requests when that binding is absent, malformed, mutable,
or outside the test-fleet deployment.

Before generating data, the service compares each destination with the allowlist and live catalog.
The table and every mapped column must exist; generated and database types must be compatible; every
required column without a database default must be mapped; key fields must form a unique key; and
declared relations must match live foreign keys. The service database role must have only the
required insert, select, and delete permissions on approved application tables plus ownership of the
service metadata schema.

Identifiers are bounded lowercase SQL identifiers and are quoted by the service. Values are always
bound parameters. The API never accepts raw SQL or physical connection information.
