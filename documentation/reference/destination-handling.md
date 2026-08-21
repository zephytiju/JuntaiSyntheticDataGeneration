# Destination handling

Deployment supplies one application KingbaseES connection for the test fleet. Authenticated internal
callers authoritatively name the logical schema, table, column mapping, and key fields within that
database; requests cannot select another database or connection, and Synthetic performs no
live-catalog preflight or independent domain-destination authorization.

Synthetic validates only the bounded destination object needed by its generic contract: non-empty
schema/table names, a complete one-to-one field-to-column mapping, mapped key fields, and an acyclic
declared relation graph. It does not infer domain semantics or decide which application destination
is allowed.

Every destination identifier is quoted through the database driver and every generated value is a
bound parameter. KingbaseES enforces object and column existence, types, defaults, keys, foreign
keys, grants, constraints, and RLS when the transaction executes. Any rejection rolls back the whole
generation. The API never accepts raw SQL or physical connection information.
