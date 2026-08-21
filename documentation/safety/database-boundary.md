# Database and data boundary

Synthetic writes preview data to caller-declared application schemas in the test fleet's single
application database cluster. This is application data, not Platform data. The injected connection
cannot select Platform databases, production application databases, another application cluster, or
deployment control planes.

The service retains request/digest/provider/timestamp metadata and the exact generated keys required
for deletion. It does not retain duplicate generated payloads, export files, Artifact references,
queue messages, or worker state. Logs and telemetry contain bounded identifiers and digests, not
generated row values or credentials.

RLS isolates service metadata by verified tenant. Application-table isolation is enforced by the
existing application schema and tenant model; Synthetic does not weaken or replace it.
