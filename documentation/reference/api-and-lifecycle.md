# HTTP API and lifecycle

All routes require a bearer token for audience `juntai.synthetic-data.api` and the exact
tenant-scoped authorization action.

- `POST /v1/generations` requires `Idempotency-Key`. It returns `201` for a new atomic commit and
  `200` for an identical replay.
- `GET /v1/generations/{generation_id}` returns `COMMITTED` or `DELETED` metadata.
- `DELETE /v1/generations/{generation_id}` atomically deletes exact keyed application rows and marks
  the generation `DELETED`. Repeated deletion is idempotent.

There are no accepted, running, retrying, cancelling, failed-job, result-download, or worker states.
Transient database failures receive only bounded request-local retries. If POST ultimately fails,
the database transaction contains neither application rows nor Synthetic generation evidence.
