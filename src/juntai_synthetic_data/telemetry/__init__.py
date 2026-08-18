"""Stable low-cardinality telemetry vocabulary."""

SERVICE_NAME = "juntai-synthetic-data-generation"
JOB_DURATION_METRIC = "juntai.synthetic_data.job.duration"
OUTPUT_RECORDS_METRIC = "juntai.synthetic_data.output.records"
OUTPUT_BYTES_METRIC = "juntai.synthetic_data.output.bytes"

__all__ = [
    "JOB_DURATION_METRIC",
    "OUTPUT_BYTES_METRIC",
    "OUTPUT_RECORDS_METRIC",
    "SERVICE_NAME",
]
