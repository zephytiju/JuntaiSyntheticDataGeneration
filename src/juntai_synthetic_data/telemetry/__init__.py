"""Stable low-cardinality telemetry vocabulary."""

SERVICE_NAME = "juntai-synthetic-data-generation"
GENERATION_DURATION_METRIC = "juntai.synthetic_data.generation.duration"
OUTPUT_RECORDS_METRIC = "juntai.synthetic_data.output.records"
OUTPUT_BYTES_METRIC = "juntai.synthetic_data.output.bytes"

__all__ = [
    "GENERATION_DURATION_METRIC",
    "OUTPUT_BYTES_METRIC",
    "OUTPUT_RECORDS_METRIC",
    "SERVICE_NAME",
]
