"""Generic asynchronous synthetic-data generation foundation."""

from .contracts.models import (
    CONTRACT_VERSION,
    REQUEST_VERSION,
    CreateJobRequest,
    GenerationContract,
    JobResult,
    JobStatus,
)
from .jobs.models import JobState
from .service import SyntheticDataService

__all__ = [
    "CONTRACT_VERSION",
    "REQUEST_VERSION",
    "CreateJobRequest",
    "GenerationContract",
    "JobResult",
    "JobState",
    "JobStatus",
    "SyntheticDataService",
]

__version__ = "1.2.0"
