"""FuseAPI lifecycle component that drains bounded queued work."""

from __future__ import annotations

import asyncio
from contextlib import suppress

from juntai_synthetic_data.jobs.models import JobState
from juntai_synthetic_data.service import SyntheticDataService


class JobScheduler:
    def __init__(self, service: SyntheticDataService, *, poll_interval: float = 0.25) -> None:
        self.service = service
        self.poll_interval = poll_interval
        self._accepting = False
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def validate(self) -> None:
        if self.poll_interval <= 0:
            raise ValueError("scheduler poll interval must be positive")

    async def materialize(self) -> None:
        return None

    async def start(self) -> None:
        self._stopping.clear()
        self._accepting = True
        self._task = asyncio.create_task(self._run(), name="synthetic-data-scheduler")

    async def readiness(self) -> None:
        if not self._accepting or self._task is None or self._task.done():
            raise RuntimeError("scheduler is not ready")

    async def remove_readiness(self) -> None:
        self._accepting = False

    async def drain(self) -> None:
        self._accepting = False
        while any(
            job.state in {JobState.RUNNING, JobState.VALIDATING, JobState.PUBLISHING}
            for job in self.service.repository.list_runnable()
        ):
            await asyncio.sleep(self.poll_interval)

    async def stop(self) -> None:
        self._stopping.set()
        task = self._task
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._task = None

    async def run_once(self) -> int:
        executed = 0
        for job in self.service.repository.list_runnable(limit=100):
            if job.state is JobState.QUEUED:
                await asyncio.to_thread(self.service.run_job, job.tenant_id, job.job_id)
                executed += 1
        return executed

    async def _run(self) -> None:
        while not self._stopping.is_set():
            if self._accepting:
                await self.run_once()
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=self.poll_interval)
