from __future__ import annotations

import os
from pathlib import Path
from typing import cast

from juntai.sdk.fuse_api import OpenAPIArtifactGenerator, ServiceArtifactIdentity

from juntai_synthetic_data.api import build_job_group
from juntai_synthetic_data.service import SyntheticDataService


def main() -> None:
    commit = os.environ["SOURCE_COMMIT"]
    identity = ServiceArtifactIdentity(
        service="synthetic-data-generation", version="1.0.0", source_commit=commit
    )
    service = cast(SyntheticDataService, object())
    bundle = OpenAPIArtifactGenerator(fuse_api_version="2.0.0").generate(
        [build_job_group(service)], identity=identity, title="Juntai Synthetic Data Generation"
    )
    bundle.write_to(Path("dist"))


if __name__ == "__main__":
    main()
