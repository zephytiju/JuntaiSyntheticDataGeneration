from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import cast

from juntai.sdk.fuse_api import OpenAPIArtifactGenerator, ServiceArtifactIdentity

from juntai_synthetic_data.api import build_generation_group
from juntai_synthetic_data.api.openapi import apply_bearer_security
from juntai_synthetic_data.service import SyntheticDataService


def main() -> None:
    commit = os.environ["SOURCE_COMMIT"]
    identity = ServiceArtifactIdentity(
        service="synthetic-data-generation", version="1.3.0", source_commit=commit
    )
    service = cast(SyntheticDataService, object())
    bundle = OpenAPIArtifactGenerator(fuse_api_version="2.0.0").generate(
        [build_generation_group(service)],
        identity=identity,
        title="Juntai Synthetic Data Generation",
    )
    output = Path("dist")
    bundle.write_to(output)
    openapi_path = output / bundle.openapi_path
    document = apply_bearer_security(json.loads(openapi_path.read_text()))
    payload = (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode()
    openapi_path.write_bytes(payload)
    checksum = hashlib.sha256(payload).hexdigest()
    (openapi_path.parent / "synthetic-data-generation.v1.sha256").write_text(
        f"{checksum}  {openapi_path.name}\n", encoding="utf-8", newline="\n"
    )


if __name__ == "__main__":
    main()
