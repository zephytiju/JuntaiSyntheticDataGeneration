from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

from juntai.sdk.fuse_api.adapters.http import HTTPAdapter

from juntai_synthetic_data.api import build_job_group
from juntai_synthetic_data.api.openapi import apply_bearer_security
from juntai_synthetic_data.service import SyntheticDataService

ROOT = Path(__file__).parents[1]


def main() -> None:
    service = cast(SyntheticDataService, object())
    app = HTTPAdapter(title="Juntai Synthetic Data Generation", version="1.2.0").build(
        [build_job_group(service)], []
    )
    document = apply_bearer_security(app.openapi())
    content = (
        json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )
    target = ROOT / "openapi" / "synthetic-data-generation.v1.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    digest = hashlib.sha256(content.encode()).hexdigest()
    (target.parent / "synthetic-data-generation.v1.sha256").write_text(f"{digest}  {target.name}\n")


if __name__ == "__main__":
    main()
