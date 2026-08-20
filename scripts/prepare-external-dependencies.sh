#!/bin/sh
set -eu

platform_output="${1:-.platform-adapters}"
iam_output="${2:-.iam-artifacts}"
platform_repository="${JUNTAI_PLATFORM_REPOSITORY:-}"

python -m pip install --disable-pip-version-check \
  build==1.3.0 setuptools==80.9.0 wheel==0.45.1

if [ -z "$platform_repository" ]; then
  platform_repository="${RUNNER_TEMP:?}/juntai-platform-adapter-source"
  token="${JUNTAI_UPSTREAM_AUDIT_TOKEN:-${GITHUB_TOKEN:-}}"
  if [ -z "$token" ]; then
    echo "JUNTAI_UPSTREAM_AUDIT_TOKEN or GITHUB_TOKEN is required" >&2
    exit 1
  fi
  GH_TOKEN="$token" gh repo clone \
    zephytiju/JuntaiPlatformInfrastructure "$platform_repository" -- --filter=blob:none
  git -C "$platform_repository" checkout --detach \
    9095fde4bee086fd62a2868cabc079a0917af84d
fi

python scripts/prepare_platform_adapters.py \
  --platform-repository "$platform_repository" \
  --output "$platform_output"

mkdir "$iam_output"
python -m pip download --disable-pip-version-check --only-binary=:all: --no-deps \
  --dest "$iam_output" \
  juntai-iam==1.1.0 juntai-iam-contracts==1.1.1
python scripts/verify_iam_artifacts.py --directory "$iam_output"
python -m pip install --disable-pip-version-check --no-deps \
  "$platform_output"/*.whl "$iam_output"/*.whl
