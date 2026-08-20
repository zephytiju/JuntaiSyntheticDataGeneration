FROM adapter-artifacts AS adapter-artifacts
FROM iam-artifacts AS iam-artifacts

FROM python:3.13-slim@sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a

RUN useradd --create-home --uid 65532 --shell /usr/sbin/nologin juntai
WORKDIR /opt/juntai
COPY --from=adapter-artifacts / /tmp/platform-adapters/
COPY --from=iam-artifacts / /tmp/iam-artifacts/
COPY . /opt/juntai
RUN test "$(sha256sum /tmp/platform-adapters/juntai_platform_queue_kafka-1.0.0-py3-none-any.whl | cut -d ' ' -f 1)" = "d787126955c11e27ec05ca7c22e8f945cf0a89bf989c1e438ee86640e56622dc" \
    && test "$(sha256sum /tmp/platform-adapters/juntai_platform_swp_stream-1.0.0-py3-none-any.whl | cut -d ' ' -f 1)" = "cba7a87783cd804f5e496473f0961757c27a0455b946ed82041a7d1d01ef6033" \
    && test "$(sha256sum /tmp/iam-artifacts/juntai_iam-1.1.0-py3-none-any.whl | cut -d ' ' -f 1)" = "007362537726dbd69c75952b73c62b90e4f7ea92a48ab214ba0ad3ffcb533e6c" \
    && test "$(sha256sum /tmp/iam-artifacts/juntai_iam_contracts-1.1.1-py3-none-any.whl | cut -d ' ' -f 1)" = "e1daa81386669cfbf74b119c73f822d80a2f5e7a64a187538c54dcff07643cf1" \
    && python -m pip install --no-cache-dir \
      /tmp/platform-adapters/*.whl /tmp/iam-artifacts/*.whl /opt/juntai \
    && rm -rf /tmp/platform-adapters /tmp/iam-artifacts \
    && find /opt/juntai -type d -exec chmod 0555 {} + \
    && find /opt/juntai -type f -exec chmod 0444 {} +

USER 65532:65532
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["juntai-synthetic-data"]
CMD ["serve"]
