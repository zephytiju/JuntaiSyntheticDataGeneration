FROM python:3.13-slim@sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a

RUN useradd --create-home --uid 65532 --shell /usr/sbin/nologin juntai
WORKDIR /opt/juntai
COPY . /opt/juntai
RUN python -m pip download --no-deps --dest /tmp/iam \
      juntai-iam==1.1.0 juntai-iam-contracts==1.1.1 \
    && test "$(sha256sum /tmp/iam/juntai_iam-1.1.0-py3-none-any.whl | cut -d ' ' -f 1)" = \
      "007362537726dbd69c75952b73c62b90e4f7ea92a48ab214ba0ad3ffcb533e6c" \
    && test "$(sha256sum /tmp/iam/juntai_iam_contracts-1.1.1-py3-none-any.whl | cut -d ' ' -f 1)" = \
      "e1daa81386669cfbf74b119c73f822d80a2f5e7a64a187538c54dcff07643cf1" \
    && python -m pip install --no-cache-dir /tmp/iam/*.whl /opt/juntai \
    && rm -rf /tmp/iam \
    && python -c 'from juntai_synthetic_data.iam_contract import validate_iam_runtime; validate_iam_runtime()' \
    && find /opt/juntai -type d -exec chmod 0555 {} + \
    && find /opt/juntai -type f -exec chmod 0444 {} +

USER 65532:65532
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["juntai-synthetic-data"]
CMD ["serve"]
