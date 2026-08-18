FROM python:3.13-slim@sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a

RUN useradd --create-home --uid 65532 --shell /usr/sbin/nologin juntai
WORKDIR /opt/juntai
COPY . /opt/juntai
RUN python -m pip install --no-cache-dir /opt/juntai \
    && find /opt/juntai -type d -exec chmod 0555 {} + \
    && find /opt/juntai -type f -exec chmod 0444 {} +

USER 65532:65532
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["juntai-synthetic-data"]
CMD ["serve"]
