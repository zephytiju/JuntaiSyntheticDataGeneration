#!/bin/sh
set -eu

kes_image="${JUNTAI_REAL_KES_IMAGE:-kingbase_v009r001c010b0004_single_x86:v1@sha256:0bce318e74adca7a3d619b55b336269017507fd679833b7ce5d8400289661724}"
service_image="${1:-juntai-synthetic-data-generation:kes-acceptance}"
evidence_path="${2:-}"
kes_container="juntai-synthetic-kes-acceptance-01aa"
kes_network="juntai-synthetic-kes-acceptance-01aa"
worker_network="juntai-synthetic-worker-isolation-01aa"
kes_data="juntai-synthetic-kes-data-01aa"
kes_secrets="juntai-synthetic-kes-secrets-01aa"
service_secrets="juntai-synthetic-service-secrets-01aa"
acceptance_password="$(openssl rand -hex 24)"
source_revision="${JUNTAI_SOURCE_REVISION:-$(git rev-parse HEAD)}"
service_digest="${JUNTAI_SERVICE_IMAGE_DIGEST:-$(docker image inspect "$service_image" --format '{{.Id}}')}"

cleanup() {
  docker rm -f "$kes_container" >/dev/null 2>&1 || true
  docker volume rm -f "$kes_data" "$kes_secrets" "$service_secrets" >/dev/null 2>&1 || true
  docker network rm "$kes_network" >/dev/null 2>&1 || true
  docker network rm "$worker_network" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM
cleanup

docker network create "$kes_network" >/dev/null
docker network create --internal "$worker_network" >/dev/null
docker volume create "$kes_data" >/dev/null
docker volume create "$kes_secrets" >/dev/null
docker volume create "$service_secrets" >/dev/null

docker run --rm --user 0:0 --entrypoint /bin/sh \
  -e ACCEPTANCE_PASSWORD="$acceptance_password" \
  -v "$kes_secrets:/run/secrets" \
  -v "$service_secrets:/run/service-secrets" \
  "$service_image" -c \
  'umask 077
   printf "%s" "$ACCEPTANCE_PASSWORD" > /run/secrets/kes-password
   printf "host=%s port=54321 dbname=kingbase user=synthetic_migration_admin password=%s" \
     "juntai-synthetic-kes-acceptance-01aa" "$ACCEPTANCE_PASSWORD" \
     > /run/service-secrets/kes-dsn
   chown 65532:65532 /run/service-secrets/kes-dsn
   chown 1000:1000 /run/secrets/kes-password
   chmod 0400 /run/secrets/kes-password
   chmod 0400 /run/service-secrets/kes-dsn'

docker run -d --name "$kes_container" --network "$kes_network" --platform linux/amd64 \
  --entrypoint /bin/bash \
  -e DB_USER=synthetic_migration_admin -e DB_MODE=pg -e ENABLE_CI=no -e ENCODING=UTF-8 \
  -v "$kes_data:/home/kingbase/userdata" -v "$kes_secrets:/run/secrets:ro" \
  "$kes_image" -c \
  'export DB_PASSWORD="$(tr -d "\n" < /run/secrets/kes-password)"
   exec /bin/bash /home/kingbase/docker-entrypoint.sh' >/dev/null

wait_for_kes() {
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    set +e
    docker run --rm --network "$kes_network" \
      -v "$service_secrets:/run/secrets:ro" \
      -e JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE=/run/secrets/kes-dsn \
      -e JUNTAI_SOURCE_REVISION="$source_revision" \
      -e JUNTAI_SERVICE_IMAGE_DIGEST="$service_digest" \
      "$service_image" migrate --check >/dev/null 2>&1
    status=$?
    set -e
    if [ "$status" -eq 0 ] || [ "$status" -eq 5 ]; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 1
  done
  docker logs "$kes_container"
  return 1
}

wait_for_kes

worker_isolation_evidence="$(docker run --rm --network "$worker_network" \
  --entrypoint python "$service_image" -c \
  'import json,socket
from juntai_synthetic_data.worker import validate_worker_isolation
validate_worker_isolation(mountinfo="tmpfs /var/run/juntai-worker-tmp")
try:
    socket.create_connection(("juntai-synthetic-kes-acceptance-01aa", 54321), timeout=1)
except OSError:
    print(json.dumps({"check":"worker-kes-network-denied","result":"passed"},sort_keys=True))
else:
    raise SystemExit("isolated worker unexpectedly reached KES")')"

primary_evidence="$(docker run --rm --network "$kes_network" \
  -v "$service_secrets:/run/secrets:ro" \
  -e JUNTAI_SOURCE_REVISION="$source_revision" \
  -e JUNTAI_SERVICE_IMAGE_DIGEST="$service_digest" \
  --entrypoint python "$service_image" \
  /opt/juntai/scripts/real_kes_acceptance.py \
  --dsn-file /run/secrets/kes-dsn --phase primary)"

docker restart "$kes_container" >/dev/null
wait_for_kes

restart_evidence="$(docker run --rm --network "$kes_network" \
  -v "$service_secrets:/run/secrets:ro" \
  -e JUNTAI_SOURCE_REVISION="$source_revision" \
  -e JUNTAI_SERVICE_IMAGE_DIGEST="$service_digest" \
  --entrypoint python "$service_image" \
  /opt/juntai/scripts/real_kes_acceptance.py \
  --dsn-file /run/secrets/kes-dsn --phase post-restart)"

if [ -n "$evidence_path" ]; then
  python scripts/compose_real_kes_evidence.py \
    --primary "$primary_evidence" --post-restart "$restart_evidence" \
    --worker-isolation "$worker_isolation_evidence" \
    --kes-image "$kes_image" --out "$evidence_path"
else
  python scripts/compose_real_kes_evidence.py \
    --primary "$primary_evidence" --post-restart "$restart_evidence" \
    --worker-isolation "$worker_isolation_evidence" \
    --kes-image "$kes_image"
fi
