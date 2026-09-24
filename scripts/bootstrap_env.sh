#!/usr/bin/env bash
# =============================================================================
#  Creates a local .env from .env.example with freshly generated secrets.
#  Nothing produced here is ever committed (.env is git-ignored).
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

if [[ -f .env ]]; then
    echo "[bootstrap] .env already exists — leaving it untouched."
    echo "[bootstrap] Delete it first if you want to regenerate secrets."
    exit 0
fi

echo "[bootstrap] generating .env from .env.example"
cp .env.example .env

gen_secret() { openssl rand -base64 24 | tr -d '\n' | tr '+/' '-_'; }
# A Fernet key is exactly 32 random bytes, url-safe base64 encoded.
gen_fernet() { openssl rand -base64 32 | tr -d '\n' | tr '+/' '-_'; }

MINIO_PWD="$(gen_secret)"
PG_PWD="$(gen_secret)"
AF_PWD="$(gen_secret)"
AF_SECRET="$(gen_secret)"
AF_FERNET="$(gen_fernet)"
HOST_UID="$(id -u)"
HOST_GID="$(id -g)"
DOCKER_GID="$(getent group docker | cut -d: -f3 || echo 999)"
: "${DOCKER_GID:=999}"

sed -i \
    -e "s|^MINIO_ROOT_PASSWORD=.*|MINIO_ROOT_PASSWORD=${MINIO_PWD}|" \
    -e "s|^AWS_SECRET_ACCESS_KEY=.*|AWS_SECRET_ACCESS_KEY=${MINIO_PWD}|" \
    -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${PG_PWD}|" \
    -e "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=${AF_PWD}|" \
    -e "s|^AIRFLOW_SECRET_KEY=.*|AIRFLOW_SECRET_KEY=${AF_SECRET}|" \
    -e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=${AF_FERNET}|" \
    -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=${HOST_UID}|" \
    -e "s|^HOST_UID=.*|HOST_UID=${HOST_UID}|" \
    -e "s|^HOST_GID=.*|HOST_GID=${HOST_GID}|" \
    .env

# LAKEHOUSE_HOST_DIR: the DockerOperator launches SIBLING containers on the
# host's docker daemon, so the paths it mounts must be HOST paths, not paths
# inside the Airflow container. This is the classic docker-socket gotcha.
if ! grep -q '^LAKEHOUSE_HOST_DIR=' .env; then
    printf '\n# Absolute path of this project ON THE HOST (Airflow DockerOperator mounts)\nLAKEHOUSE_HOST_DIR=%s\n' "$(pwd)" >> .env
fi

# DOCKER_GID is only needed by the Airflow scheduler (DockerOperator).
if ! grep -q '^DOCKER_GID=' .env; then
    printf '\n# Host docker group id (needed by Airflow DockerOperator)\nDOCKER_GID=%s\n' "${DOCKER_GID}" >> .env
fi

chmod 600 .env

cat <<MSG

[bootstrap] .env created.

    MinIO console   http://localhost:9001
        user        $(grep '^MINIO_ROOT_USER=' .env | cut -d= -f2)
        password    ${MINIO_PWD}

    Airflow UI      http://localhost:8088
        user        $(grep '^AIRFLOW_ADMIN_USER=' .env | cut -d= -f2)
        password    ${AF_PWD}

  These credentials live in .env only, which is git-ignored.
  Run 'make creds' to print them again.

MSG
