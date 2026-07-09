#!/usr/bin/env sh
set -eu

BUILDER_NAME="${1:-ci-builder}"
CONTEXT_NAME="${BUILDER_NAME}-context"
DOCKER_ENDPOINT="${DOCKER_HOST:-unix:///var/run/docker.sock}"

docker run --privileged --rm tonistiigi/binfmt --install arm64 >/dev/null

if docker buildx inspect "${BUILDER_NAME}" >/dev/null 2>&1; then
  docker buildx use "${BUILDER_NAME}"
else
  if ! docker context inspect "${CONTEXT_NAME}" >/dev/null 2>&1; then
    DOCKER_CONTEXT_ARGS="host=${DOCKER_ENDPOINT}"

    if [ -n "${DOCKER_TLS_VERIFY:-}" ]; then
      CERT_PATH="${DOCKER_CERT_PATH:-/certs/client}"
      DOCKER_CONTEXT_ARGS="${DOCKER_CONTEXT_ARGS},ca=${CERT_PATH}/ca.pem,cert=${CERT_PATH}/cert.pem,key=${CERT_PATH}/key.pem"
    fi

    docker context create "${CONTEXT_NAME}" --docker "${DOCKER_CONTEXT_ARGS}" >/dev/null
  fi

  docker buildx create --use --name "${BUILDER_NAME}" --driver docker-container "${CONTEXT_NAME}"
fi

docker buildx inspect --bootstrap
