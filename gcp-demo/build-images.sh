#!/usr/bin/env bash
# Build & push the two images the demo needs YOU to provide:
#
#   ./build-images.sh            # both (ingress, then restate)
#   ./build-images.sh ingress    # only the custom ingress image (base + GCP auth handler)
#   ./build-images.sh restate    # only the Restate image (built from ../restate)
#
# - ingress: builds the base image (./gradlew jibDockerBuild -> $BASE_INGRESS_IMAGE),
#            layers the GCP auth handler (Dockerfile.gcp), and pushes $INGRESS_IMAGE.
#            Set SKIP_BASE_BUILD=1 to reuse an already-built base.
# - restate: `docker build` the Restate source at $RESTATE_SRC_DIR (default ../restate)
#            into $RESTATE_IMAGE, then pushes it.
#
# Prereq: gcloud auth login.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
require_cmd gcloud "${CONTAINER_ENGINE}"
require_var PROJECT_ID REGION

authorize() {
  step "Authorizing ${CONTAINER_ENGINE} against ${REGION}-docker.pkg.dev"
  ensure_ar_repo
  gcloud auth configure-docker "${REGION}-docker.pkg.dev" -q
}

build_ingress() {
  require_var INGRESS_IMAGE
  local root="${HERE}/.."
  if [ "${SKIP_BASE_BUILD:-0}" = "1" ]; then
    info "SKIP_BASE_BUILD=1 -> reusing existing base ${BASE_INGRESS_IMAGE}"
  else
    step "Building base ingress image (${BASE_INGRESS_IMAGE}) via ./gradlew jibDockerBuild"
    ( cd "${root}" && ./gradlew jibDockerBuild )
  fi
  step "Layering GCP auth handler -> ${INGRESS_IMAGE}"
  "${CONTAINER_ENGINE}" build \
    -f "${HERE}/Dockerfile.gcp" \
    --build-arg BASE_IMAGE="${BASE_INGRESS_IMAGE}" \
    --build-arg AUTH_HANDLER_VERSION="${AUTH_HANDLER_VERSION}" \
    -t "${INGRESS_IMAGE}" \
    "${HERE}"
  step "Pushing ${INGRESS_IMAGE}"
  "${CONTAINER_ENGINE}" push "${INGRESS_IMAGE}"
  ok "ingress image pushed: ${INGRESS_IMAGE}"
}

build_restate() {
  require_var RESTATE_IMAGE
  local src="${RESTATE_SRC_DIR:-${HERE}/../../restate}"
  local dockerfile="${src}/${RESTATE_DOCKERFILE}"
  [ -d "${src}" ] || die "Restate source dir not found: ${src} (set RESTATE_SRC_DIR in config.env)"
  [ -f "${dockerfile}" ] || die "Restate Dockerfile not found: ${dockerfile} (set RESTATE_DOCKERFILE)"
  step "Building ${RESTATE_IMAGE} from ${src} (${RESTATE_DOCKERFILE})"
  "${CONTAINER_ENGINE}" build -f "${dockerfile}" -t "${RESTATE_IMAGE}" "${src}"
  step "Pushing ${RESTATE_IMAGE}"
  "${CONTAINER_ENGINE}" push "${RESTATE_IMAGE}"
  ok "restate image pushed: ${RESTATE_IMAGE}"
}

case "${1:-all}" in
  ingress) authorize; build_ingress ;;
  restate) authorize; build_restate ;;
  all)     authorize; build_ingress; build_restate ;;
  *) die "usage: $0 [all|ingress|restate]" ;;
esac
