#!/usr/bin/env bash
#
# Regenerate the low-level core (core/) from the QueueFlow OpenAPI spec.
#
# The hand-written ergonomic facade in src/ is NEVER touched by this script.
# Only core/ (models + per-tag API clients + fetch runtime) is regenerated, so
# it can never drift from the server. Run after the spec changes.
#
# Requires: docker. The spec is read from the sibling queueflow-core-rs repo
# (override with SPEC=/path/to/openapi.json).

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC="${SPEC:-${ROOT}/../queueflow-core-rs/spec/openapi.json}"
CONFIG="${ROOT}/.openapi-core-config.yaml"
OUT="${ROOT}/core"
IMAGE="openapitools/openapi-generator-cli:v7.10.0"

if [[ ! -f "${SPEC}" ]]; then
  echo "spec not found: ${SPEC}" >&2
  echo "Generate it first: (cd ../queueflow-core-rs && make spec)" >&2
  exit 1
fi

# Wipe core/ so stale generated files never linger, then regenerate.
find "${OUT}" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
mkdir -p "${OUT}"

docker run --rm \
  -v "${SPEC}:/spec/openapi.json:ro" \
  -v "${CONFIG}:/config.yaml:ro" \
  -v "${OUT}:/out" \
  "${IMAGE}" generate \
    -i /spec/openapi.json -c /config.yaml -o /out

echo "==> Regenerated ${OUT} from ${SPEC}"
echo "==> Run 'npm run typecheck' to verify the facade still matches."
