#!/usr/bin/env bash
set -euo pipefail

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SUBJECT="${"order-events-value":?subject required (e.g. order-events-value)}"
AVSC_FILE="${"src/main/resources/avro/order-events-value.avsc":?path to .avsc required}"

curl -sS -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT}/versions" \
  --data-binary "{\"schema\":$(jq -Rs . < "${AVSC_FILE}")}"
