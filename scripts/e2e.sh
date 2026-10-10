#!/usr/bin/env bash
# End to end: this SDK against the real workflows-stack (api, live, engine, dispatch in one
# process, `workflows-stack all`) on Postgres, Redis and an S3 fixture, with org-api faked by
# workflows-conformance fake-org-api.
#   scripts/e2e.sh <workflows-stack checkout>
# Needs DATABASE_URL (use a database of its own: it migrates and fills it). REDIS_URL and B2_* are
# optional locally (see below); CI sets them.
set -euo pipefail
stack=$(cd "${1:?usage: scripts/e2e.sh <workflows-stack checkout>}" && pwd)
root=$(pwd)
mkdir -p .bin
(cd "$stack" && go build -o "$root/.bin/workflows-stack" ./cmd/workflows-stack && go build -o "$root/.bin/workflows-conformance" ./cmd/workflows-conformance)

export E2E_API_KEY=ezgh_E2Etestkey000000000000000000000000000000
export E2E_ORG=0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70 E2E_PROJECT=0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81
# APP_ENV=local: payloads default to the S3 test fixture on 127.0.0.1:41203 (B2_* override it, as CI
# does), the KEK and service credential have local defaults, and REDIS_URL is optional.
export APP_ENV=local REGION=us-local-1 INTERNAL_API_TOKEN=e2e-internal-token
export ORG_API_URL=http://127.0.0.1:4100 WORKFLOWS_LIVE_URL=http://127.0.0.1:6150 WORKFLOWS_EGRESS_ALLOW_LOCAL=1
# Exact JSON replay (key order, NUL) needs the engine's json-column migration 0005.
if compgen -G "$stack/internal/store/migrations/0005_*" > /dev/null; then export E2E_EXACT_JSON=1; fi
E2E_STATE=$(mktemp) && rm -f "$E2E_STATE" && export E2E_STATE
pids=()
cleanup() { for p in "${pids[@]}"; do pkill -P "$p" 2>/dev/null || true; kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
wait_for() { for _ in $(seq 1 150); do curl -fsS -o /dev/null "$1" && return 0; sleep 0.2; done; echo "timed out waiting for $1" >&2; return 1; }

.bin/workflows-conformance fake-org-api --addr 127.0.0.1:4100 --api-key "$E2E_API_KEY" --internal-token "$INTERNAL_API_TOKEN" & pids+=($!)
wait_for http://127.0.0.1:4100/health
.bin/workflows-stack migrate
.bin/workflows-stack all > .bin/stack.log 2>&1 & pids+=($!)
wait_for http://127.0.0.1:6100/ready || { tail -n 200 .bin/stack.log; exit 1; }

read -r E2E_APP_ID EZGH_WORKFLOWS_SIGNING_SECRET < <(bun e2e/setup.ts) || { tail -n 100 .bin/stack.log; exit 1; }
export E2E_APP_ID EZGH_WORKFLOWS_SIGNING_SECRET

# The app runs under a supervisor: e2e-crash exits it mid-step on purpose.
( while true; do PORT=3200 bun e2e/app.ts || true; sleep 0.2; done ) & pids+=($!)
wait_for http://127.0.0.1:3200/__e2e/state

bun e2e/run.ts || { echo "--- stack log"; tail -n 300 .bin/stack.log; exit 1; }
