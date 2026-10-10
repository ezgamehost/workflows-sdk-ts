#!/usr/bin/env bash
# Runs workflows-stack's conformance harness against conformance/app.ts (push scenarios) and
# conformance/worker.ts (pull scenarios: the worker polls the harness on --pull-addr).
#   scripts/conformance.sh <workflows-stack checkout at .workflows-stack-rev>
set -euo pipefail
stack=$(cd "${1:?usage: scripts/conformance.sh <workflows-stack checkout>}" && pwd)
root=$(pwd)
port=${PORT:-3100}
pull_port=${PULL_PORT:-3300}
secret=whsec_$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')
mkdir -p .bin
(cd "$stack" && go build -o "$root/.bin/workflows-conformance" ./cmd/workflows-conformance)
CONFORMANCE_SECRET=$secret PORT=$port bun conformance/app.ts & app=$!
# The worker starts polling before the harness listens; it backs off (at most 500 ms) until it's up.
CONFORMANCE_LIVE=http://127.0.0.1:$pull_port bun conformance/worker.ts & wk=$!
trap 'kill $app $wk 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.1; done
.bin/workflows-conformance run --app "http://127.0.0.1:$port/" --secret "$secret" --scenarios "$stack/protocol/conformance" \
  --pull-addr "127.0.0.1:$pull_port" --api-key conformance-key --pull-app 0199a3c2-7d30-7f62-9b5c-4eaf8d3f6b92 --pull-queue conformance
