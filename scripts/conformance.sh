#!/usr/bin/env bash
# Runs workflows-stack's conformance harness against conformance/app.ts.
#   scripts/conformance.sh <workflows-stack checkout at .workflows-stack-rev>
set -euo pipefail
stack=$(cd "${1:?usage: scripts/conformance.sh <workflows-stack checkout>}" && pwd)
root=$(pwd)
port=${PORT:-3100}
secret=whsec_$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')
mkdir -p .bin
(cd "$stack" && go build -o "$root/.bin/workflows-conformance" ./cmd/workflows-conformance)
CONFORMANCE_SECRET=$secret PORT=$port bun conformance/app.ts & app=$!
trap 'kill $app 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.1; done
.bin/workflows-conformance run --app "http://127.0.0.1:$port/" --secret "$secret" --scenarios "$stack/protocol/conformance"
