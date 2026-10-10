#!/usr/bin/env bash
# Type-checks a consumer of the packed package with older TypeScript versions, without
# skipLibCheck, under bundler and node16 resolution: the published .d.ts files must not need
# TypeScript 6 (for example generic Uint8Array<ArrayBuffer>).
#   scripts/types-compat.sh [typescript versions…]   (default: 5.4.5 5.6.3)
set -euo pipefail
root=$(pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
npm pack --silent --pack-destination "$work" >/dev/null
tarball=$(ls "$work"/*.tgz)
cd "$work"
cat > package.json <<JSON
{ "name": "consumer", "private": true, "type": "module" }
JSON
cat > consumer.ts <<'TS'
import { Client, NonRetriableError, serve, workflow, type Run, type Workflow } from "@ezgh/workflows"
import { decodeSecret, sign, verify, type Invocation } from "@ezgh/workflows/protocol"
const wf: Workflow<{ n: number }, number> = workflow("w", async ({ input, step }) => (await step.run("s", ({ signal }) => (signal.aborted ? 0 : (input as { n: number }).n))) as number)
export const handler: (req: Request) => Promise<Response> = serve({ workflows: [wf], signingSecrets: [undefined] })
export const key: Uint8Array = decodeSecret("whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA")
export const signed: Promise<string> = sign([key], new Date(), new Uint8Array(0))
export const checked: Promise<void> = verify("", [key], new Uint8Array(0), new Date())
export const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p" })
export const run: Promise<Run> = client.get("r")
export type I = Invocation
export const e = new NonRetriableError("x")
TS
npm install --silent --no-audit --no-fund "$tarball" >/dev/null
for v in "${@:-5.4.5 5.6.3}"; do
  for v1 in $v; do
    for mode in "bundler esnext" "node16 node16"; do
      set -- $mode
      echo "typescript $v1, moduleResolution $1"
      npx --yes -p "typescript@$v1" tsc --noEmit --strict --target es2022 --lib es2022,dom \
        --module "$2" --moduleResolution "$1" consumer.ts
    done
  done
done
echo "types-compat: ok"
