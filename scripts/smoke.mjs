// Imports the built package under plain Node (no TypeScript, no bundler), so a missing file
// extension or a Bun-only API in dist fails here rather than for users.
import assert from "node:assert/strict"
import * as root from "../dist/index.js"
import * as protocol from "../dist/protocol/index.js"

assert.equal(root.SDK_NAME, "@ezgh/workflows")
assert.equal(await protocol.stepId("load-user", 0), "314ad9948a19a4844e0ea75b7de59ecd")
const key = protocol.decodeSecret("whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA")
const body = new TextEncoder().encode('{"a":1}')
const t = new Date(1791547200 * 1000)
assert.equal(await protocol.sign([key], t, body), "t=1791547200,v1=9d78a93e4b31945c5c8a6b811ab91c1225ffa28a8e900dca9a5f877803388bc2")
await protocol.verify(await protocol.sign([key], t, body), [key], body, t)
for (const name of ["workflow", "serve", "Client"]) if (name in root) assert.equal(typeof root[name], "function")
// serve() under Node: a signed sync, then an orchestrate invocation against a fake workflows-live.
const secret = "whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"
const hello = root.workflow("hello", async ({ step }) => step.run("greet", () => "hi"))
const handler = root.serve({ workflows: [hello], signingSecrets: [secret], fetch: async () => new Response(null, { status: 204 }) })
const invoke = async (inv) => {
  const raw = new TextEncoder().encode(JSON.stringify(inv))
  const sig = await protocol.sign([key], new Date(), raw)
  return handler(new Request("http://app.test/", { method: "POST", body: raw, headers: { "EZGH-Signature": sig } }))
}
const common = { protocolVersion: "1.0", invocationId: "inv_aaaaaaaaaaaaaaaaaaaaaaaaaa", token: "tok", endpoints: { live: "http://live.test" }, deadline: new Date(Date.now() + 60_000).toISOString(), cancel: null }
const sync = await invoke({ ...common, kind: "sync" })
assert.equal(sync.status, 200)
assert.deepEqual((await sync.json()).workflows, [{ name: "hello" }])
const orchestrated = await invoke({ ...common, kind: "orchestrate", run: { id: "01a12088-d200-73ff-8899-aabbccddeeff", workflow: "hello", attempt: 1, startedAt: "2026-10-09T12:00:00Z" }, journal: { complete: true, entries: [] } })
assert.equal(orchestrated.status, 200)
assert.deepEqual((await orchestrated.json()).commands.at(-1), { type: "runCompleted", output: "hi" })
console.log("node smoke: ok")
