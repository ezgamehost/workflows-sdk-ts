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
console.log("node smoke: ok")
