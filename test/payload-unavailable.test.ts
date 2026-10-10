// A payload-store outage at the live host must not cost a step attempt: the SDK passes the live
// host's 503 payload_unavailable through to dispatch.
import { expect, test } from "bun:test"
import { decodeSecret, sign } from "../src/protocol/index.ts"
import { CheckpointFailed, httpLive } from "../src/runtime/live.ts"
import { workflow } from "../src/runtime/workflow.ts"
import { serve } from "../src/serve.ts"
import { invocation } from "./helpers.ts"

const cp = { invocationId: "inv_x", seq: 1, commands: [] }
const unavailable = (code: string) => () => Response.json({ error: { code, message: "down" } }, { status: 503 })

test("live: giving up after 503 payload_unavailable reports that code", async () => {
  const err = await httpLive("https://l", "t", async () => unavailable("payload_unavailable")(), { attempts: 2, retryDelayMs: 1 }).checkpoint(cp).catch((e) => e)
  expect(err).toBeInstanceOf(CheckpointFailed)
  expect(err.code).toBe("payload_unavailable")
  const other = await httpLive("https://l", "t", async () => unavailable("unavailable")(), { attempts: 2, retryDelayMs: 1 }).checkpoint(cp).catch((e) => e)
  expect(other.code).toBe("checkpoint_failed")
  // The last answer decides: payload_unavailable then a plain 503 is checkpoint_failed.
  let n = 0
  const mixed = await httpLive("https://l", "t", async () => (n++ === 0 ? unavailable("payload_unavailable")() : new Response(null, { status: 503 })), { attempts: 2, retryDelayMs: 1 }).checkpoint(cp).catch((e) => e)
  expect(mixed.code).toBe("checkpoint_failed")
  const refused = await httpLive("https://l", "t", async () => new Response(null, { status: 413 })).checkpoint(cp).catch((e) => e)
  expect(refused.code).toBe("checkpoint_failed")
})

test("serve: answers 503 {error:{code:payload_unavailable}} to dispatch, checkpoint_failed otherwise", async () => {
  const secret = "whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"
  for (const [code, want] of [["payload_unavailable", "payload_unavailable"], ["unavailable", "checkpoint_failed"]]) {
    const h = serve({ workflows: [workflow("w", async ({ step }) => step.run("s", () => 1))], signingSecrets: [secret], fetch: async () => unavailable(code!)() })
    // A 6.5 s deadline leaves 1.5 s of retries past the 5 s reserve.
    const raw = new TextEncoder().encode(JSON.stringify({ ...invocation("w"), token: "tok", deadline: new Date(Date.now() + 6500).toISOString() }))
    const res = await h(new Request("https://app.test/", { method: "POST", body: raw, headers: { "EZGH-Signature": await sign([decodeSecret(secret)], new Date(), raw) } }))
    const body = (await res.json()) as { code: string; error: { code: string; message: string } }
    expect([res.status, body.code, body.error.code, typeof body.error.message] as unknown[]).toEqual([503, want, want, "string"])
  }
})
