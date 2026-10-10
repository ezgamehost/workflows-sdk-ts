// Fixes from the final SDK review: values Go can't decode or validate, explicit-id reuse, the
// public surface, and the smaller items.
import { describe, expect, test } from "bun:test"
import * as root from "../src/index.ts"
import * as publicProtocol from "../src/protocol/public.ts"
import { Client, WorkflowsApiError } from "../src/client.ts"
import { detectDeployment } from "../src/env.ts"
import { RetryAfterError, WorkflowError, encodeError } from "../src/runtime/errors.ts"
import { execute } from "../src/runtime/execution.ts"
import { httpLive } from "../src/runtime/live.ts"
import { workflow } from "../src/runtime/workflow.ts"
import { serve } from "../src/serve.ts"
import { FakeLive, invocation } from "./helpers.ts"

describe("values Go must decode and validate", () => {
  test("retryAfterMs is clamped to 0-86,400,000 and omitted when not a number", async () => {
    for (const [delay, want] of [[1e22, 86_400_000], [-5, 0], [Infinity, 86_400_000], [1500.7, 1500]] as const) {
      const wf = workflow("w", async ({ step }) => step.run("s", () => { throw new RetryAfterError("slow", delay) }))
      const [cmd] = await execute(invocation("w"), wf, new FakeLive())
      expect([delay, (cmd as { retryAfterMs?: number }).retryAfterMs]).toEqual([delay, want])
    }
    const wf = workflow("w", async ({ step }) => step.run("s", () => { throw new RetryAfterError("slow", NaN) }))
    expect((await execute(invocation("w"), wf, new FakeLive()))[0]).not.toHaveProperty("retryAfterMs")
  })
  test("timeoutMs is at most 900,000 (a step can't outlive an invocation)", async () => {
    expect(() => workflow("w", async () => 1, { timeoutMs: 900_001 })).toThrow(TypeError)
    expect(() => workflow("w", async () => 1, { timeoutMs: 2 ** 31 + 5 })).toThrow(TypeError)
    expect(() => workflow("w", async () => 1, { timeoutMs: 900_000 })).not.toThrow()
    const wf = workflow("w", async ({ step }) => step.run("s", () => 1, { timeoutMs: 3_000_000_000 }))
    expect(await execute(invocation("w"), wf, new FakeLive())).toEqual([{ type: "runFailed", error: expect.objectContaining({ name: "TypeError" }) }])
  })
  test("sleepUntil a past time sleeps until now, never a non-RFC 3339 year", async () => {
    const now = new Date("2026-10-10T12:00:00.000Z")
    for (const at of [new Date(-8.64e15), "0001-01-01T00:00:00Z", new Date("2026-10-10T11:00:00Z")]) {
      const wf = workflow("w", async ({ step }) => step.sleepUntil("s", at))
      const [cmd] = await execute(invocation("w"), wf, new FakeLive(), { now: () => now })
      expect(cmd).toMatchObject({ type: "sleep", until: "2026-10-10T12:00:00.000Z" })
    }
  })
})

describe("explicit ids", () => {
  test("reusing an explicit id, even sequentially, fails the run", async () => {
    const wf = workflow("w", async ({ step }) => { await step.run("a", () => 1, { id: "same" }); return step.run("b", () => 2, { id: "same" }) })
    const live = new FakeLive()
    const commands = await execute(invocation("w"), wf, live)
    expect(commands.at(-1)).toEqual({ type: "runFailed", error: expect.objectContaining({ code: "workflows.duplicate_step_name" }) })
    expect(live.checkpoints.length).toBe(1) // only "a" started
  })
})

describe("step names", () => {
  test("NUL is refused", async () => {
    const wf = workflow("w", async ({ step }) => step.run("a\u0000b", () => 1))
    expect(await execute(invocation("w"), wf, new FakeLive())).toEqual([{ type: "runFailed", error: expect.objectContaining({ name: "TypeError" }) }])
  })
})

describe("error codes", () => {
  test("only the platform's own workflows.* codes are kept", () => {
    expect(encodeError(new WorkflowError("x", { code: "workflows.callback_timeout" })).code).toBe("workflows.callback_timeout")
    expect(encodeError(new WorkflowError("x", { code: "workflows.made_up" }))).toMatchObject({ code: "user", name: "workflows.made_up", data: { code: "workflows.made_up" } })
  })
})

describe("serve", () => {
  test("empty secrets (an env var set but empty at build time) don't break the module load", () => {
    expect(() => serve({ workflows: [], signingSecrets: [""] })).not.toThrow()
  })
})

describe("live", () => {
  test("Retry-After 0 or a past date doesn't hot-loop", async () => {
    for (const ra of ["0", new Date(0).toUTCString()]) {
      let n = 0
      const f = async () => { n++; return new Response(null, { status: 503, headers: { "Retry-After": ra } }) }
      await httpLive("https://l", "t", f, { deadline: Date.now() + 1200, reserveMs: 0, retryDelayMs: 200 }).checkpoint({ invocationId: "inv_x", seq: 1, commands: [] }).catch(() => {})
      expect([ra, n <= 5]).toEqual([ra, true])
    }
  })
})

describe("public surface", () => {
  test("no root protocol namespace; ./protocol is sign, verify, decodeSecret, SignatureError", () => {
    expect("protocol" in root).toBe(false)
    expect(Object.keys(publicProtocol).sort()).toEqual(["SignatureError", "decodeSecret", "sign", "verify"])
  })
  test("a Workflow is opaque: only its name shows", () => {
    const wf = workflow("w", async () => 1, { retry: { maxAttempts: 2 } })
    expect(Object.keys(wf)).toEqual(["name"])
    expect(JSON.parse(JSON.stringify(wf))).toEqual({ name: "w" })
  })
})

describe("client", () => {
  test("createApp and rotateSecret don't resend after a network error: the secret may exist", async () => {
    for (const call of [(c: Client) => c.createApp("a"), (c: Client) => c.rotateSecret("wfa_1")]) {
      let n = 0
      const c = new Client({ apiKey: "k", organizationId: "o", projectId: "p", maxRetryDelayMs: 1, fetch: async () => { n++; throw new TypeError("fetch failed") } })
      const err = await call(c).catch((e) => e)
      expect(n).toBe(1)
      expect(err).toBeInstanceOf(WorkflowsApiError)
      expect(err.code).toBe("outcome_unknown")
      expect(err.message).toContain("rotate")
    }
  })
  test("other calls still retry network errors", async () => {
    let n = 0
    const c = new Client({ apiKey: "k", organizationId: "o", projectId: "p", maxRetryDelayMs: 1, fetch: async () => { if (n++ === 0) throw new TypeError("fetch failed"); return Response.json({ id: "r" }) } })
    expect(await c.get("r")).toEqual({ id: "r" } as never)
  })
  test("retries and maxRetryDelayMs are validated", () => {
    for (const bad of [{ retries: NaN }, { retries: -1 }, { retries: 1.5 }, { maxRetryDelayMs: NaN }, { maxRetryDelayMs: -1 }])
      expect(() => new Client({ apiKey: "k", organizationId: "o", projectId: "p", ...bad })).toThrow(TypeError)
  })
})

describe("env", () => {
  test("a process.env that throws (Deno without --allow-env) means no deployment", () => {
    const g = globalThis as { process?: unknown }
    const saved = g.process
    try {
      Object.defineProperty(g, "process", { configurable: true, value: { get env() { throw new Error("PermissionDenied") } } })
      expect(detectDeployment()).toBeUndefined()
    } finally { Object.defineProperty(g, "process", { configurable: true, writable: true, value: saved }) }
  })
})

describe("deadline", () => {
  test("the skew-free deadline is capped by the deadline as this clock reads it", async () => {
    const { decodeSecret: dec, sign: sg } = await import("../src/protocol/index.ts")
    const secret = "whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"
    let checkpoints = 0
    const h = serve({ workflows: [workflow("w", async ({ step }) => step.run("s", () => 1))], signingSecrets: [secret], fetch: async () => { checkpoints++; return new Response(null, { status: 204 }) } })
    const deadline = new Date(Date.now() + 3000) // 3 s left by this clock…
    const t = new Date(deadline.getTime() - 297_000) // …though t claims a 297 s budget
    const raw = new TextEncoder().encode(JSON.stringify({ ...invocation("w"), token: "tok", deadline: deadline.toISOString() }))
    const res = await h(new Request("https://app.test/", { method: "POST", body: raw, headers: { "EZGH-Signature": await sg([dec(secret)], t, raw) } }))
    expect([res.status, ((await res.json()) as { commands: unknown[] }).commands, checkpoints]).toEqual([200, [], 0])
  })
})
