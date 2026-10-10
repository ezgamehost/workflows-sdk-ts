// Fixes from the SDK chunk review: wire limits the engine enforces, signature cost, deadlines,
// live retries, and the smaller items.
import { describe, expect, test } from "bun:test"
import { inspect } from "node:util"
import { DEFAULT_RETRY, decodeSecret, parseSignature, sign, SignatureError, verify } from "../src/protocol/index.ts"
import { Client } from "../src/client.ts"
import { WorkflowError, encodeError } from "../src/runtime/errors.ts"
import { execute } from "../src/runtime/execution.ts"
import { CheckpointFailed, InvocationSuperseded, httpLive } from "../src/runtime/live.ts"
import { workflow } from "../src/runtime/workflow.ts"
import { serve } from "../src/serve.ts"
import { FakeLive, entry, invocation, sid } from "./helpers.ts"

const bytes = (s: string) => new TextEncoder().encode(s).length
const wellFormed = (s: string) => !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(s)

describe("error codes and sizes", () => {
  test("a code that isn't user or workflows.* goes on the wire as user, the code kept in name and data", () => {
    expect(encodeError(new WorkflowError("teapot", { code: "http.418", data: { status: 418 } }))).toMatchObject({ code: "user", name: "http.418", message: "teapot", data: { status: 418, code: "http.418" } })
    expect(encodeError(new WorkflowError("x", { code: "Bad Code", name: "HttpError" }))).toMatchObject({ code: "user", name: "HttpError", data: { code: "Bad Code" } })
    expect(encodeError(new WorkflowError("x", { code: "workflows.callback_timeout" }))).toMatchObject({ code: "workflows.callback_timeout", name: "WorkflowError" })
    expect(encodeError(new WorkflowError("x", { code: "user", data: [1] }))).toMatchObject({ code: "user", data: [1] })
  })
  test("stacks are cut to 8192 UTF-8 bytes on a code-point boundary", () => {
    for (const ch of ["é", "😀", "a", "€"]) {
      const e = new Error("wide")
      e.stack = ch.repeat(10_000)
      const w = encodeError(e)
      expect(bytes(w.stack!)).toBeLessThanOrEqual(8192)
      expect(bytes(w.stack!)).toBeGreaterThan(8192 - 4)
      expect(wellFormed(w.stack!)).toBe(true)
      expect(w.stack!.endsWith("�")).toBe(false)
    }
  })
  test("messages are cut to 8 KiB and data over 64 KiB is dropped", () => {
    const e = Object.assign(new Error("é".repeat(10_000)), { data: { blob: "x".repeat(70_000) } })
    const w = encodeError(e)
    expect(bytes(w.message)).toBeLessThanOrEqual(8192)
    expect(w.data).toBeUndefined()
    expect(encodeError(Object.assign(new Error("m"), { data: { small: 1 } })).data).toEqual({ small: 1 })
  })
})

describe("retry policies and names", () => {
  test("workflow() merges a partial retry with the defaults", () => {
    expect(workflow("w", async () => 1, { retry: { maxAttempts: 2 } }).opts.retry).toEqual({ ...DEFAULT_RETRY, maxAttempts: 2 })
  })
  test("retry bounds are enforced when the workflow is defined", () => {
    for (const retry of [{ maxAttempts: 0 }, { maxAttempts: 101 }, { maxAttempts: 1.5 }, { maxMs: 86_400_001 }, { multiplier: 11 }, { multiplier: 0.5 }, { initialMs: -1 }])
      expect(() => workflow("w", async () => 1, { retry })).toThrow(TypeError)
    expect(() => workflow("w", async () => 1, { retry: { maxAttempts: 100, maxMs: 86_400_000, multiplier: 10 } })).not.toThrow()
  })
  test("and when a step is called", async () => {
    const wf = workflow("w", async ({ step }) => step.run("s", () => 1, { retry: { multiplier: 20 } }))
    expect(await execute(invocation("w"), wf, new FakeLive())).toEqual([{ type: "runFailed", error: expect.objectContaining({ code: "user", name: "TypeError" }) }])
  })
  test("step names are 1-256 UTF-8 bytes", async () => {
    const live = new FakeLive()
    const wf = workflow("w", async ({ step }) => step.run("é".repeat(129), () => 1))
    expect(await execute(invocation("w"), wf, live)).toEqual([{ type: "runFailed", error: expect.objectContaining({ name: "TypeError" }) }])
    expect(live.checkpoints).toEqual([])
    const ok = workflow("w", async ({ step }) => step.run("é".repeat(128), () => 1))
    expect((await execute(invocation("w"), ok, new FakeLive())).at(-1)).toEqual({ type: "runCompleted", output: 1 })
  })
  test("two pending calls with one explicit id fail the run", async () => {
    const wf = workflow("w", async ({ step }) => Promise.all([step.run("a", () => 1, { id: "same" }), step.run("b", () => 2, { id: "same" })]))
    const live = new FakeLive()
    expect(await execute(invocation("w"), wf, live)).toEqual([{ type: "runFailed", error: expect.objectContaining({ code: "workflows.duplicate_step_name" }) }])
    expect(live.checkpoints).toEqual([])
  })
})

describe("signatures", () => {
  const key = decodeSecret("whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA")
  const body = new TextEncoder().encode('{"a":1}')
  const t = new Date(1_791_547_200_000)
  test("more than 8 signatures is invalid", async () => {
    const header = await sign([key], t, body)
    const many = header + ",v1=" + "0".repeat(64)
    await verify(many, [key], body, t)
    const tooMany = `t=1791547200,${Array(8).fill("v1=" + "0".repeat(64)).join(",")},${header.split(",")[1]}`
    expect((await verify(tooMany, [key], body, t).catch((e) => e)).reason).toBe("invalid")
  })
  test("the header and its timestamp are checked before the body", () => {
    expect(() => parseSignature("t=1791547200,v1=" + "0".repeat(64), new Date(1_791_547_200_000 + 301_000))).toThrow(SignatureError)
    expect(parseSignature("t=1791547200,v1=" + "0".repeat(64), t).t).toBe(1_791_547_200)
  })
  test("serve answers an expired signature without reading the body", async () => {
    let pulled = false
    const stream = new ReadableStream({ pull() { pulled = true } }, { highWaterMark: 0 })
    const h = serve({ workflows: [], signingSecrets: ["whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"] })
    const res = await h(new Request("https://app.test/", { method: "POST", body: stream, duplex: "half", headers: { "EZGH-Signature": "t=1000,v1=" + "0".repeat(64) } } as RequestInit))
    expect([res.status, pulled]).toEqual([401, false])
  })
})

describe("deadlines and live calls", () => {
  test("the deadline is local: receipt time plus (deadline - the signature's t)", async () => {
    const secret = "whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"
    let checkpoints = 0
    const h = serve({ workflows: [workflow("w", async ({ step }) => step.run("s", () => 1))], signingSecrets: [secret], fetch: async () => { checkpoints++; return new Response(null, { status: 204 }) } })
    const at = new Date(Date.now() + 250_000) // the engine's clock runs 250 s ahead
    const inv = { ...invocation("w"), token: "tok", deadline: new Date(at.getTime() + 3000).toISOString() }
    const raw = new TextEncoder().encode(JSON.stringify(inv))
    const res = await h(new Request("https://app.test/", { method: "POST", body: raw, headers: { "EZGH-Signature": await sign([decodeSecret(secret)], at, raw) } }))
    expect([res.status, ((await res.json()) as { commands: unknown[] }).commands, checkpoints]).toEqual([200, [], 0])
  })
  test("live 503s are retried after Retry-After within the time budget", async () => {
    let n = 0
    const f = async () => (n++ === 0 ? new Response(null, { status: 503, headers: { "Retry-After": "1" } }) : new Response(null, { status: 204 }))
    const started = Date.now()
    await httpLive("https://l", "t", f, { deadline: Date.now() + 20_000, reserveMs: 1000 }).checkpoint({ invocationId: "inv_x", seq: 1, commands: [] })
    expect(n).toBe(2)
    expect(Date.now() - started).toBeGreaterThanOrEqual(950)
  })
  test("they give up once the budget is spent", async () => {
    let n = 0
    const f = async () => { n++; return new Response(null, { status: 503, headers: { "Retry-After": "1" } }) }
    const started = Date.now()
    const err = await httpLive("https://l", "t", f, { deadline: Date.now() + 2500, reserveMs: 1000 }).checkpoint({ invocationId: "inv_x", seq: 1, commands: [] }).catch((e) => e)
    expect(err).toBeInstanceOf(CheckpointFailed)
    expect(Date.now() - started).toBeLessThan(2000)
    expect(n).toBeGreaterThanOrEqual(1)
  })
  test("a hung live call is aborted at the deadline", async () => {
    const f = (_u: string, init?: RequestInit) => new Promise<Response>((_, rej) => init?.signal?.addEventListener("abort", () => rej(init.signal!.reason)))
    const started = Date.now()
    const err = await httpLive("https://l", "t", f, { deadline: Date.now() + 300, reserveMs: 0 }).checkpoint({ invocationId: "inv_x", seq: 1, commands: [] }).catch((e) => e)
    expect(err).toBeInstanceOf(CheckpointFailed)
    expect(Date.now() - started).toBeLessThan(1500)
  })
  test("409 still stops at once", async () => {
    await expect(httpLive("https://l", "t", async () => new Response(null, { status: 409 }), { deadline: Date.now() + 10_000 }).checkpoint({ invocationId: "inv_x", seq: 1, commands: [] })).rejects.toBeInstanceOf(InvocationSuperseded)
  })
})

describe("nits", () => {
  test("sleeps longer than a year, and invalid times, fail the call", async () => {
    for (const body of [
      async (step: any) => step.sleep("s", 400 * 86_400_000),
      async (step: any) => step.sleep("s", "500d"),
      async (step: any) => step.sleepUntil("s", "not a time"),
      async (step: any) => step.sleepUntil("s", new Date(8.64e15)),
    ]) {
      const wf = workflow("w", async ({ step }) => body(step))
      expect(await execute(invocation("w"), wf, new FakeLive())).toEqual([{ type: "runFailed", error: expect.objectContaining({ name: "TypeError" }) }])
    }
  })
  test("unknown or waiting journal statuses are pending, not re-run", async () => {
    let ran = false
    const wf = workflow("w", async ({ step }) => step.run("s", () => { ran = true; return 1 }))
    const live = new FakeLive()
    expect(await execute(invocation("w", [await entry("s", 0, 1, { status: "waiting" })]), wf, live)).toEqual([])
    expect([ran, live.checkpoints]).toEqual([false, []])
  })
  test("a step gets an AbortSignal that fires on its timeout", async () => {
    let aborted = false
    const wf = workflow("w", async ({ step }) => step.run("s", ({ signal }) => new Promise((r) => { signal.addEventListener("abort", () => { aborted = true; r(1) }) }), { timeoutMs: 20 }))
    const commands = await execute(invocation("w"), wf, new FakeLive())
    expect(commands[0]).toMatchObject({ type: "stepFailed", error: { code: "workflows.step_timeout" } })
    expect(aborted).toBe(true)
  })
  test("serve decodes secrets lazily: an unset one doesn't break the build", async () => {
    const h = serve({ workflows: [], signingSecrets: [process.env.SURELY_UNSET_SECRET] })
    const res = await h(new Request("https://app.test/", { method: "POST", body: "{}", headers: { "EZGH-Signature": "t=1,v1=" + "0".repeat(64) } }))
    expect(res.status).toBe(500)
    expect(() => serve({ workflows: [], signingSecrets: ["whsec_bad"] })).toThrow(TypeError)
  })
  test("serve passes maxStepsPerInvocation through", async () => {
    const secret = "whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"
    const wf = workflow("w", async ({ step }) => { await step.run("a", () => 1); return step.run("b", () => 2) })
    const h = serve({ workflows: [wf], signingSecrets: [secret], maxStepsPerInvocation: 1, fetch: async () => new Response(null, { status: 204 }) })
    const raw = new TextEncoder().encode(JSON.stringify({ ...invocation("w"), token: "tok" }))
    const res = await h(new Request("https://app.test/", { method: "POST", body: raw, headers: { "EZGH-Signature": await sign([decodeSecret(secret)], new Date(), raw) } }))
    expect(((await res.json()) as { commands: unknown[] }).commands).toEqual([{ type: "stepCompleted", stepId: await sid("a"), name: "a", output: 1 }])
  })
  test("the Client's API key isn't visible", () => {
    const key = "ezgh_" + "s".repeat(40)
    const client = new Client({ apiKey: key, organizationId: "o", projectId: "p" })
    expect(JSON.stringify(client)).not.toContain(key)
    expect(inspect(client, { depth: 5, showHidden: true })).not.toContain(key)
  })
  test("recorded JSON replays with its key order and NUL characters", async () => {
    const recorded = JSON.parse('{"b":1,"a":"x\\u0000y","c":{"z":[2,1],"y":null}}') as unknown
    const wf = workflow("w", async ({ step }) => step.run("s", () => ({})))
    const commands = await execute(invocation("w", [await entry("s", 0, 1, { output: recorded })]), wf, new FakeLive())
    expect(JSON.stringify(commands)).toBe('[{"type":"runCompleted","output":{"b":1,"a":"x\\u0000y","c":{"z":[2,1],"y":null}}}]')
  })
})
