import { afterAll, beforeEach, describe, expect, test } from "bun:test"
import { createHash } from "node:crypto"
import { QueuesApiError, QueuesClient, workflow } from "../src/index.ts"
import type { Command, JournalEntry } from "../src/protocol/index.ts"
import { DEFAULT_RETRY } from "../src/protocol/index.ts"
import { execute } from "../src/runtime/execution.ts"
import { sendToQueueDigest } from "../src/queues.ts"
import { FakeLive, invocation, sid } from "./helpers.ts"

// A local fake queues-api: it records every request and answers from a script.
interface Seen { method: string; path: string; headers: Headers; body: unknown }
let seen: Seen[] = []
let answers: ((n: number) => Response)[] = []
const ok = (n: number) => Response.json({ messages: Array.from({ length: n }, (_, i) => ({ messageId: `0199a3d0-0000-7000-8000-00000000000${i}` })) })
const fake = Bun.serve({
  port: 0,
  async fetch(req) {
    const url = new URL(req.url)
    const text = await req.text()
    seen.push({ method: req.method, path: url.pathname, headers: req.headers, body: text ? JSON.parse(text) : undefined })
    const answer = answers[seen.length - 1] ?? answers.at(-1)
    return answer ? answer(seen.length - 1) : ok((JSON.parse(text) as { messages: unknown[] }).messages.length)
  },
})
afterAll(() => fake.stop(true))
beforeEach(() => { seen = []; answers = [] })

const BASE = `http://127.0.0.1:${fake.port}`
const KEY = "ezgh_" + "a".repeat(40)
const client = (extra: Partial<ConstructorParameters<typeof QueuesClient>[0]> = {}) =>
  new QueuesClient({ apiKey: KEY, organizationId: "org-1", projectId: "prj-1", baseUrl: BASE, maxRetryDelayMs: 20, ...extra })
const PATH = "/v1/organizations/org-1/projects/prj-1/queues/que_abc/messages"
const err = (status: number, code: string, retryAfter?: number) => () =>
  Response.json({ error: { code, message: `${code} here` } }, { status, headers: retryAfter === undefined ? {} : { "Retry-After": String(retryAfter) } })

describe("QueuesClient.send", () => {
  test("posts the messages to the queue's path with the API key and the Idempotency-Key", async () => {
    const out = await client().send("que_abc", [{ body: "hi", attributes: { orderId: "o-1" } }], { idempotencyKey: "k-1" })
    expect(out).toEqual({ messages: [{ messageId: "0199a3d0-0000-7000-8000-000000000000" }] })
    expect(seen).toHaveLength(1)
    expect(seen[0]!.method).toBe("POST")
    expect(seen[0]!.path).toBe(PATH)
    expect(seen[0]!.headers.get("authorization")).toBe(`Bearer ${KEY}`)
    expect(seen[0]!.headers.get("content-type")).toBe("application/json")
    expect(seen[0]!.headers.get("idempotency-key")).toBe("k-1")
    expect(seen[0]!.body).toEqual({ messages: [{ body: "hi", attributes: { orderId: "o-1" } }] })
  })

  test("without an idempotencyKey, a random one, the same on every attempt", async () => {
    answers = [err(503, "unavailable", 0), err(500, "internal_error"), () => ok(1)]
    await client().send("que_abc", [{ body: "x" }])
    expect(seen).toHaveLength(3)
    const keys = new Set(seen.map((s) => s.headers.get("idempotency-key")))
    expect(keys.size).toBe(1)
    expect([...keys][0]).toMatch(/^[0-9a-f-]{36}$/)
  })

  test("the default base URL is queues-api.ezghcloud.com, and path parts are escaped", async () => {
    const urls: string[] = []
    const c = new QueuesClient({ apiKey: "k", organizationId: "o/1", projectId: "p", fetch: async (u) => { urls.push(String(u)); return ok(1) } })
    await c.send("que_a b", [{ body: "x" }])
    expect(urls).toEqual(["https://queues-api.ezghcloud.com/v1/organizations/o%2F1/projects/p/queues/que_a%20b/messages"])
  })

  test("resends 429 and 5xx after Retry-After, with the same key", async () => {
    answers = [err(429, "too_many_requests", 1), err(502, "bad_gateway"), err(409, "idempotency_in_progress", 0), () => ok(1)]
    const t0 = Date.now()
    await client({ maxRetryDelayMs: 5_000 }).send("que_abc", [{ body: "x" }], { idempotencyKey: "same" })
    expect(Date.now() - t0).toBeGreaterThanOrEqual(950) // Retry-After: 1
    expect(seen.map((s) => s.headers.get("idempotency-key"))).toEqual(["same", "same", "same", "same"])
  })

  test("resends a network error", async () => {
    let n = 0
    const c = new QueuesClient({ apiKey: "k", organizationId: "o", projectId: "p", maxRetryDelayMs: 1,
      fetch: async () => { if (n++ === 0) throw new TypeError("fetch failed"); return ok(1) } })
    await c.send("que_abc", [{ body: "x" }])
    expect(n).toBe(2)
    const down = new QueuesClient({ apiKey: "k", organizationId: "o", projectId: "p", maxRetryDelayMs: 1, retries: 1, fetch: async () => { throw new TypeError("fetch failed") } })
    await expect(down.send("que_abc", [{ body: "x" }])).rejects.toMatchObject({ name: "QueuesApiError", status: 0, code: "network_error" })
  })

  test("errors carry the status, the platform code and Retry-After; refusals aren't resent", async () => {
    answers = [err(400, "invalid_request")]
    const bad = await client().send("que_abc", [{ body: "x" }]).catch((e) => e)
    expect(bad).toBeInstanceOf(QueuesApiError)
    expect(bad).toMatchObject({ status: 400, code: "invalid_request", message: "invalid_request here" })
    expect(seen).toHaveLength(1)

    seen = []
    answers = [err(429, "quota_exceeded")]
    await expect(client().send("que_abc", [{ body: "x" }])).rejects.toMatchObject({ status: 429, code: "quota_exceeded" })
    expect(seen).toHaveLength(1)

    seen = []
    answers = [err(429, "too_many_requests", 0)]
    await expect(client({ retries: 2 }).send("que_abc", [{ body: "x" }])).rejects.toMatchObject({ status: 429, code: "too_many_requests", retryAfterSeconds: 0 })
    expect(seen).toHaveLength(3)

    seen = []
    answers = [() => new Response("upstream", { status: 404 })]
    await expect(client().send("que_abc", [{ body: "x" }])).rejects.toMatchObject({ status: 404, code: "unknown" })

    answers = [() => new Response(null, { status: 302, headers: { Location: "https://elsewhere.test/" } })]
    await expect(client().send("que_abc", [{ body: "x" }])).rejects.toMatchObject({ status: 302, code: "redirect" })
  })

  test("refuses bad arguments before sending", async () => {
    expect(() => new QueuesClient({ apiKey: "", organizationId: "o", projectId: "p" })).toThrow(TypeError)
    await expect(client().send("que_abc", [])).rejects.toBeInstanceOf(TypeError)
    await expect(client().send("que_abc", Array.from({ length: 11 }, () => ({ body: "x" })))).rejects.toBeInstanceOf(TypeError)
    await expect(client().send("que_abc", [{ body: "x" }], { idempotencyKey: "has space" })).rejects.toBeInstanceOf(TypeError)
    expect(seen).toHaveLength(0)
  })

  test("the API key doesn't show when the client is printed or serialized", () => {
    const c = client()
    expect(JSON.stringify(c)).not.toContain(KEY)
    expect(Bun.inspect(c)).not.toContain(KEY)
  })
})

// The key derivation vector, the same in every EZGH Workflows SDK.
const RUN = "0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a"
const DIGEST = "ac73d9ecbcab73fa5c63cae08e17eac274aca628299cc68bd17498109cb268c0"

describe("step.sendToQueue", () => {
  test("key derivation: hex(SHA-256(runId:stepId))", async () => {
    expect(await sid("notify")).toBe("70d6d439c310423a9f3007a881987180")
    expect(await sendToQueueDigest(RUN, "70d6d439c310423a9f3007a881987180")).toBe(DIGEST)
    expect(createHash("sha256").update(`${RUN}:70d6d439c310423a9f3007a881987180`).digest("hex")).toBe(DIGEST)
  })

  const notify = workflow("notify", async ({ step }) => {
    const sent = await step.sendToQueue("notify", client(), "que_abc", [
      { body: "a", groupId: "acct-9" },
      { body: "b" },
      { body: "c", groupId: "acct-9", dedupId: "mine" },
    ])
    return sent.messages.length
  })

  test("sends once with the derived Idempotency-Key and FIFO dedupIds, and records the result", async () => {
    const live = new FakeLive()
    const commands = await execute(invocation("notify"), notify, live)
    expect(seen).toHaveLength(1)
    expect(seen[0]!.path).toBe(PATH)
    expect(seen[0]!.headers.get("idempotency-key")).toBe(`wfq_${DIGEST}`)
    expect(seen[0]!.body).toEqual({ messages: [
      { body: "a", groupId: "acct-9", dedupId: `${DIGEST}:0` },
      { body: "b" },
      { body: "c", groupId: "acct-9", dedupId: "mine" },
    ] })
    expect(live.commandsOf(0)).toEqual([{ type: "stepStarted", stepId: await sid("notify"), name: "notify", attempt: 1, retry: DEFAULT_RETRY }])
    expect(commands).toEqual([
      { type: "stepCompleted", stepId: await sid("notify"), name: "notify", output: (await ok(3).json()) as never },
      { type: "runCompleted", output: 3 },
    ])
  })

  test("memoized: a replay returns the recorded result without calling the server", async () => {
    const first = await execute(invocation("notify"), notify, new FakeLive())
    expect(seen).toHaveLength(1)
    const done = first[0] as Extract<Command, { type: "stepCompleted" }>
    const journal: JournalEntry[] = [{ seq: 1, stepId: done.stepId, name: "notify", kind: "run", status: "completed", attempt: 1, output: done.output }]
    const replay = await execute(invocation("notify", journal), notify, new FakeLive())
    expect(replay).toEqual([{ type: "runCompleted", output: 3 }])
    expect(seen).toHaveLength(1)
  })

  test("a retried attempt sends the same Idempotency-Key", async () => {
    const journal: JournalEntry[] = [{ seq: 1, stepId: await sid("notify"), name: "notify", kind: "run", status: "retrying", attempt: 1,
      error: { code: "user", name: "QueuesApiError", message: "unavailable" } }]
    const commands = await execute(invocation("notify", journal), notify, new FakeLive())
    expect(commands.at(-1)).toEqual({ type: "runCompleted", output: 3 })
    expect(seen[0]!.headers.get("idempotency-key")).toBe(`wfq_${DIGEST}`)
  })

  test("other runs and steps get other keys", async () => {
    const two = workflow("two", async ({ step }) => {
      await step.sendToQueue("n", client(), "que_abc", [{ body: "1" }])
      await step.sendToQueue("n", client(), "que_abc", [{ body: "2" }])
    })
    await execute(invocation("two"), two, new FakeLive())
    await execute(invocation("two", [], { run: { id: "0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1b", workflow: "two", input: null, attempt: 1, startedAt: "2026-10-09T12:00:00Z" } }), two, new FakeLive())
    expect(new Set(seen.map((s) => s.headers.get("idempotency-key"))).size).toBe(4)
    expect(seen[1]!.headers.get("idempotency-key")).toBe(`wfq_${await sendToQueueDigest(RUN, await sid("n", 1))}`)
  })

  test("a refusal fails the step without retries; an outage is retryable", async () => {
    answers = [err(403, "access_denied")]
    const refused = await execute(invocation("notify"), notify, new FakeLive())
    expect(refused[0]).toMatchObject({ type: "stepFailed", retryable: false, error: { code: "workflows.non_retriable", name: "NonRetriableError",
      cause: { code: "user", name: "QueuesApiError", data: { status: 403, code: "access_denied" } } } })

    answers = [err(503, "unavailable", 0)]
    const down = await execute(invocation("notify"), notify, new FakeLive())
    expect(down[0]).toMatchObject({ type: "stepFailed", retryable: true, error: { code: "user", name: "QueuesApiError", data: { status: 503, code: "unavailable" } } })
  })

  test("bad arguments fail the run before any step", async () => {
    const wf = workflow("bad", async ({ step }) => step.sendToQueue("n", client(), "que_abc", []))
    const commands = await execute(invocation("bad"), wf, new FakeLive())
    expect(commands).toMatchObject([{ type: "runFailed", error: { name: "TypeError" } }])
    expect(seen).toHaveLength(0)
  })
})
