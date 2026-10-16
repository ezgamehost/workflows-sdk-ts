import { describe, expect, test } from "bun:test"
import { CancelledError, WorkerAuthError, devQueue, worker, workflow } from "../src/index.ts"
import type { Invocation, InvocationResult, PollRequest } from "../src/protocol/index.ts"
import { invocation } from "./helpers.ts"
import { validateCheckpoint, validateInvocationResult, validatePollRequest } from "../src/protocol/validate.ts"

const LIVE = "https://live.test"
const ORG = "0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70"
const PROJECT = "0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81"
const RUN = "0199a3d0-2b3c-7012-9f50-6b7c8d9e0f1a"
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

// FakeLive serves the worker's whole surface: polls, checkpoints, heartbeats and completes.
class FakeLive {
  polls: PollRequest[] = []
  pollAuth: string[] = []
  queue: Invocation[][] = []                  // each poll answer, in order; then 204s
  pollStatus: number[] = []                   // scripted poll statuses before answers
  retryAfter = "0"                            // their Retry-After
  holdMs = 30                                 // how long an empty poll is held
  pollAborts = 0                              // held polls the worker aborted
  slowBodyMs = 0                              // answers with tasks stream their body this late
  answered = false                            // a poll answer with tasks has begun
  pollTimes: number[] = []
  checkpoints: { token: string; body: unknown }[] = []
  checkpointStatus = 204
  heartbeats: string[] = []
  heartbeatStatus = 200
  cancelAfterHeartbeats = Infinity            // answer cancel once this many heartbeats arrived
  completes: { token: string; result: InvocationResult }[] = []
  running = 0
  maxRunning = 0
  fetch = async (url: string, init?: RequestInit): Promise<Response> => {
    const path = new URL(url).pathname
    const auth = (init?.headers as Record<string, string>)?.Authorization ?? ""
    if (path === `/v1/organizations/${ORG}/projects/${PROJECT}/workers/poll`) {
      this.polls.push(JSON.parse(String(init!.body)))
      validatePollRequest(this.polls.at(-1)) // what the gateway would accept
      this.pollTimes.push(Date.now())
      this.pollAuth.push(auth)
      const status = this.pollStatus.shift()
      if (status !== undefined) return new Response(JSON.stringify({ error: { code: "x", message: "scripted" } }), { status, headers: { "Retry-After": this.retryAfter } })
      const next = this.queue.shift()
      if (!next) {
        // Hold briefly like a long-poll, unless the worker aborts.
        await new Promise<void>((r) => { const t = setTimeout(r, this.holdMs); init?.signal?.addEventListener("abort", () => { clearTimeout(t); r() }, { once: true }) })
        if (init?.signal?.aborted) { this.pollAborts++; throw new DOMException("aborted", "AbortError") }
        return new Response(null, { status: 204 })
      }
      this.running += next.length
      this.maxRunning = Math.max(this.maxRunning, this.running)
      this.answered = true
      if (this.slowBodyMs > 0) {
        const text = JSON.stringify({ invocations: next }), delay = this.slowBodyMs
        const body = new ReadableStream<Uint8Array>({ async start(c) { await sleep(delay); c.enqueue(new TextEncoder().encode(text)); c.close() } })
        return new Response(body, { headers: { "Content-Type": "application/json", Date: new Date().toUTCString() } })
      }
      return Response.json({ invocations: next }, { headers: { Date: new Date().toUTCString() } })
    }
    const token = auth.replace(/^Bearer /, "")
    if (path === "/v1/invocations/checkpoint") { validateCheckpoint(JSON.parse(String(init!.body))); this.checkpoints.push({ token, body: JSON.parse(String(init!.body)) }); return new Response(null, { status: this.checkpointStatus }) }
    if (path === "/v1/invocations/heartbeat") {
      this.heartbeats.push(token)
      if (this.heartbeatStatus === 0) throw new TypeError("network down")
      if (this.heartbeatStatus !== 200) return new Response(null, { status: this.heartbeatStatus })
      const cancel = this.heartbeats.length >= this.cancelAfterHeartbeats ? { requestedAt: new Date().toISOString(), graceUntil: new Date(Date.now() + 60_000).toISOString() } : null
      return Response.json({ cancel })
    }
    if (path === "/v1/invocations/complete") { validateInvocationResult(JSON.parse(String(init!.body))); this.completes.push({ token, result: JSON.parse(String(init!.body)) }); this.running--; return new Response(null, { status: 204 }) }
    return new Response(null, { status: 404 })
  }
}

const inv = (wf: string, token: string, input: unknown = { x: 2 }, runId = RUN): Invocation =>
  invocation(wf, [], { protocolVersion: "1.1", token, endpoints: { live: LIVE, api: "https://api.test" },
    run: { id: runId, workflow: wf, input, attempt: 1, startedAt: "2026-10-10T00:00:00Z" } })
const pulled = workflow("pulled", async ({ input, step }) => step.run("compute", () => (input as { x: number }).x * 2))
const base = (live: FakeLive) => ({ apiKey: "ezgh_testkey", organizationId: ORG, projectId: PROJECT, app: "0199a3c2-0000-7000-8000-00000000a001", liveUrl: LIVE, fetch: live.fetch, backoffMaxMs: 10, heartbeatIntervalMs: 20 })

async function until(cond: () => boolean, ms = 3000) {
  const end = Date.now() + ms
  while (!cond() && Date.now() < end) await sleep(5)
  expect(cond()).toBe(true)
}

describe("pull worker", () => {
  test("polls with the API key, announces its workflows, runs the invocation and completes it", async () => {
    const live = new FakeLive()
    live.queue.push([inv("pulled", "tok-1")])
    const w = worker({ ...base(live), workflows: [pulled], queue: "jobs", maxTasks: 3, workerId: "w-1", deployment: "dep-7" })
    const running = w.run()
    await until(() => live.completes.length === 1)
    await w.stop()
    await running
    expect(live.pollAuth[0]).toBe("Bearer ezgh_testkey")
    expect(live.polls[0]).toEqual({ app: "0199a3c2-0000-7000-8000-00000000a001", queue: "jobs", maxTasks: 3, workerId: "w-1", protocolVersions: ["1.0", "1.1", "1.2"],
      sdk: { name: "@ezghcloud/workflows", version: expect.any(String) }, workflows: [{ name: "pulled" }] })
    expect(live.checkpoints[0]!.token).toBe("tok-1")
    expect(live.completes[0]).toEqual({ token: "tok-1", result: { commands: [
      { type: "stepCompleted", stepId: expect.any(String), name: "compute", output: 4 }, { type: "runCompleted", output: 4 },
    ], deployment: "dep-7", sdk: { name: "@ezghcloud/workflows", version: expect.any(String) } } })
  })

  test("ctx.traceparent is the pulled invocation's traceparent field, and \"\" without it", async () => {
    const live = new FakeLive()
    const tp = "00-0199a3d02b3c70129f506b7c8d9e0f1a-0123456789abcdef-01"
    live.queue.push([{ ...inv("traced", "tok-1"), traceparent: tp }, inv("traced", "tok-2", null, "0199a3d0-2b3c-7012-9f50-6b7c8d9e0f1b")])
    const traced = workflow("traced", async ({ traceparent }) => traceparent)
    const w = worker({ ...base(live), workflows: [traced], queue: "jobs", maxTasks: 2 })
    const running = w.run()
    await until(() => live.completes.length === 2)
    await w.stop()
    await running
    const out = (tok: string) => live.completes.find((c) => c.token === tok)!.result.commands
    expect(out("tok-1")).toEqual([{ type: "runCompleted", output: tp }])
    expect(out("tok-2")).toEqual([{ type: "runCompleted", output: "" }])
  })
  test("never runs more than concurrency invocations, and asks only for free slots", async () => {
    const live = new FakeLive()
    const slow = workflow("slow", async ({ step }) => step.run("wait", async () => { await sleep(80); return 1 }))
    live.queue.push([inv("slow", "a", {}, RUN), inv("slow", "b", {}, "0199a3d0-2b3c-7013-9f50-6b7c8d9e0f1a")], [inv("slow", "c", {}, "0199a3d0-2b3c-7014-9f50-6b7c8d9e0f1a")])
    const w = worker({ ...base(live), workflows: [slow], concurrency: 2, maxTasks: 4 })
    const running = w.run()
    await until(() => live.completes.length === 3, 5000)
    await w.stop()
    await running
    expect(live.polls[0]!.maxTasks).toBe(2)
    expect(live.maxRunning).toBeLessThanOrEqual(2)
  })

  test("a heartbeat that says cancelling aborts the running step's signal", async () => {
    const live = new FakeLive()
    live.cancelAfterHeartbeats = 2
    const long = workflow("longstep", async ({ step }) => step.run("long", async ({ signal }) => {
      const end = Date.now() + 3000
      while (!signal.aborted && Date.now() < end) await sleep(5)
      return signal.aborted ? "aborted" : "done"
    }))
    live.queue.push([inv("longstep", "tok-c", {})])
    const w = worker({ ...base(live), workflows: [long] })
    const running = w.run()
    await until(() => live.completes.length === 1)
    await w.stop()
    await running
    expect(live.heartbeats.length).toBeGreaterThanOrEqual(2)
    expect(live.completes[0]!.result.commands).toEqual([
      { type: "stepCompleted", stepId: expect.any(String), name: "long", output: "aborted" }, { type: "runCompleted", output: "aborted" },
    ])
  })

  test("a superseded checkpoint drops the invocation without completing it", async () => {
    const live = new FakeLive()
    live.checkpointStatus = 409
    live.queue.push([inv("pulled", "tok-s")])
    const w = worker({ ...base(live), workflows: [pulled] })
    const running = w.run()
    await until(() => live.checkpoints.length === 1)
    await sleep(50)
    await w.stop()
    await running
    expect(live.completes).toEqual([])
  })

  test("a heartbeat answered 409 (superseded) aborts the step and drops the result", async () => {
    const live = new FakeLive()
    live.heartbeatStatus = 409
    let reason: unknown
    const long = workflow("longstep", async ({ step }) => step.run("long", async ({ signal }) => {
      const end = Date.now() + 3000
      while (!signal.aborted && Date.now() < end) await sleep(5)
      reason = signal.reason
      return "aborted"
    }))
    live.queue.push([inv("longstep", "tok-h", {})])
    const w = worker({ ...base(live), workflows: [long] })
    const running = w.run()
    await until(() => reason !== undefined)
    await sleep(50)
    await w.stop()
    await running
    expect(reason).toBeInstanceOf(CancelledError)
    expect(live.completes).toEqual([])
  })

  test("an unknown workflow completes with workflowNotFound", async () => {
    const live = new FakeLive()
    live.queue.push([inv("nope", "tok-n")])
    const w = worker({ ...base(live), workflows: [pulled] })
    const running = w.run()
    await until(() => live.completes.length === 1)
    await w.stop()
    await running
    expect(live.completes[0]!.result.commands).toEqual([{ type: "workflowNotFound", workflow: "nope" }])
  })

  test("503 and 429 polls back off and retry; 401 rejects run() with WorkerAuthError", async () => {
    const live = new FakeLive()
    live.pollStatus.push(503, 429)
    live.queue.push([inv("pulled", "tok-r")])
    const errors: string[] = []
    const w = worker({ ...base(live), workflows: [pulled], onError: (_e, ctx) => errors.push(ctx) })
    const running = w.run()
    await until(() => live.completes.length === 1)
    await w.stop()
    await running
    expect(errors.filter((c) => c === "poll").length).toBe(2)

    const denied = new FakeLive()
    denied.pollStatus.push(401)
    await expect(worker({ ...base(denied), workflows: [pulled] }).run()).rejects.toBeInstanceOf(WorkerAuthError)
  })

  test("a poll answered 503 with Retry-After: 1 (tasks it couldn't start) waits a second, never hot-loops", async () => {
    const live = new FakeLive()
    live.pollStatus.push(503, 503)
    live.retryAfter = "1"
    live.queue.push([inv("pulled", "tok-503")])
    const w = worker({ ...base(live), backoffMaxMs: 30_000, workflows: [pulled] })
    const running = w.run()
    await until(() => live.completes.length === 1, 6000)
    await w.stop()
    await running
    expect(live.pollTimes[1]! - live.pollTimes[0]!).toBeGreaterThanOrEqual(950)
    expect(live.pollTimes[2]! - live.pollTimes[1]!).toBeGreaterThanOrEqual(950)
  }, 10_000)

  test("failed polls without Retry-After back off exponentially, with jitter", async () => {
    const live = new FakeLive()
    live.pollStatus.push(500, 502, 503)
    live.retryAfter = ""
    const w = worker({ ...base(live), backoffMaxMs: 30_000, workflows: [pulled] })
    const running = w.run()
    await until(() => live.polls.length >= 4, 8000)
    await w.stop()
    await running
    const gaps = live.pollTimes.slice(1, 4).map((t, i) => t - live.pollTimes[i]!)
    // 500 ms, 1 s and 2 s, each scaled by a jitter in [0.5, 1)
    expect(gaps[0]!).toBeGreaterThanOrEqual(240)
    expect(gaps[1]!).toBeGreaterThanOrEqual(490)
    expect(gaps[2]!).toBeGreaterThanOrEqual(990)
  }, 12_000)

  test("stop() aborts the waiting poll, lets a running invocation finish within the grace period, then run() resolves", async () => {
    const live = new FakeLive()
    let finished = false
    const slow = workflow("slow", async ({ step }) => step.run("wait", async () => { await sleep(100); finished = true; return 1 }))
    live.queue.push([inv("slow", "tok-g", {})])
    const w = worker({ ...base(live), workflows: [slow] })
    const running = w.run()
    await until(() => live.checkpoints.length === 1)
    live.holdMs = 60_000 // the next poll waits like a real long poll
    await until(() => live.polls.length >= 2)
    const t0 = Date.now()
    await w.stop({ graceMs: 2000 })
    await running
    expect(live.pollAborts).toBe(1) // the waiting poll was aborted, not waited out
    expect(Date.now() - t0).toBeLessThan(1500)
    expect(finished).toBe(true)
    expect(live.completes.length).toBe(1)
  })

  test("stop() leaves no timer behind, so an idle worker's process exits at once", async () => {
    const live = new FakeLive()
    live.holdMs = 60_000
    const pending = new Map<unknown, number>()
    const realSet = globalThis.setTimeout, realClear = globalThis.clearTimeout
    globalThis.setTimeout = ((fn: (...a: unknown[]) => void, ms?: number, ...a: unknown[]) => {
      const t: unknown = realSet(() => { pending.delete(t); fn(...a) }, ms)
      pending.set(t, ms ?? 0)
      return t
    }) as unknown as typeof setTimeout
    globalThis.clearTimeout = ((t: unknown) => { pending.delete(t); realClear(t as Parameters<typeof clearTimeout>[0]) }) as typeof clearTimeout
    try {
      const w = worker({ ...base(live), workflows: [pulled] })
      const running = w.run()
      await until(() => live.polls.length >= 1)
      await w.stop()
      await running
      expect([...pending.values()].filter((ms) => ms >= 1000)).toEqual([]) // the 30 s grace timer is cleared
    } finally {
      globalThis.setTimeout = realSet
      globalThis.clearTimeout = realClear
    }
  })

  test("a poll answer arriving as stop() begins is run within the grace period, not dropped", async () => {
    const live = new FakeLive()
    live.slowBodyMs = 150 // the answer's headers arrive, its body (the started tasks) a moment later
    live.queue.push([inv("pulled", "tok-late")])
    const w = worker({ ...base(live), workflows: [pulled] })
    const running = w.run()
    await until(() => live.answered)
    await w.stop({ graceMs: 3000 })
    await running
    expect(live.pollAborts).toBe(0)
    expect(live.completes.map((c) => c.token)).toEqual(["tok-late"])
  })

  test("a fatal poll error (401) stops polling, lets running invocations finish, then rejects run()", async () => {
    const live = new FakeLive()
    let finished = false
    const slow = workflow("slow", async ({ step }) => step.run("wait", async () => { await sleep(200); finished = true; return 1 }))
    live.queue.push([inv("slow", "tok-f", {})])
    const w = worker({ ...base(live), workflows: [slow], concurrency: 2 })
    const running = w.run().then(() => "resolved", (e: unknown) => e)
    await until(() => live.checkpoints.length === 1)
    live.pollStatus.push(401)
    const outcome = await running
    expect(outcome).toBeInstanceOf(WorkerAuthError)
    expect(finished).toBe(true)
    expect(live.completes.map((c) => c.token)).toEqual(["tok-f"])
    const polls = live.polls.length
    await sleep(100)
    expect(live.polls.length).toBe(polls) // no polling after the fatal error
  })

  test("heartbeat failures reach onError (a refused token, a network error)", async () => {
    for (const status of [401, 0]) {
      const live = new FakeLive()
      live.heartbeatStatus = status
      const errors: string[] = []
      const slow = workflow("slow", async ({ step }) => step.run("wait", async () => { await sleep(120); return 1 }))
      live.queue.push([inv("slow", `tok-hb${status}`, {})])
      const w = worker({ ...base(live), workflows: [slow], onError: (e, ctx) => errors.push(`${ctx}: ${(e as Error).message}`) })
      const running = w.run()
      await until(() => live.completes.length === 1)
      await w.stop()
      await running
      expect(errors.some((e) => e.startsWith("heartbeat") && (status === 0 ? e.includes("network down") : e.includes("401")))).toBe(true)
    }
  })

  test("dev mode polls dev-<user>", async () => {
    expect(devQueue("Alice Smith")).toBe("dev-alice-smith")
    expect(devQueue("first_last")).toBe("dev-first-last")
    expect(devQueue("")).toMatch(/^dev-[a-z0-9-]+$/)
    const live = new FakeLive()
    const w = worker({ ...base(live), workflows: [pulled], dev: { user: "bob" } })
    const running = w.run()
    await until(() => live.polls.length >= 1)
    await w.stop()
    await running
    expect(live.polls[0]!.queue).toBe("dev-bob")
  })

  test("options are checked", () => {
    const live = new FakeLive()
    expect(() => worker({ ...base(live), workflows: [] })).toThrow(TypeError)
    expect(() => worker({ ...base(live), workflows: [pulled], maxTasks: 11 })).toThrow(TypeError)
    expect(() => worker({ ...base(live), workflows: [pulled], queue: "bad queue" })).toThrow(TypeError)
    expect(() => worker({ ...base(live), workflows: [pulled], queue: "Upper" })).toThrow(TypeError)
    expect(() => worker({ ...base(live), workflows: [pulled], queue: "under_score" })).toThrow(TypeError)
    expect(() => worker({ ...base(live), workflows: [pulled], workerId: "has space" })).toThrow(TypeError)
    expect(() => worker({ ...base(live), workflows: [pulled], heartbeatIntervalMs: 60_000 })).toThrow(TypeError)
    expect(() => worker({ ...base(live), workflows: [pulled], heartbeatIntervalMs: 30_000 })).not.toThrow()
    expect(() => worker({ ...base(live), workflows: [pulled], apiKey: "" })).toThrow(TypeError)
  })
})
