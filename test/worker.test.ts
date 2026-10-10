import { describe, expect, test } from "bun:test"
import { CancelledError, WorkerAuthError, devQueue, worker, workflow } from "../src/index.ts"
import type { Invocation, InvocationResult, PollRequest } from "../src/protocol/index.ts"
import { invocation } from "./helpers.ts"

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
      this.pollAuth.push(auth)
      const status = this.pollStatus.shift()
      if (status !== undefined) return new Response(JSON.stringify({ error: { code: "x", message: "scripted" } }), { status, headers: { "Retry-After": "0" } })
      const next = this.queue.shift()
      if (!next) {
        // Hold briefly like a long-poll, unless the worker aborts.
        await new Promise<void>((r) => { const t = setTimeout(r, 30); init?.signal?.addEventListener("abort", () => { clearTimeout(t); r() }, { once: true }) })
        if (init?.signal?.aborted) throw new DOMException("aborted", "AbortError")
        return new Response(null, { status: 204 })
      }
      this.running += next.length
      this.maxRunning = Math.max(this.maxRunning, this.running)
      return Response.json({ invocations: next }, { headers: { Date: new Date().toUTCString() } })
    }
    const token = auth.replace(/^Bearer /, "")
    if (path === "/v1/invocations/checkpoint") { this.checkpoints.push({ token, body: JSON.parse(String(init!.body)) }); return new Response(null, { status: this.checkpointStatus }) }
    if (path === "/v1/invocations/heartbeat") {
      this.heartbeats.push(token)
      if (this.heartbeatStatus !== 200) return new Response(null, { status: this.heartbeatStatus })
      const cancel = this.heartbeats.length >= this.cancelAfterHeartbeats ? { requestedAt: new Date().toISOString(), graceUntil: new Date(Date.now() + 60_000).toISOString() } : null
      return Response.json({ cancel })
    }
    if (path === "/v1/invocations/complete") { this.completes.push({ token, result: JSON.parse(String(init!.body)) }); this.running--; return new Response(null, { status: 204 }) }
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
    expect(live.polls[0]).toEqual({ app: "0199a3c2-0000-7000-8000-00000000a001", queue: "jobs", maxTasks: 3, workerId: "w-1", protocolVersions: ["1.0", "1.1"],
      sdk: { name: "@ezgh/workflows", version: expect.any(String) }, workflows: [{ name: "pulled" }] })
    expect(live.checkpoints[0]!.token).toBe("tok-1")
    expect(live.completes[0]).toEqual({ token: "tok-1", result: { commands: [
      { type: "stepCompleted", stepId: expect.any(String), name: "compute", output: 4 }, { type: "runCompleted", output: 4 },
    ], deployment: "dep-7", sdk: { name: "@ezgh/workflows", version: expect.any(String) } } })
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

  test("stop() aborts the waiting poll, lets a running invocation finish within the grace period, then run() resolves", async () => {
    const live = new FakeLive()
    let finished = false
    const slow = workflow("slow", async ({ step }) => step.run("wait", async () => { await sleep(100); finished = true; return 1 }))
    live.queue.push([inv("slow", "tok-g", {})])
    const w = worker({ ...base(live), workflows: [slow] })
    const running = w.run()
    await until(() => live.checkpoints.length === 1)
    await w.stop({ graceMs: 2000 })
    await running
    expect(finished).toBe(true)
    expect(live.completes.length).toBe(1)
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
    expect(() => worker({ ...base(live), workflows: [pulled], apiKey: "" })).toThrow(TypeError)
  })
})
