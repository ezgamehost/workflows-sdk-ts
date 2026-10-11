// Fixes from the final cross-SDK review: TypeScript aligned with Go and Python where they were right.
import { describe, expect, test } from "bun:test"
import { Client, WorkflowsApiError, completeCallbackURL, worker, workflow } from "../src/index.ts"
import { detectDeployment } from "../src/env.ts"
import type { Command, Invocation, JournalEntry } from "../src/protocol/index.ts"
import { validateCommand } from "../src/protocol/validate.ts"
import { NonRetriableError, RetryAfterError, WorkflowError } from "../src/runtime/errors.ts"
import { execute } from "../src/runtime/execution.ts"
import { FakeLive, invocation, sid } from "./helpers.ts"

const RUN = "0199a3d0-2b3c-7012-9f50-6b7c8d9e0f1a"
const v11 = (wf: string, entries: JournalEntry[] = [], extra: Partial<Invocation> = {}): Invocation =>
  invocation(wf, entries, {
    protocolVersion: "1.1", endpoints: { live: "https://live.test", api: "https://api.test" },
    run: { id: RUN, workflow: wf, input: {}, attempt: 1, startedAt: "2026-10-09T12:00:00Z" }, ...extra,
  })
const cmds = (xs: Command[]) => { xs.forEach(validateCommand); return JSON.parse(JSON.stringify(xs)) as Record<string, unknown>[] }
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

type Handler = (req: { method: string; path: string; headers: Headers; body: string; n: number }) => Response | Promise<Response>
function fakeApi(handler: Handler) {
  const seen: { method: string; path: string; key: string | null; redirect?: RequestRedirect }[] = []
  const f = async (url: string, init?: RequestInit) => {
    const u = new URL(url)
    const headers = new Headers(init?.headers)
    seen.push({ method: init?.method ?? "GET", path: u.pathname + u.search, key: headers.get("Idempotency-Key"), ...(init?.redirect ? { redirect: init.redirect } : {}) })
    if (init?.redirect !== "manual" && false) throw new Error("unreachable")
    return handler({ method: init?.method ?? "GET", path: u.pathname, headers, body: String(init?.body ?? ""), n: seen.length })
  }
  return { seen, f }
}
const client = (f: (u: string, i?: RequestInit) => Promise<Response>, extra = {}) =>
  new Client({ apiKey: "k", organizationId: "o", projectId: "p", baseUrl: "https://api.test", fetch: f, maxRetryDelayMs: 1, ...extra })
const pushApp = { id: "a1", kind: "push", name: "n" }

describe("1. secret-bearing calls never answer success without the secret", () => {
  test("a 503 on createApp or rotateSecret is outcome_unknown, never resent", async () => {
    for (const call of [(c: Client) => c.createApp("n"), (c: Client) => c.rotateSecret("a1")]) {
      const api = fakeApi(() => Response.json({ error: { code: "unavailable", message: "x" } }, { status: 503 }))
      const err = await call(client(api.f)).then(() => null, (e: unknown) => e)
      expect(err).toBeInstanceOf(WorkflowsApiError)
      expect((err as WorkflowsApiError).code).toBe("outcome_unknown")
      expect(api.seen.length).toBe(1)
    }
  })
  test("a 409 idempotency_in_progress on createApp is outcome_unknown too", async () => {
    const api = fakeApi(() => Response.json({ error: { code: "idempotency_in_progress", message: "x" } }, { status: 409, headers: { "Retry-After": "1" } }))
    expect(((await client(api.f).createApp("n").then(() => null, (e: unknown) => e)) as WorkflowsApiError).code).toBe("outcome_unknown")
  })
  test("a push app's success with a null secret (an Idempotency-Key replay) is outcome_unknown", async () => {
    const api = fakeApi(() => Response.json({ app: pushApp, signingSecret: null }, { status: 200 }))
    expect(((await client(api.f).createApp("n").then(() => null, (e: unknown) => e)) as WorkflowsApiError).code).toBe("outcome_unknown")
    expect(((await client(api.f).rotateSecret("a1").then(() => null, (e: unknown) => e)) as WorkflowsApiError).code).toBe("outcome_unknown")
  })
  test("a pull app has no secret: null is its success; a 429 (not processed) is retried", async () => {
    const api = fakeApi(({ n }) => n === 1 ? Response.json({ error: { code: "too_many_requests", message: "x" } }, { status: 429, headers: { "Retry-After": "0" } })
      : Response.json({ app: { id: "p1", kind: "pull", name: "jobs" }, signingSecret: null }, { status: 201 }))
    expect((await client(api.f).createApp("jobs", { kind: "pull" })).app.id).toBe("p1")
    expect(api.seen.length).toBe(2)
  })
})

describe("shared gap: 409 idempotency_in_progress is retried after its Retry-After", () => {
  test("start retries it with the same key", async () => {
    const api = fakeApi(({ n }) => n === 1 ? Response.json({ error: { code: "idempotency_in_progress", message: "x" } }, { status: 409, headers: { "Retry-After": "0" } })
      : Response.json({ id: "r1", status: "running" }, { status: 201 }))
    expect((await client(api.f).start("a", "w")).id).toBe("r1")
    expect(api.seen.map((s) => s.key)).toEqual([api.seen[0]!.key, api.seen[0]!.key])
  })
  test("other 409s aren't retried", async () => {
    const api = fakeApi(() => Response.json({ error: { code: "run_key_conflict", message: "x" } }, { status: 409 }))
    expect(((await client(api.f).start("a", "w").then(() => null, (e: unknown) => e)) as WorkflowsApiError).code).toBe("run_key_conflict")
    expect(api.seen.length).toBe(1)
  })
})

describe("6. redirects are errors, never followed", () => {
  test("the client asks for manual redirects and treats a 3xx as an error", async () => {
    for (const status of [301, 302, 307, 308]) {
      const api = fakeApi(() => new Response(null, { status, headers: { Location: "https://elsewhere.test/landing" } }))
      const err = await client(api.f).start("a", "w").then(() => null, (e: unknown) => e)
      expect([status, (err as WorkflowsApiError)?.status, (err as WorkflowsApiError)?.code]).toEqual([status, status, "redirect"])
      expect(api.seen.every((s) => s.redirect === "manual")).toBe(true)
      expect(api.seen.length).toBe(1)
    }
  })
  test("completeCallbackURL refuses a redirect too", async () => {
    let redirect: RequestRedirect | undefined
    const err = await completeCallbackURL("https://api.test/public/v1/callbacks/x", { ok: true }, { fetch: async (_u, i) => { redirect = i?.redirect; return new Response(null, { status: 302, headers: { Location: "/x" } }) } }).then(() => null, (e: unknown) => e)
    expect(redirect).toBe("manual")
    expect((err as WorkflowsApiError).status).toBe(302)
  })
  test("the worker's poll asks for manual redirects, and a 3xx is fatal", async () => {
    let redirect: RequestRedirect | undefined
    const w = worker({ apiKey: "k", organizationId: "o", projectId: "p", app: "0199a3c2-0000-7000-8000-00000000a001", liveUrl: "https://live.test",
      workflows: [workflow("w", async () => 1)], backoffMaxMs: 10,
      fetch: async (_u, i) => { redirect = i?.redirect; return new Response(null, { status: 301, headers: { Location: "https://x.test/" } }) } })
    const err = await w.run().then(() => null, (e: unknown) => e)
    expect(redirect).toBe("manual")
    expect((err as Error).name).toBe("WorkerConfigError")
  })
})

describe("12. completeCallbackURL", () => {
  test("POSTs the JSON value to the URL with no credential; 2xx is success, an error answer throws", async () => {
    const seen: { url: string; body: string; headers: Headers }[] = []
    await completeCallbackURL("https://api.test/public/v1/callbacks/012.tok", { approved: true }, { fetch: async (u, i) => { seen.push({ url: u, body: String(i?.body), headers: new Headers(i?.headers) }); return Response.json({ status: "completed" }) } })
    expect(seen[0]!.url).toBe("https://api.test/public/v1/callbacks/012.tok")
    expect(JSON.parse(seen[0]!.body)).toEqual({ approved: true })
    expect(seen[0]!.headers.get("Content-Type")).toBe("application/json")
    expect(seen[0]!.headers.get("Authorization")).toBeNull()
    const err = await completeCallbackURL("https://api.test/public/v1/callbacks/012.tok", 1, { fetch: async () => Response.json({ error: { code: "callback_completed", message: "x" } }, { status: 409 }) }).then(() => null, (e: unknown) => e)
    expect([(err as WorkflowsApiError).status, (err as WorkflowsApiError).code]).toEqual([409, "callback_completed"])
    expect(String((err as Error).message)).not.toContain("012.tok") // the token is a credential
  })
})

describe("14. client argument checks and the default timeout", () => {
  test("an empty Idempotency-Key is refused; limit is at least 1", async () => {
    const api = fakeApi(() => Response.json({ id: "r1" }))
    const c = client(api.f)
    expect(() => c.start("a", "w", undefined, { idempotencyKey: "" })).toThrow(TypeError)
    expect(() => c.events("r1", { limit: 0 })).toThrow(TypeError)
    expect(() => c.listSchedules({ limit: 0 })).toThrow(TypeError)
    expect(() => c.listApps({ limit: -1 })).toThrow(TypeError)
    expect(api.seen.length).toBe(0)
  })
  test("each request has a timeout (default 30 s, overridable): a hung request is retried, then fails", async () => {
    let calls = 0
    const f = (_u: string, i?: RequestInit) => new Promise<Response>((_r, rej) => { calls++; i?.signal?.addEventListener("abort", () => rej(i.signal!.reason), { once: true }) })
    const err = await client(f, { timeoutMs: 30, retries: 1 }).get("r1").then(() => null, (e: unknown) => e)
    expect(err).not.toBeNull()
    expect(calls).toBe(2)
    expect(() => client(f, { timeoutMs: 0 })).toThrow(TypeError)
  })
})

describe("2. a cancel signal mid-step: no new step starts; the invocation ends", () => {
  test("step two never starts after the heartbeat's cancel; step one's outcome is reported", async () => {
    const wf = workflow("seq2", async ({ step }) => {
      await step.run("one", () => new Promise((r) => setTimeout(() => r("x"), 150)))
      await step.run("two", () => "y")
      return "done"
    })
    const ac = new AbortController()
    setTimeout(() => ac.abort(), 30)
    const live = new FakeLive()
    const out = cmds(await execute(v11("seq2"), wf, live, { cancelSignal: ac.signal }))
    expect(live.checkpoints.map((c) => c.commands.map((x) => x.type))).toEqual([["stepStarted"]])
    expect(out).toEqual([{ type: "stepCompleted", stepId: await sid("one"), name: "one", output: "x" }])
  })
})

describe("7. a result past the transport's cap fails the run non-retriably", () => {
  const kids = workflow("kids", async ({ step }) => Promise.all(Array.from({ length: 10 }, (_, i) => step.invoke(`k${i}`, { workflow: "child", input: "x".repeat(900 * 1024) }))))
  test("ten 900 KiB invokes don't fit a pull result (8 MiB)", async () => {
    expect(cmds(await execute(v11("kids"), kids, new FakeLive(), { maxResultBytes: 8 << 20 }))).toEqual([
      { type: "runFailed", error: expect.objectContaining({ code: "workflows.non_retriable" }) },
    ])
  })
  test("they fit a push result (16 MiB)", async () => {
    const out = cmds(await execute(v11("kids"), kids, new FakeLive(), { maxResultBytes: 16 << 20 }))
    expect(out.map((c) => c.type)).toEqual(Array(10).fill("invoke"))
  })
})

describe("13. error classification walks the cause chain", () => {
  const failing = (e: unknown) => workflow("f", async ({ step }) => step.run("s", () => { throw e }))
  const failed = async (e: unknown) => (await execute(invocation("f"), failing(e), new FakeLive()))[0] as Extract<Command, { type: "stepFailed" }>
  test("a NonRetriableError cause makes the step non-retriable", async () => {
    expect((await failed(new Error("wrapper", { cause: new NonRetriableError("root") }))).retryable).toBe(false)
  })
  test("a RetryAfterError cause keeps its delay", async () => {
    expect((await failed(new Error("wrapper", { cause: new RetryAfterError("slow", 2500) }))).retryAfterMs).toBe(2500)
  })
  test("an error whose code is workflows.non_retriable is non-retriable", async () => {
    expect((await failed(new WorkflowError("x", { code: "workflows.non_retriable" }))).retryable).toBe(false)
    expect((await failed(new Error("plain"))).retryable).toBe(true)
  })
})

describe("15. deployment labels", () => {
  const withEnv = (env: Record<string, string | undefined>, fn: () => void) => {
    const saved: Record<string, string | undefined> = {}
    const keys = ["VERCEL_DEPLOYMENT_ID", "AWS_LAMBDA_FUNCTION_NAME", "AWS_LAMBDA_FUNCTION_VERSION", "K_REVISION", "FLY_MACHINE_VERSION", "RENDER_GIT_COMMIT"]
    for (const k of keys) { saved[k] = process.env[k]; delete process.env[k] }
    for (const [k, v] of Object.entries(env)) if (v !== undefined) process.env[k] = v
    try { fn() } finally { for (const k of keys) { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k] } }
  }
  test("Fly.io and Render are read; Lambda's $LATEST is skipped", () => {
    withEnv({ FLY_MACHINE_VERSION: "01HFLY" }, () => expect(detectDeployment()).toBe("01HFLY"))
    withEnv({ RENDER_GIT_COMMIT: "abc123" }, () => expect(detectDeployment()).toBe("abc123"))
    withEnv({ AWS_LAMBDA_FUNCTION_NAME: "fn", AWS_LAMBDA_FUNCTION_VERSION: "$LATEST" }, () => expect(detectDeployment()).toBeUndefined())
    withEnv({ AWS_LAMBDA_FUNCTION_NAME: "fn", AWS_LAMBDA_FUNCTION_VERSION: "7" }, () => expect(detectDeployment()).toBe("fn:7"))
  })
  test("a label is cut to 256 bytes of UTF-8 and loses NUL characters", () => {
    withEnv({ VERCEL_DEPLOYMENT_ID: "a\u0000b" + "é".repeat(200) }, () => {
      const d = detectDeployment()!
      expect(d.includes("\u0000")).toBe(false)
      expect(new TextEncoder().encode(d).length).toBeLessThanOrEqual(256)
      expect(d.startsWith("ab")).toBe(true)
    })
  })
})

describe("16. worker: stop() cancels what's left after the grace, and announcements are checked up front", () => {
  test("stop() aborts running steps after the grace period, then waits at most 5 s", async () => {
    let sawCancel = false
    const long = workflow("long", async ({ step }) => step.run("s", async ({ signal }) => {
      while (!signal.aborted) await sleep(5)
      sawCancel = true
      return "stopped"
    }))
    const inv = invocation("long", [], { protocolVersion: "1.1", token: "tok-s", endpoints: { live: "https://live.test" }, run: { id: RUN, workflow: "long", attempt: 1, startedAt: "2026-10-10T00:00:00Z" } })
    let delivered = false
    const completes: unknown[] = []
    const w = worker({ apiKey: "k", organizationId: "o", projectId: "p", app: "0199a3c2-0000-7000-8000-00000000a001", liveUrl: "https://live.test", workflows: [long], backoffMaxMs: 10, heartbeatIntervalMs: 1000,
      fetch: async (u, i) => {
        const path = new URL(u).pathname
        if (path.endsWith("/workers/poll")) {
          if (!delivered) { delivered = true; return Response.json({ invocations: [inv] }) }
          await new Promise<void>((r) => i?.signal?.addEventListener("abort", () => r(), { once: true }))
          throw new DOMException("aborted", "AbortError")
        }
        if (path === "/v1/invocations/complete") completes.push(JSON.parse(String(i?.body)))
        return path === "/v1/invocations/heartbeat" ? Response.json({ cancel: null }) : new Response(null, { status: 204 })
      } })
    const running = w.run()
    while (!delivered) await sleep(5)
    await sleep(30)
    const t0 = Date.now()
    await w.stop({ graceMs: 100 })
    await running
    expect(sawCancel).toBe(true)
    expect(Date.now() - t0).toBeLessThan(3000)
  })
  test("more than 1,000 workflows is refused at construction", () => {
    const many = Array.from({ length: 1001 }, (_, i) => workflow(`w${i}`, async () => i))
    expect(() => worker({ apiKey: "k", organizationId: "o", projectId: "p", app: "0199a3c2-0000-7000-8000-00000000a001", workflows: many })).toThrow(TypeError)
  })
})

describe("6. the live host's redirects fail the invocation", () => {
  test("a checkpoint answered 307 is CheckpointFailed, never followed", async () => {
    const { httpLive, CheckpointFailed } = await import("../src/runtime/live.ts")
    let redirect: RequestRedirect | undefined
    let calls = 0
    const io = httpLive("https://live.test", "tok", async (_u, i) => { calls++; redirect = i?.redirect; return new Response(null, { status: 307, headers: { Location: "https://elsewhere.test/" } }) })
    await expect(io.checkpoint({ invocationId: "inv_aaaaaaaaaaaaaaaaaaaaaaaaaa", seq: 1, commands: [] })).rejects.toBeInstanceOf(CheckpointFailed)
    expect([redirect, calls]).toEqual(["manual", 1])
  })
})
