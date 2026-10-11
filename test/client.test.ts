import { expect, test } from "bun:test"
import { Client, WorkflowsApiError } from "../src/client.ts"

function recorder(answer: (n: number) => Response) {
  const seen: { url: string; init: RequestInit }[] = []
  const f = async (url: string, init?: RequestInit) => { seen.push({ url, init: init ?? {} }); return answer(seen.length - 1) }
  return { seen, f }
}
const header = (init: RequestInit, name: string) => (init.headers as Record<string, string>)[name]

test("start posts to the project's runs with an API key and an Idempotency-Key", async () => {
  const { seen, f } = recorder(() => Response.json({ id: "r1", status: "running" }, { status: 201 }))
  const client = new Client({ apiKey: "ezgh_" + "a".repeat(40), organizationId: "o1", projectId: "p1", baseUrl: "https://api.test/", fetch: f })
  const run = await client.start("wfa_x", "hello", { a: 1 }, { runKey: "order-1" })
  expect(run).toEqual({ id: "r1", status: "running" } as never)
  expect(seen[0]!.url).toBe("https://api.test/v1/organizations/o1/projects/p1/runs")
  expect(seen[0]!.init.method).toBe("POST")
  expect(header(seen[0]!.init, "Authorization")).toBe("Bearer ezgh_" + "a".repeat(40))
  expect(header(seen[0]!.init, "Idempotency-Key")).toMatch(/^[0-9a-f-]{36}$/)
  expect(JSON.parse(String(seen[0]!.init.body))).toEqual({ appId: "wfa_x", workflow: "hello", input: { a: 1 }, runKey: "order-1" })
})

test("routes: the default base URL is workflows-api.ezghcloud.com", async () => {
  const { seen, f } = recorder(() => Response.json({}))
  const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p", fetch: f })
  await client.get("r/1")
  await client.cancel("r1", { gracePeriodSeconds: 0 })
  await client.cancel("r1")
  await client.retry("r1")
  await client.output("r1")
  await client.input("r1")
  await client.stepOutput("r1", "abc")
  await client.events("r1", { cursor: "c1", limit: 10 })
  await client.events("r1")
  await client.createApp("shop", { endpoint: "https://app.example/api/workflows" })
  await client.getApp("wfa_1")
  await client.sync("wfa_1", { endpoint: "https://app.example/api/workflows" })
  await client.rotateSecret("wfa_1")
  const root = "https://workflows-api.ezghcloud.com/v1/organizations/o/projects/p"
  expect(seen.map((s) => [s.init.method, s.url.slice(root.length), s.init.body === undefined ? undefined : JSON.parse(String(s.init.body))])).toEqual([
    ["GET", "/runs/r%2F1", undefined],
    ["POST", "/runs/r1/cancel", { gracePeriodSeconds: 0 }],
    ["POST", "/runs/r1/cancel", {}],
    ["POST", "/runs/r1/retry", {}],
    ["GET", "/runs/r1/payloads/output", undefined],
    ["GET", "/runs/r1/payloads/input", undefined],
    ["GET", "/runs/r1/steps/abc/output", undefined],
    ["GET", "/runs/r1/events?cursor=c1&limit=10", undefined],
    ["GET", "/runs/r1/events", undefined],
    ["POST", "/apps", { name: "shop", kind: "push", endpoint: "https://app.example/api/workflows" }],
    ["GET", "/apps/wfa_1", undefined],
    ["POST", "/apps/wfa_1/sync", { endpoint: "https://app.example/api/workflows" }],
    ["POST", "/apps/wfa_1/rotate-secret", {}],
  ])
  expect(header(seen[0]!.init, "Idempotency-Key")).toBeUndefined()
})

test("errors carry the platform code", async () => {
  const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p", baseUrl: "https://api.test",
    fetch: async () => Response.json({ error: { code: "run_key_conflict", message: "Different input" } }, { status: 409 }) })
  const err = await client.start("a", "w").catch((e) => e)
  expect(err).toBeInstanceOf(WorkflowsApiError)
  expect([err.status, err.code, err.message]).toEqual([409, "run_key_conflict", "Different input"])
  const html = new Client({ apiKey: "k", organizationId: "o", projectId: "p", retries: 0, fetch: async () => new Response("<html>", { status: 502 }) })
  const err2 = await html.get("r").catch((e) => e)
  expect([err2.status, err2.code]).toEqual([502, "unknown"])
})

test("503 and 429 are retried after Retry-After with the same Idempotency-Key, then surface", async () => {
  const { seen, f } = recorder((n) => n < 2
    ? Response.json({ error: { code: "unavailable", message: "warming up" } }, { status: n === 0 ? 503 : 429, headers: { "Retry-After": "1" } })
    : Response.json({ id: "r1" }, { status: 201 }))
  const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p", fetch: f, maxRetryDelayMs: 5 })
  expect(await client.start("a", "w")).toEqual({ id: "r1" } as never)
  expect(seen.length).toBe(3)
  expect(new Set(seen.map((s) => header(s.init, "Idempotency-Key"))).size).toBe(1)
  const down = new Client({ apiKey: "k", organizationId: "o", projectId: "p", retries: 2, maxRetryDelayMs: 1,
    fetch: async () => Response.json({ error: { code: "unavailable", message: "down" } }, { status: 503, headers: { "Retry-After": "5" } }) })
  const err = await down.get("r").catch((e) => e)
  expect([err.status, err.code, err.retryAfterSeconds]).toEqual([503, "unavailable", 5])
})

test("schedules: create, get, list, update with the revision, pause, resume, delete, trigger", async () => {
  const calls: { method: string; path: string; body?: unknown; headers: Record<string, string> }[] = []
  const sched = { id: "s1", resourceName: "r", appId: "a1", workflow: "w", name: "n", cron: "* * * * *", timeZone: "UTC", overlap: "skip",
    jitterSeconds: 0, catchupWindowSeconds: 3600, paused: false, revision: 3, nextFireAt: null, lastScheduledTime: null, activeRunId: null, missed: 0, skipped: 0, createdAt: "t", updatedAt: "t" }
  const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p", baseUrl: "https://api.test", fetch: async (url, init) => {
    const u = new URL(url)
    calls.push({ method: init!.method!, path: u.pathname + u.search, body: init!.body ? JSON.parse(String(init!.body)) : undefined, headers: init!.headers as Record<string, string> })
    if (init!.method === "DELETE") return new Response(null, { status: 204 })
    if (u.pathname.endsWith("/trigger")) return Response.json({ id: "r1", status: "running" }, { status: 201 })
    if (u.pathname.endsWith("/callbacks/step-1")) return Response.json({ id: "r1", status: "running" })
    return Response.json(u.pathname.endsWith("/schedules") && init!.method === "GET" ? { schedules: [sched], nextCursor: null } : sched)
  } })
  await client.createSchedule({ appId: "a1", workflow: "w", name: "n", cron: "* * * * *" })
  await client.getSchedule("s1")
  await client.listSchedules({ limit: 10 })
  await client.updateSchedule("s1", { cron: "0 * * * *" }, { revision: 3 })
  await client.pauseSchedule("s1"); await client.resumeSchedule("s1"); await client.deleteSchedule("s1")
  await client.triggerSchedule("s1", { idempotencyKey: "trig-1" })
  await client.completeCallback("r1", "step-1", { ok: true })
  const base = "/v1/organizations/o/projects/p"
  expect(calls.map((c) => `${c.method} ${c.path}`)).toEqual([
    `POST ${base}/schedules`, `GET ${base}/schedules/s1`, `GET ${base}/schedules?limit=10`, `PATCH ${base}/schedules/s1`,
    `POST ${base}/schedules/s1/pause`, `POST ${base}/schedules/s1/resume`, `DELETE ${base}/schedules/s1`,
    `POST ${base}/schedules/s1/trigger`, `POST ${base}/runs/r1/callbacks/step-1`,
  ])
  expect(calls[3]!.headers["If-Match"]).toBe('"3"')
  expect(calls[7]!.headers["Idempotency-Key"]).toBe("trig-1")
  expect(calls[8]!.body).toEqual({ ok: true })
})

test("createApp creates pull apps", async () => {
  let body: unknown
  const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p", fetch: async (_u, init) => { body = JSON.parse(String(init!.body)); return Response.json({ app: {}, signingSecret: null }, { status: 201 }) } })
  await client.createApp("jobs", { kind: "pull", queue: "jobs", partitions: 2 })
  expect(body).toEqual({ name: "jobs", kind: "pull", queue: "jobs", partitions: 2 })
})
