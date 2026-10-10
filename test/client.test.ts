import { expect, test } from "bun:test"
import { Client, WorkflowsApiError } from "../src/client.ts"

function recorder(answer: () => Response) {
  const seen: { url: string; init: RequestInit }[] = []
  const f = (async (url: string, init: RequestInit) => { seen.push({ url, init }); return answer() }) as unknown as typeof fetch
  return { seen, f }
}

test("start posts to the project's runs with an API key and an Idempotency-Key", async () => {
  const { seen, f } = recorder(() => Response.json({ id: "r1", status: "running" }, { status: 201 }))
  const client = new Client({ apiKey: "ezgh_" + "a".repeat(40), organizationId: "o1", projectId: "p1", baseUrl: "https://api.test/", fetch: f })
  const run = await client.start("wfa_x", "hello", { a: 1 }, { runKey: "order-1" })
  expect(run).toEqual({ id: "r1", status: "running" } as never)
  expect(seen[0]!.url).toBe("https://api.test/v1/organizations/o1/projects/p1/runs")
  expect(seen[0]!.init.method).toBe("POST")
  const h = seen[0]!.init.headers as Record<string, string>
  expect(h.Authorization).toBe("Bearer ezgh_" + "a".repeat(40))
  expect(h["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/)
  expect(JSON.parse(String(seen[0]!.init.body))).toEqual({ appId: "wfa_x", workflow: "hello", input: { a: 1 }, runKey: "order-1" })
})

test("the default base URL is workflows-api.ezghcloud.com; get, cancel, retry, output and sync", async () => {
  const { seen, f } = recorder(() => Response.json({}))
  const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p", fetch: f })
  await client.get("r/1")
  await client.cancel("r1")
  await client.retry("r1")
  await client.output("r1")
  await client.sync("wfa_1", { endpoint: "https://app.example/api/workflows" })
  expect(seen.map((s) => [s.init.method, s.url])).toEqual([
    ["GET", "https://workflows-api.ezghcloud.com/v1/organizations/o/projects/p/runs/r%2F1"],
    ["POST", "https://workflows-api.ezghcloud.com/v1/organizations/o/projects/p/runs/r1/cancel"],
    ["POST", "https://workflows-api.ezghcloud.com/v1/organizations/o/projects/p/runs/r1/retry"],
    ["GET", "https://workflows-api.ezghcloud.com/v1/organizations/o/projects/p/runs/r1/payloads/output"],
    ["POST", "https://workflows-api.ezghcloud.com/v1/organizations/o/projects/p/apps/wfa_1/sync"],
  ])
  expect(JSON.parse(String(seen[4]!.init.body))).toEqual({ endpoint: "https://app.example/api/workflows" })
  expect((seen[0]!.init.headers as Record<string, string>)["Idempotency-Key"]).toBeUndefined()
})

test("errors carry the platform code", async () => {
  const client = new Client({ apiKey: "k", organizationId: "o", projectId: "p", baseUrl: "https://api.test",
    fetch: (async () => Response.json({ error: { code: "run_key_conflict", message: "Different input" } }, { status: 409 })) as never })
  const err = await client.start("a", "w").catch((e) => e)
  expect(err).toBeInstanceOf(WorkflowsApiError)
  expect([err.status, err.code, err.message]).toEqual([409, "run_key_conflict", "Different input"])
  const html = new Client({ apiKey: "k", organizationId: "o", projectId: "p", fetch: (async () => new Response("<html>", { status: 502 })) as never })
  const err2 = await html.get("r").catch((e) => e)
  expect([err2.status, err2.code]).toEqual([502, "unknown"])
})
