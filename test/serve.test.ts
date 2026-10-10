import { describe, expect, test } from "bun:test"
import { decodeSecret, sign } from "../src/protocol/index.ts"
import { serve } from "../src/serve.ts"
import { workflow } from "../src/runtime/workflow.ts"

const SECRET = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
const hello = workflow("hello", async ({ step }) => step.run("greet", () => "hi"))
const checkpoints: unknown[] = []
const liveFetch: typeof fetch = (async (url: string | URL | Request, init?: RequestInit) => {
  checkpoints.push({ url: String(url), auth: (init?.headers as Record<string, string>).Authorization, body: JSON.parse(String(init?.body)) })
  return new Response(null, { status: 204 })
}) as typeof fetch
const handler = serve({ workflows: [hello], signingSecrets: [SECRET], maxInvocationSeconds: 120, deployment: "dep-1", fetch: liveFetch })

async function post(body: unknown, opts: { secret?: string; at?: Date; h?: (req: Request) => Promise<Response> } = {}) {
  const raw = new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body))
  const sig = await sign([decodeSecret(opts.secret ?? SECRET)], opts.at ?? new Date(), raw)
  return (opts.h ?? handler)(new Request("https://app.test/api/workflows", { method: "POST", body: raw, headers: { "EZGH-Signature": sig, "Content-Type": "application/json" } }))
}
const base = { protocolVersion: "1.0", invocationId: "inv_aaaaaaaaaaaaaaaaaaaaaaaaaa", token: "tok", endpoints: { live: "https://live.test" }, deadline: new Date(Date.now() + 120_000).toISOString(), cancel: null }
const run = { id: "0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a", workflow: "hello", attempt: 1, startedAt: "2026-10-09T12:00:00Z" }

describe("serve", () => {
  test("sync answers the app's workflows", async () => {
    const res = await post({ ...base, kind: "sync" })
    expect(res.status).toBe(200)
    expect(res.headers.get("Content-Type")).toBe("application/json")
    expect(await res.json()).toEqual({ protocolVersions: ["1.0"], sdk: { name: "@ezgh/workflows", version: "0.1.0" }, maxInvocationSeconds: 120, workflows: [{ name: "hello" }], deployment: "dep-1" })
  })
  test("sync reports workflow defaults", async () => {
    const wf = workflow("tuned", async () => 1, { retry: { maxAttempts: 2, initialMs: 5, maxMs: 10, multiplier: 2 }, timeoutMs: 1000 })
    const res = await post({ ...base, kind: "sync" }, { h: serve({ workflows: [wf], signingSecrets: [SECRET] }) })
    expect(((await res.json()) as { workflows: unknown[]; maxInvocationSeconds: number })).toMatchObject({ maxInvocationSeconds: 300, workflows: [{ name: "tuned", retry: { maxAttempts: 2, initialMs: 5, maxMs: 10, multiplier: 2 }, timeoutMs: 1000 }] })
  })
  test("an orchestrate invocation runs and checkpoints to endpoints.live with the token", async () => {
    checkpoints.length = 0
    const res = await post({ ...base, kind: "orchestrate", run, journal: { complete: true, entries: [] } })
    expect(res.status).toBe(200)
    const body = (await res.json()) as { commands: { type: string }[]; sdk: { name: string; version: string }; deployment: string }
    expect(body.commands.map((c) => c.type)).toEqual(["stepCompleted", "runCompleted"])
    expect(body.deployment).toBe("dep-1")
    expect(body.sdk).toEqual({ name: "@ezgh/workflows", version: "0.1.0" })
    expect(checkpoints).toEqual([{ url: "https://live.test/v1/invocations/checkpoint", auth: "Bearer tok", body: expect.objectContaining({ seq: 1 }) }])
  })
  test("an unknown workflow is workflowNotFound", async () => {
    const res = await post({ ...base, kind: "orchestrate", run: { ...run, workflow: "gone" }, journal: { complete: true, entries: [] } })
    expect(((await res.json()) as { commands: unknown[] }).commands).toEqual([{ type: "workflowNotFound", workflow: "gone" }])
  })
  test("bad, missing and expired signatures are 401", async () => {
    const bad = await post({ ...base, kind: "sync" }, { secret: "whsec_Hx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQA" })
    expect([bad.status, await bad.json()]).toEqual([401, { code: "invalid_signature" }])
    const old = await post({ ...base, kind: "sync" }, { at: new Date(Date.now() - 600_000) })
    expect([old.status, await old.json()]).toEqual([401, { code: "expired_signature" }])
    const none = await handler(new Request("https://app.test/", { method: "POST", body: "{}" }))
    expect(none.status).toBe(401)
  })
  test("a rotation: either active secret verifies", async () => {
    const other = "whsec_Hx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQA"
    const h = serve({ workflows: [hello], signingSecrets: [SECRET, other] })
    expect((await post({ ...base, kind: "sync" }, { secret: other, h })).status).toBe(200)
  })
  test("non-POST is 405, bad JSON and an unsupported major version are 400", async () => {
    expect((await handler(new Request("https://app.test/", { method: "GET" }))).status).toBe(405)
    const v2 = await post({ ...base, protocolVersion: "2.0", kind: "sync" })
    expect(v2.status).toBe(400)
    expect(await v2.json()).toMatchObject({ code: "invalid_invocation" })
    expect((await post("not json")).status).toBe(400)
  })
  test("bodies over 8 MiB are 413", async () => {
    const big = new Uint8Array(8 * 1024 * 1024 + 1)
    expect((await handler(new Request("https://app.test/", { method: "POST", body: big }))).status).toBe(413)
    const stream = new ReadableStream({ start(c) { for (let i = 0; i < 9; i++) c.enqueue(new Uint8Array(1024 * 1024)); c.close() } })
    expect((await handler(new Request("https://app.test/", { method: "POST", body: stream, duplex: "half" } as RequestInit))).status).toBe(413)
  })
  test("a superseded checkpoint is 409, an unavailable live host 503", async () => {
    for (const [status, want, code] of [[409, 409, "invocation_superseded"], [400, 503, "checkpoint_failed"]] as const) {
      const h = serve({ workflows: [hello], signingSecrets: [SECRET], fetch: (async () => new Response("{}", { status })) as unknown as typeof fetch })
      const res = await post({ ...base, kind: "orchestrate", run, journal: { complete: true, entries: [] } }, { h })
      expect([res.status, await res.json()]).toEqual([want, { code }])
    }
  })
  test("an SDK failure is 500 sdk_error", async () => {
    const h = serve({ workflows: [hello], signingSecrets: [SECRET], fetch: (async () => { throw new Error("x") }) as unknown as typeof fetch })
    const res = await post({ ...base, kind: "orchestrate", run, journal: { complete: false, entries: [] } }, { h })
    expect(res.status).toBe(503) // journal paging failed after retries: checkpoint_failed
    const broken = serve({ workflows: [hello], signingSecrets: [SECRET], fetch: (async () => Response.json("not a journal")) as unknown as typeof fetch })
    const res2 = await post({ ...base, kind: "orchestrate", run, journal: { complete: false, entries: [] } }, { h: broken })
    expect([res2.status, await res2.json()]).toEqual([500, { code: "sdk_error" }])
  })
  test("configuration errors are refused at serve()", () => {
    expect(() => serve({ workflows: [hello, hello], signingSecrets: [SECRET] })).toThrow(TypeError)
    expect(() => serve({ workflows: [hello], signingSecrets: [] })).toThrow(TypeError)
    expect(() => serve({ workflows: [hello], signingSecrets: ["nope"] })).toThrow(TypeError)
    expect(() => serve({ workflows: [hello], signingSecrets: [SECRET], maxInvocationSeconds: 901 })).toThrow(TypeError)
  })
})
