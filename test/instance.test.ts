import { afterEach, describe, expect, test } from "bun:test"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Client, QueuesClient, WorkflowsApiError } from "../src/index.ts"
import { InstanceProvider, NoBotError, setProductNamePath } from "../src/instance.ts"

const KEY1 = "ezgh_" + "a".repeat(40)
const KEY2 = "ezgh_" + "b".repeat(40)

// A fake metadata service and API behind one fetch.
class Fake {
  puts = 0
  gets = 0
  token = ""
  keys = [KEY1]
  status = 0 // credentials answer, when not 0
  retryAfter = ""
  notFound = false
  ttlMs = 10 * 60_000
  clock = () => Date.now()
  refused = new Set<string>()
  apiAuth: string[] = []
  fetch = async (url: string, init?: RequestInit): Promise<Response> => {
    const u = new URL(url)
    const h = new Headers(init?.headers)
    if (u.pathname === "/ezgh/v1/token") {
      this.puts++
      this.token = `tok${this.puts}`
      return new Response(this.token)
    }
    if (u.pathname === "/ezgh/v1/credentials") {
      this.gets++
      if (h.get("EZGH-Metadata-Token") !== this.token) return Response.json({ error: { code: "invalid_token" } }, { status: 401 })
      if (this.status === 404) return Response.json({ error: { code: this.notFound ? "not_found" : "no_bot" } }, { status: 404 })
      if (this.status) return new Response(null, { status: this.status, headers: this.retryAfter ? { "Retry-After": this.retryAfter } : {} })
      const k = this.keys.length > 1 ? this.keys.shift()! : this.keys[0]!
      return Response.json({ apiKey: k, expiresAt: new Date(this.clock() + this.ttlMs).toISOString(), bot: "bot_abc" })
    }
    const auth = h.get("Authorization") ?? ""
    this.apiAuth.push(auth)
    if (this.refused.has(auth)) return Response.json({ error: { code: "unauthorized", message: "no" } }, { status: 401 })
    return Response.json({ id: "run_1" })
  }
}

let restore = () => {}
const onInstance = (yes: boolean) => {
  const dir = mkdtempSync(join(tmpdir(), "ezgh-"))
  const p = join(dir, "product_name")
  if (yes) writeFileSync(p, "EZGH Compute\n")
  restore = setProductNamePath(p)
}
afterEach(() => restore())

describe("instance credentials", () => {
  test("provider recovers: new token on 401, refetch only near expiry, keeps the key while metadata is down", async () => {
    const f = new Fake()
    f.keys = [KEY1, KEY2]
    let now = 1_000_000_000_000
    f.clock = () => now
    const p = new InstanceProvider(f.fetch, () => now)
    expect(await p.key()).toBe(KEY1)
    expect(await p.key()).toBe(KEY1)
    expect([f.puts, f.gets]).toEqual([1, 1])
    // hostd forgot the token: a refused key forces a refetch, which gets one new PUT.
    f.token = "forgotten"
    p.refused(KEY1)
    expect(await p.key()).toBe(KEY2)
    expect(f.puts).toBe(2)
    now += 4 * 60_000
    expect(await p.key()).toBe(KEY2)
    expect(f.gets).toBe(3) // not refetched at 4 minutes of a 10 minute key
    f.keys = [KEY1]
    now += 2 * 60_000 // within 5 minutes of expiry
    expect(await p.key()).toBe(KEY1)
    f.status = 503
    now += 6 * 60_000 // 12 min: KEY1 (fetched at 6) lives until 16
    expect(await p.key()).toBe(KEY1)
    now += 5 * 60_000
    await expect(p.key()).rejects.toThrow()
    f.status = 404
    now += 60_000
    await expect(p.key()).rejects.toBeInstanceOf(NoBotError)
  })

  test("rate limited: keeps the key and backs off per Retry-After", async () => {
    const f = new Fake()
    let now = 1_000_000_000_000
    f.clock = () => now
    const p = new InstanceProvider(f.fetch, () => now)
    expect(await p.key()).toBe(KEY1)
    f.status = 429
    f.retryAfter = "120"
    now += 6 * 60_000
    expect(await p.key()).toBe(KEY1)
    const n = f.gets
    now += 60_000 // inside Retry-After
    expect(await p.key()).toBe(KEY1)
    expect(f.gets).toBe(n)
  })

  test("404 not_found is transient; only no_bot means no Bot", async () => {
    const f = new Fake()
    let now = 1_000_000_000_000
    f.clock = () => now
    const p = new InstanceProvider(f.fetch, () => now)
    expect(await p.key()).toBe(KEY1)
    f.status = 404
    f.notFound = true
    now += 6 * 60_000
    expect(await p.key()).toBe(KEY1)
    now += 5 * 60_000 // expired
    const e = await p.key().catch((x) => x)
    expect(e).toBeInstanceOf(Error)
    expect(e).not.toBeInstanceOf(NoBotError)
  })

  test("an API 401 refetches the key once and retries once; a second 401 is returned", async () => {
    onInstance(true)
    const f = new Fake()
    f.keys = [KEY1, KEY2]
    const c = new Client({ organizationId: "o", projectId: "p", baseUrl: "http://api.test", fetch: f.fetch })
    await c.getApp("app_1")
    f.refused.add(`Bearer ${KEY1}`)
    await c.getApp("app_1")
    expect(f.apiAuth.slice(-2)).toEqual([`Bearer ${KEY1}`, `Bearer ${KEY2}`])
    f.refused.add(`Bearer ${KEY2}`)
    const n = f.apiAuth.length
    const e = await c.getApp("app_1").catch((x) => x)
    expect(e).toBeInstanceOf(WorkflowsApiError)
    expect(e.status).toBe(401)
    expect(f.apiAuth.length - n).toBe(2)
  })

  test("the queues client uses them too", async () => {
    onInstance(true)
    const f = new Fake()
    const orig = f.fetch
    f.fetch = async (u, i) => (new URL(u).pathname.endsWith("/messages") ? (f.apiAuth.push(new Headers(i?.headers).get("Authorization")!), Response.json({ messages: [{ messageId: "m" }] })) : orig(u, i))
    const q = new QueuesClient({ organizationId: "o", projectId: "p", fetch: f.fetch })
    await q.send("que_abc", [{ body: "hi" }])
    expect(f.apiAuth).toEqual([`Bearer ${KEY1}`])
  })

  test("an explicit API key wins and the metadata service is never asked", async () => {
    onInstance(true)
    const f = new Fake()
    const c = new Client({ apiKey: "ezgh_explicit", organizationId: "o", projectId: "p", baseUrl: "http://api.test", fetch: f.fetch })
    await c.getApp("app_1")
    expect(f.apiAuth).toEqual(["Bearer ezgh_explicit"])
    expect([f.puts, f.gets]).toEqual([0, 0])
  })

  test("off an instance, no API key is an error and nothing is probed", () => {
    onInstance(false)
    const f = new Fake()
    expect(() => new Client({ organizationId: "o", projectId: "p", fetch: f.fetch })).toThrow("needs an apiKey")
    expect(() => new QueuesClient({ organizationId: "o", projectId: "p", fetch: f.fetch })).toThrow("needs an apiKey")
    expect([f.puts, f.gets]).toEqual([0, 0])
  })
})
