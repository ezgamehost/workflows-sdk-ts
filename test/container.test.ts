import { afterEach, describe, expect, test } from "bun:test"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Client, WorkflowsApiError } from "../src/index.ts"
import { ContainerProvider, checkContainerUri } from "../src/container.ts"
import { setMetadataForTests, setProductNamePath } from "../src/instance.ts"

const POD_KEY = "ezgh_" + "p".repeat(40)
const KEY1 = "ezgh_" + "a".repeat(40)
const KEY2 = "ezgh_" + "b".repeat(40)

// A fake pod identity agent (/v1/credentials) and API on one local server.
class Agent {
  gets = 0
  tokens: string[] = [] // the bearer tokens the agent saw
  keys = [POD_KEY] // handed out in order, the last repeated
  status = 0
  retryAfter = ""
  clock = () => Date.now()
  refused = new Set<string>()
  apiAuth: string[] = []
  srv = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (req) => {
    const auth = req.headers.get("Authorization") ?? ""
    if (new URL(req.url).pathname !== "/v1/credentials") {
      this.apiAuth.push(auth)
      if (this.refused.has(auth)) return Response.json({ error: { code: "unauthorized", message: "no" } }, { status: 401 })
      return Response.json({ id: "app_1" })
    }
    this.gets++
    this.tokens.push(auth.replace(/^Bearer /, ""))
    if (this.status) return new Response(null, { status: this.status, headers: this.retryAfter ? { "Retry-After": this.retryAfter } : {} })
    const k = this.keys.length > 1 ? this.keys.shift()! : this.keys[0]!
    return Response.json({ apiKey: k, expiresAt: new Date(this.clock() + 10 * 60_000).toISOString(), bot: "bot_pod" })
  } })
  get url() { return `http://127.0.0.1:${this.srv.port}` }
  get uri() { return `${this.url}/v1/credentials` }
}

const cleanup: (() => void)[] = []
afterEach(() => { while (cleanup.length) cleanup.pop()!() })

function agent(): Agent {
  const a = new Agent()
  cleanup.push(() => a.srv.stop(true))
  return a
}
function tokenFile(token: string): string {
  const p = join(mkdtempSync(join(tmpdir(), "ezgh-")), "token")
  writeFileSync(p, token + "\n")
  return p
}
// inPod sets the variables EZGH Kubernetes injects.
function inPod(uri: string, file: string) {
  process.env.EZGH_CONTAINER_CREDENTIALS_FULL_URI = uri
  process.env.EZGH_CONTAINER_AUTHORIZATION_TOKEN_FILE = file
  cleanup.push(() => { delete process.env.EZGH_CONTAINER_CREDENTIALS_FULL_URI; delete process.env.EZGH_CONTAINER_AUTHORIZATION_TOKEN_FILE })
}
// onInstance makes this an EZGH instance whose metadata service counts the requests it gets.
function onInstance(): { hits: number } {
  const m = { hits: 0 }
  const dir = mkdtempSync(join(tmpdir(), "ezgh-"))
  writeFileSync(join(dir, "product_name"), "EZGH Compute\n")
  cleanup.push(setProductNamePath(join(dir, "product_name")))
  cleanup.push(setMetadataForTests({ fetch: async () => { m.hits++; return new Response("tok") } }))
  return m
}

describe("container credentials", () => {
  test("container credentials win over the instance", async () => {
    const meta = onInstance()
    const a = agent()
    inPod(a.uri, tokenFile("sa"))
    await new Client({ organizationId: "o", projectId: "p", baseUrl: a.url }).getApp("app_1")
    expect({ api: a.apiAuth, tokens: a.tokens, meta: meta.hits }).toEqual({ api: [`Bearer ${POD_KEY}`], tokens: ["sa"], meta: 0 })
  })

  test("an explicit key wins over the container", async () => {
    const a = agent()
    inPod(a.uri, tokenFile("sa"))
    await new Client({ apiKey: "ezgh_explicit", organizationId: "o", projectId: "p", baseUrl: a.url }).getApp("app_1")
    expect({ api: a.apiAuth, gets: a.gets }).toEqual({ api: ["Bearer ezgh_explicit"], gets: 0 })
  })

  test("refreshes 5 minutes before expiry", async () => {
    const a = agent()
    a.keys = [KEY1, KEY2]
    let now = Date.now()
    a.clock = () => now
    const p = new ContainerProvider(a.uri, tokenFile("sa"), () => now)
    expect(await p.key()).toBe(KEY1)
    now += 4 * 60_000
    expect(await p.key()).toBe(KEY1)
    expect(a.gets).toBe(1) // not refetched 4 minutes into a 10 minute key
    now += 2 * 60_000 // within 5 minutes of expiry
    expect(await p.key()).toBe(KEY2)
    expect(a.gets).toBe(2)
  })

  test("a failed fetch keeps the key until expiry", async () => {
    const a = agent()
    let now = Date.now()
    a.clock = () => now
    const p = new ContainerProvider(a.uri, tokenFile("sa"), () => now)
    expect(await p.key()).toBe(POD_KEY)
    a.status = 503
    a.retryAfter = "120"
    now += 6 * 60_000
    expect(await p.key()).toBe(POD_KEY)
    const n = a.gets
    now += 60_000 // inside Retry-After: nothing is asked
    expect(await p.key()).toBe(POD_KEY)
    expect(a.gets).toBe(n)
    now += 4 * 60_000 // expired
    await expect(p.key()).rejects.toThrow("answered 503")
  })

  test("a 401 refetches once", async () => {
    const a = agent()
    a.keys = [KEY1, KEY2]
    inPod(a.uri, tokenFile("sa"))
    const c = new Client({ organizationId: "o", projectId: "p", baseUrl: a.url })
    await c.getApp("app_1")
    a.refused.add(`Bearer ${KEY1}`)
    await c.getApp("app_1")
    expect(a.apiAuth.slice(-2)).toEqual([`Bearer ${KEY1}`, `Bearer ${KEY2}`])
    a.refused.add(`Bearer ${KEY2}`)
    const n = a.apiAuth.length
    const e = await c.getApp("app_1").catch((x) => x)
    expect(e).toBeInstanceOf(WorkflowsApiError)
    expect(e.status).toBe(401)
    expect(a.apiAuth.length - n).toBe(2)
  })

  test("a rotated token file is read on the next fetch", async () => {
    const a = agent()
    const file = tokenFile("sa-1")
    const p = new ContainerProvider(a.uri, file)
    await p.key()
    writeFileSync(file, "sa-2\n")
    p.refused(POD_KEY)
    await p.key()
    expect(a.tokens).toEqual(["sa-1", "sa-2"])
  })

  test("a non-link-local URI is refused without a request", async () => {
    const a = agent()
    for (const bad of [`http://localhost:${a.srv.port}/v1/credentials`, `https://127.0.0.1:${a.srv.port}/`, "http://169.254.169.254/", "http://u:p@127.0.0.1/", "http://10.0.0.1/", "nonsense"])
      expect(() => checkContainerUri(bad)).toThrow("must be http://169.254.170.23")
    for (const ok of ["http://169.254.170.23/v1/credentials", "http://[fd65:7a67:6800::23]/v1/credentials", "http://127.0.0.1:8080/", "http://[::1]/"])
      expect(() => checkContainerUri(ok)).not.toThrow()
    inPod(`http://localhost:${a.srv.port}/v1/credentials`, tokenFile("sa"))
    const e = await new Client({ organizationId: "o", projectId: "p", baseUrl: a.url }).getApp("app_1").catch((x) => x)
    expect(String(e)).toContain("must be http://169.254.170.23")
    expect([a.gets, a.apiAuth.length]).toEqual([0, 0])
  })

  test("a missing token file is an error, not the instance", async () => {
    const meta = onInstance()
    const a = agent()
    inPod(a.uri, join(tmpdir(), "ezgh-no-such-token"))
    const e = await new Client({ organizationId: "o", projectId: "p", baseUrl: a.url }).getApp("app_1").catch((x) => x)
    expect(String(e)).toContain("EZGH_CONTAINER_AUTHORIZATION_TOKEN_FILE")
    expect([a.gets, a.apiAuth.length, meta.hits]).toEqual([0, 0, 0])
  })
})
