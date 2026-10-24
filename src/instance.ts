// Instance credentials: on an EZGH compute instance with a Bot attached, a client given no API
// key uses the Bot's short-lived key from the instance's metadata service.
import type { FetchLike } from "./runtime/live.ts"

const PRODUCT = "EZGH Compute"
const REFRESH_AHEAD_MS = 5 * 60_000
const TOKEN_TTL_S = 6 * 3600
const RETRY_AFTER_MS = 5_000
let productNamePath = "/sys/class/dmi/id/product_name"
let metadataEndpoint = "http://169.254.169.254"
let metadataFetch: FetchLike = directFetch

// For tests only (not exported from the package index).
export function setProductNamePath(p: string): () => void {
  const old = productNamePath
  productNamePath = p
  return () => { productNamePath = old }
}

// For tests only: where the metadata service is, and the fetch that reaches it.
export function setMetadataForTests(o: { endpoint?: string; fetch?: FetchLike }): () => void {
  const old = { e: metadataEndpoint, f: metadataFetch }
  if (o.endpoint !== undefined) metadataEndpoint = o.endpoint
  if (o.fetch !== undefined) metadataFetch = o.fetch
  return () => { metadataEndpoint = old.e; metadataFetch = old.f }
}

// directFetch reaches the metadata service without ever using a proxy: the session token and the
// Bot's key must not cross one (a forward proxy on another EZGH instance would answer with its own
// Bot's key), and global fetch honours HTTP_PROXY in Bun and Deno. node:http with its own Agent
// uses none (Node's env-proxy support applies to the global agents only).
interface NodeHttp {
  Agent: new () => unknown
  request(url: string, o: { method: string; headers?: Record<string, string>; agent: unknown; signal?: AbortSignal }, cb: (res: NodeRes) => void): NodeReq
}
interface NodeReq { on(ev: "error", f: (e: Error) => void): void; end(): void }
interface NodeRes {
  statusCode?: number
  headers: Record<string, string | string[] | undefined>
  on(ev: "data", f: (c: Uint8Array) => void): void
  on(ev: "end", f: () => void): void
  on(ev: "error", f: (e: Error) => void): void
}

function directFetch(url: string, init: RequestInit = {}): Promise<Response> {
  const http = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process?.getBuiltinModule?.("node:http") as NodeHttp | undefined
  if (!http) return Promise.reject(new Error("no node:http to reach the metadata service"))
  return new Promise<Response>((resolve, reject) => {
    const req = http.request(url, { method: init.method ?? "GET", headers: init.headers as Record<string, string>, agent: new http.Agent(), ...(init.signal ? { signal: init.signal } : {}) }, (res) => {
      const chunks: Uint8Array[] = []
      res.on("data", (c) => chunks.push(c))
      res.on("error", reject)
      res.on("end", () => {
        const h = new Headers()
        for (const [k, v] of Object.entries(res.headers)) if (v !== undefined) h.set(k, Array.isArray(v) ? v.join(", ") : v)
        const body = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0))
        let at = 0
        for (const c of chunks) { body.set(c, at); at += c.length }
        const nobody = res.statusCode === 204 || res.statusCode === 304
        resolve(new Response(nobody ? null : body, { status: res.statusCode ?? 0, headers: h }))
      })
    })
    req.on("error", reject)
    req.end()
  })
}

// NoBotError: the instance has no Bot attached, so there are no instance credentials.
export class NoBotError extends Error {
  override name = "NoBotError"
  constructor() { super("This instance has no Bot attached") }
}

// onInstance: this machine is an EZGH compute instance (SMBIOS product name). No network.
// Needs node:fs through process.getBuiltinModule (Node 22.3+, Bun, Deno 2); elsewhere false.
function onInstance(): boolean {
  try {
    const p = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process
    const fs = p?.getBuiltinModule?.("node:fs") as { readFileSync(path: string, enc: "utf8"): string } | undefined
    return fs?.readFileSync(productNamePath, "utf8").trim() === PRODUCT
  } catch {
    return false
  }
}

// KeySource gives the key for each request; refused(key) is told the API answered 401 to it and
// says whether one retry is worthwhile.
export interface KeySource {
  key(): Promise<string>
  refused(key: string): boolean
}

// keySourceFor: the API key if given, else the instance's credentials on an instance, else a
// TypeError ("<what> needs an apiKey").
export function keySourceFor(apiKey: unknown, what: string, now: () => number = Date.now): KeySource {
  if (typeof apiKey === "string" && apiKey !== "") return { key: async () => apiKey, refused: () => false }
  if ((apiKey === undefined || apiKey === "") && onInstance()) return new InstanceProvider(undefined, now)
  throw new TypeError(`${what} needs an apiKey`)
}

export class InstanceProvider implements KeySource {
  #token = ""
  #tokenExp = 0
  #apiKey = ""
  #expiresAt = 0
  #nextTry = 0
  #lastErr: unknown
  #retryMs = 0 // the metadata service's Retry-After (503, 429) on the last answer
  #inflight: Promise<string> | undefined
  constructor(private readonly fetchImpl?: FetchLike, private readonly now: () => number = Date.now, private readonly endpoint?: string) {}

  async key(): Promise<string> {
    const now = this.now()
    if (this.#apiKey && now + REFRESH_AHEAD_MS < this.#expiresAt) return this.#apiKey
    if (now >= this.#nextTry) {
      try {
        return await (this.#inflight ??= this.fetchKey().finally(() => { this.#inflight = undefined }))
      } catch (e) {
        this.#lastErr = e
        this.#nextTry = now + Math.max(RETRY_AFTER_MS, this.#retryMs)
        if (e instanceof NoBotError) this.#apiKey = ""
        // A failed fetch keeps the old key until it actually expires.
        if (this.#apiKey && now < this.#expiresAt) return this.#apiKey
        throw e
      }
    }
    if (this.#apiKey && now < this.#expiresAt) return this.#apiKey
    // Backing off: report the failure that started it, without asking again.
    throw this.#lastErr ?? new Error("Instance credentials expired and the metadata service is not answering")
  }

  refused(key: string): boolean {
    if (this.#apiKey === key) { this.#apiKey = ""; this.#expiresAt = 0; this.#nextTry = 0 }
    return true
  }

  private async fetchKey(): Promise<string> {
    for (let attempt = 0; ; attempt++) {
      if (!this.#token || this.now() >= this.#tokenExp) await this.newToken()
      const res = await this.call("GET", "/ezgh/v1/credentials", { "EZGH-Metadata-Token": this.#token }, 10_000)
      if (res.status === 401 && attempt === 0) { this.#token = ""; await res.body?.cancel().catch(() => {}); continue }
      if (res.status === 404) {
        // no_bot is final; any other 404 (not_found: mid-migration) is transient, like a 503.
        const c = (await res.json().catch(() => undefined)) as { error?: { code?: unknown } } | undefined
        if (c?.error?.code === "no_bot") throw new NoBotError()
        this.#retryMs = 0
        throw new Error("Instance metadata answered 404 not_found")
      }
      if (!res.ok) { const ra = Number(res.headers.get("Retry-After")); this.#retryMs = Number.isFinite(ra) && ra > 0 ? ra * 1000 : 0; await res.body?.cancel().catch(() => {}); throw new Error(`Instance metadata answered ${res.status}`) }
      const c = (await res.json().catch(() => undefined)) as { apiKey?: unknown; expiresAt?: unknown } | undefined
      const expires = typeof c?.expiresAt === "string" ? Date.parse(c.expiresAt) : NaN
      if (typeof c?.apiKey !== "string" || c.apiKey === "" || Number.isNaN(expires) || expires <= 0) throw new Error("Instance metadata sent an unreadable answer")
      this.#apiKey = c.apiKey
      this.#expiresAt = expires
      return this.#apiKey
    }
  }

  private async newToken(): Promise<void> {
    const res = await this.call("PUT", "/ezgh/v1/token", { "EZGH-Metadata-Token-TTL-Seconds": String(TOKEN_TTL_S) }, 1_000)
    const text = res.ok ? (await res.text()).trim() : (await res.body?.cancel().catch(() => {}), "")
    if (!text) throw new Error(`Instance metadata answered ${res.status} to the token request`)
    this.#token = text
    this.#tokenExp = this.now() + TOKEN_TTL_S * 1000 - REFRESH_AHEAD_MS
  }

  private async call(method: string, path: string, headers: Record<string, string>, timeoutMs: number): Promise<Response> {
    try {
      return await (this.fetchImpl ?? metadataFetch)((this.endpoint ?? metadataEndpoint) + path, { method, headers: { "EZGH-Metadata": "true", ...headers }, redirect: "manual", signal: AbortSignal.timeout(timeoutMs) })
    } catch (e) {
      throw new Error(`Instance metadata unreachable: ${e instanceof Error ? e.message : String(e)}`)
    }
  }
}
