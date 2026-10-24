// Instance credentials: on an EZGH compute instance with a Bot attached, a client given no API
// key uses the Bot's short-lived key from the instance's metadata service.
import type { FetchLike } from "./runtime/live.ts"

const METADATA = "http://169.254.169.254"
const PRODUCT = "EZGH Compute"
const REFRESH_AHEAD_MS = 5 * 60_000
const TOKEN_TTL_S = 6 * 3600
const RETRY_AFTER_MS = 5_000
let productNamePath = "/sys/class/dmi/id/product_name"

// For tests only (not exported from the package index).
export function setProductNamePath(p: string): () => void {
  const old = productNamePath
  productNamePath = p
  return () => { productNamePath = old }
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
export function keySourceFor(apiKey: unknown, what: string, fetchImpl: FetchLike, now: () => number = Date.now): KeySource {
  if (typeof apiKey === "string" && apiKey !== "") return { key: async () => apiKey, refused: () => false }
  if ((apiKey === undefined || apiKey === "") && onInstance()) return new InstanceProvider(fetchImpl, now)
  throw new TypeError(`${what} needs an apiKey`)
}

export class InstanceProvider implements KeySource {
  #token = ""
  #tokenExp = 0
  #apiKey = ""
  #expiresAt = 0
  #nextTry = 0
  #retryMs = 0 // the metadata service's Retry-After (503, 429) on the last answer
  #inflight: Promise<string> | undefined
  constructor(private readonly fetchImpl: FetchLike, private readonly now: () => number = Date.now, private readonly endpoint = METADATA) {}

  async key(): Promise<string> {
    const now = this.now()
    if (this.#apiKey && now + REFRESH_AHEAD_MS < this.#expiresAt) return this.#apiKey
    if (!this.#apiKey || now >= this.#nextTry) {
      try {
        return await (this.#inflight ??= this.fetchKey().finally(() => { this.#inflight = undefined }))
      } catch (e) {
        this.#nextTry = now + Math.max(RETRY_AFTER_MS, this.#retryMs)
        if (e instanceof NoBotError) this.#apiKey = ""
        // A failed fetch keeps the old key until it actually expires.
        if (this.#apiKey && now < this.#expiresAt) return this.#apiKey
        throw e
      }
    }
    if (now < this.#expiresAt) return this.#apiKey
    throw new Error("Instance credentials expired and the metadata service is not answering")
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
      if (typeof c?.apiKey !== "string" || c.apiKey === "" || Number.isNaN(expires)) throw new Error("Instance metadata sent an unreadable answer")
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
      return await this.fetchImpl(this.endpoint + path, { method, headers: { "EZGH-Metadata": "true", ...headers }, redirect: "manual", signal: AbortSignal.timeout(timeoutMs) })
    } catch (e) {
      throw new Error(`Instance metadata unreachable: ${e instanceof Error ? e.message : String(e)}`)
    }
  }
}
