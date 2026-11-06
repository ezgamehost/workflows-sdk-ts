// Container credentials: in an EZGH Kubernetes pod whose service account has a pod identity
// association, a client given no API key uses the Bot's short-lived key from the node's pod
// identity agent, presenting the pod's projected service account token.
import { REFRESH_AHEAD_MS, RETRY_AFTER_MS, directFetch, retryAfterMs, type KeySource } from "./instance.ts"

// The variables EZGH Kubernetes injects into such a pod.
const URI_ENV = "EZGH_CONTAINER_CREDENTIALS_FULL_URI"
const TOKEN_FILE_ENV = "EZGH_CONTAINER_AUTHORIZATION_TOKEN_FILE"
const AGENT_HOSTS = new Set(["169.254.170.23", "[fd65:7a67:6800::23]"])

type Proc = { env?: Record<string, string | undefined>; getBuiltinModule?: (id: string) => unknown }
const proc = () => (globalThis as { process?: Proc }).process

// containerEnv: the URI and token file when EZGH_CONTAINER_CREDENTIALS_FULL_URI is set. No network.
export function containerEnv(): { uri: string; tokenFile: string } | undefined {
  try {
    const env = proc()?.env // Deno throws without --allow-env
    return env?.[URI_ENV] ? { uri: env[URI_ENV], tokenFile: env[TOKEN_FILE_ENV] ?? "" } : undefined
  } catch {
    return undefined
  }
}

// checkContainerUri allows only the pod identity agent's addresses, or loopback, over plain http,
// so the service account token never leaves the node.
export function checkContainerUri(raw: string): void {
  let u: URL | undefined
  try { u = new URL(raw) } catch { /* refused below */ }
  if (u && u.protocol === "http:" && !u.username && !u.password && (AGENT_HOSTS.has(u.hostname) || u.hostname === "[::1]" || /^127\.\d+\.\d+\.\d+$/.test(u.hostname))) return
  throw new Error(`${URI_ENV} must be http://169.254.170.23, http://[fd65:7a67:6800::23] or a loopback address`)
}

export class ContainerProvider implements KeySource {
  #apiKey = ""
  #expiresAt = 0
  #nextTry = 0
  #lastErr: unknown
  #retryMs = 0 // the agent's Retry-After on the last answer
  #inflight: Promise<string> | undefined
  constructor(private readonly uri: string, private readonly tokenFile: string, private readonly now: () => number = Date.now) {}

  async key(): Promise<string> {
    const now = this.now()
    if (this.#apiKey && now + REFRESH_AHEAD_MS < this.#expiresAt) return this.#apiKey
    if (now >= this.#nextTry) {
      try {
        return await (this.#inflight ??= this.fetchKey().finally(() => { this.#inflight = undefined }))
      } catch (e) {
        const after = this.now() // the fetch may have taken seconds
        this.#lastErr = e
        this.#nextTry = after + Math.max(RETRY_AFTER_MS, this.#retryMs)
        // A failed fetch keeps the old key until it actually expires.
        if (this.#apiKey && after < this.#expiresAt) return this.#apiKey
        throw e
      }
    }
    if (this.#apiKey && now < this.#expiresAt) return this.#apiKey
    // Backing off: report the failure that started it, without asking again.
    throw this.#lastErr ?? new Error("Container credentials expired and the pod identity agent is not answering")
  }

  refused(key: string): boolean {
    if (this.#apiKey === key) { this.#apiKey = ""; this.#expiresAt = 0; this.#nextTry = 0 }
    return true
  }

  private async fetchKey(): Promise<string> {
    this.#retryMs = 0
    checkContainerUri(this.uri)
    if (!this.tokenFile) throw new Error(`${URI_ENV} is set but ${TOKEN_FILE_ENV} is not`)
    // Read afresh every time: kubelet rotates the projected token.
    let token: string
    try {
      const fs = proc()?.getBuiltinModule?.("node:fs") as { readFileSync(path: string, enc: "utf8"): string } | undefined
      if (!fs) throw new Error("no node:fs")
      token = fs.readFileSync(this.tokenFile, "utf8").trim()
    } catch (e) {
      throw new Error(`Reading the pod's service account token (${TOKEN_FILE_ENV}): ${e instanceof Error ? e.message : String(e)}`)
    }
    if (!token) throw new Error(`The pod's service account token (${TOKEN_FILE_ENV}) is empty`)
    let res: Response
    try {
      res = await directFetch(this.uri, { headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(10_000) }, 1_000)
    } catch (e) {
      throw new Error(`Pod identity agent unreachable: ${e instanceof Error ? e.message : String(e)}`)
    }
    this.#retryMs = retryAfterMs(res.headers.get("Retry-After"))
    const c = (await res.json().catch(() => undefined)) as { apiKey?: unknown; expiresAt?: unknown; error?: { code?: unknown } } | undefined
    if (!res.ok) throw new Error(`Pod identity agent answered ${res.status}${typeof c?.error?.code === "string" ? ` (${c.error.code})` : ""}`)
    const expires = typeof c?.expiresAt === "string" ? Date.parse(c.expiresAt) : NaN
    if (typeof c?.apiKey !== "string" || c.apiKey === "" || Number.isNaN(expires) || expires <= 0) throw new Error("Pod identity agent sent an unreadable answer")
    this.#apiKey = c.apiKey
    this.#expiresAt = expires
    return this.#apiKey
  }
}
