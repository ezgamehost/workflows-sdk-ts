import type { Checkpoint, Journal } from "../protocol/index.ts"

// LiveIO is what an invocation calls on workflows-live while it runs.
export interface LiveIO {
  checkpoint(cp: Checkpoint): Promise<void>
  journalPage(afterSeq: number): Promise<Journal>
  payload(id: string): Promise<unknown>
}
// The engine has moved on: another invocation owns the run. Stop at once.
export class InvocationSuperseded extends Error { override name = "InvocationSuperseded" }
// workflows-live refused or couldn't take a call after retries: the step must not run. code is
// what serve() tells dispatch: "payload_unavailable" when the live host's last answer was a 503
// payload_unavailable (its payload store is down: dispatch charges no step attempt), else
// "checkpoint_failed".
export class CheckpointFailed extends Error {
  override name = "CheckpointFailed"
  constructor(message: string, readonly code: "checkpoint_failed" | "payload_unavailable" = "checkpoint_failed") { super(message) }
}

// errorCode reads the kit's {"error":{"code"}} from a small error body, or undefined.
async function errorCode(res: Response): Promise<string | undefined> {
  try {
    const text = await res.text()
    if (text.length > 64 * 1024) return undefined
    const code = (JSON.parse(text) as { error?: { code?: unknown } }).error?.code
    return typeof code === "string" ? code : undefined
  } catch {
    return undefined
  }
}

export interface LiveOptions {
  // The invocation's local deadline (epoch ms). Each call is aborted at it, and 429/5xx answers and
  // network errors are retried (after Retry-After, or backoff) while more than reserveMs would
  // remain. Without a deadline: 4 attempts, each call aborted after 30 s.
  deadline?: number
  reserveMs?: number
  now?: () => number
  // Without a deadline: attempts and the first backoff delay (tests).
  attempts?: number
  retryDelayMs?: number
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const MAX_BACKOFF_MS = 5000

function retryAfterMs(res: Response): number | undefined {
  const v = res.headers.get("Retry-After")
  if (!v) return undefined
  const n = Number(v)
  if (Number.isFinite(n) && n >= 0) return n * 1000
  const at = Date.parse(v)
  return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now())
}

// httpLive talks to workflows-live (plan D2, D25) with Authorization: Bearer <invocation token>.
// 409 means superseded; 429, 5xx and network errors are retried; anything else non-2xx fails
// the invocation (CheckpointFailed).
export function httpLive(base: string, token: string, fetchImpl: FetchLike = (u, i) => fetch(u, i), opts: LiveOptions = {}): LiveIO {
  const root = base.replace(/\/+$/, "")
  const now = opts.now ?? Date.now
  const reserve = opts.reserveMs ?? 0
  const firstDelay = opts.retryDelayMs ?? 200
  async function call(path: string, init: RequestInit): Promise<Response> {
    const headers: Record<string, string> = { Authorization: `Bearer ${token}` }
    if (init.body !== undefined) headers["Content-Type"] = "application/json"
    const what = path.split("?")[0]
    let last = ""
    let payloadDown = false // the last answer was 503 payload_unavailable
    const failed = (message: string) => new CheckpointFailed(message, payloadDown ? "payload_unavailable" : "checkpoint_failed")
    for (let attempt = 0; ; attempt++) {
      const timeout = opts.deadline === undefined ? 30_000 : opts.deadline - now()
      if (timeout <= 0) throw failed(`No time left for ${what}: ${last || "the deadline passed"}`)
      let res: Response | undefined
      let wait: number | undefined
      try {
        res = await fetchImpl(root + path, { ...init, headers, signal: AbortSignal.timeout(Math.max(1, Math.floor(timeout))), redirect: "manual" })
      } catch (e) {
        last = e instanceof Error ? e.name : "network error"
        payloadDown = false
      }
      if (res) {
        if (res.ok) return res
        const code = res.status === 503 ? await errorCode(res) : (await res.body?.cancel().catch(() => {}), undefined)
        if (res.status === 409) throw new InvocationSuperseded("The engine superseded this invocation")
        if (res.status !== 429 && res.status < 500) throw new CheckpointFailed(`workflows-live answered ${res.status} for ${what}`)
        last = code ? `HTTP ${res.status} ${code}` : `HTTP ${res.status}`
        payloadDown = code === "payload_unavailable"
        wait = retryAfterMs(res)
      }
      // Retry-After never shortens the backoff (a 0 or a past date mustn't spin).
      const delay = Math.max(wait ?? 0, Math.min(MAX_BACKOFF_MS, firstDelay * 2 ** attempt))
      const more = opts.deadline === undefined ? attempt + 1 < (opts.attempts ?? 4) : opts.deadline - now() - delay > reserve
      if (!more) throw failed(`workflows-live didn't take ${what}: ${last}`)
      await sleep(delay)
    }
  }
  return {
    async checkpoint(cp) {
      const res = await call("/v1/invocations/checkpoint", { method: "POST", body: JSON.stringify(cp) })
      await res.body?.cancel().catch(() => {})
    },
    async journalPage(afterSeq) {
      return (await (await call(`/v1/invocations/journal?afterSeq=${afterSeq}`, { method: "GET" })).json()) as Journal
    },
    async payload(id) {
      return (await call(`/v1/invocations/payloads/${encodeURIComponent(id)}`, { method: "GET" })).json()
    },
  }
}

// FetchLike is the part of fetch the SDK uses; the global fetch satisfies it.
export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>
