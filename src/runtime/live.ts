import type { Checkpoint, Journal } from "../protocol/index.ts"

// LiveIO is what an invocation calls on workflows-live while it runs.
export interface LiveIO {
  checkpoint(cp: Checkpoint): Promise<void>
  journalPage(afterSeq: number): Promise<Journal>
  payload(id: string): Promise<unknown>
}
// The engine has moved on: another invocation owns the run. Stop at once.
export class InvocationSuperseded extends Error { override name = "InvocationSuperseded" }
// workflows-live refused or couldn't take a call after retries: the step must not run.
export class CheckpointFailed extends Error { override name = "CheckpointFailed" }

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
    for (let attempt = 0; ; attempt++) {
      const timeout = opts.deadline === undefined ? 30_000 : opts.deadline - now()
      if (timeout <= 0) throw new CheckpointFailed(`No time left for ${what}: ${last || "the deadline passed"}`)
      let res: Response | undefined
      let wait: number | undefined
      try {
        res = await fetchImpl(root + path, { ...init, headers, signal: AbortSignal.timeout(Math.max(1, Math.floor(timeout))) })
      } catch (e) {
        last = e instanceof Error ? e.name : "network error"
      }
      if (res) {
        if (res.ok) return res
        await res.body?.cancel().catch(() => {})
        if (res.status === 409) throw new InvocationSuperseded("The engine superseded this invocation")
        if (res.status !== 429 && res.status < 500) throw new CheckpointFailed(`workflows-live answered ${res.status} for ${what}`)
        last = `HTTP ${res.status}`
        wait = retryAfterMs(res)
      }
      // Retry-After never shortens the backoff (a 0 or a past date mustn't spin).
      const delay = Math.max(wait ?? 0, Math.min(MAX_BACKOFF_MS, firstDelay * 2 ** attempt))
      const more = opts.deadline === undefined ? attempt + 1 < (opts.attempts ?? 4) : opts.deadline - now() - delay > reserve
      if (!more) throw new CheckpointFailed(`workflows-live didn't take ${what}: ${last}`)
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
