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

export interface LiveOptions { attempts?: number; retryDelayMs?: number }

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

// httpLive talks to workflows-live (plan D2, D25) with Authorization: Bearer <invocation token>.
// 409 means superseded; 429, 5xx and network errors are retried with backoff; anything else
// non-2xx fails the invocation.
export function httpLive(base: string, token: string, fetchImpl: typeof fetch = fetch, opts: LiveOptions = {}): LiveIO {
  const root = base.replace(/\/+$/, "")
  const attempts = opts.attempts ?? 4
  const delay = opts.retryDelayMs ?? 200
  async function call(path: string, init: RequestInit): Promise<Response> {
    const headers: Record<string, string> = { Authorization: `Bearer ${token}` }
    if (init.body !== undefined) headers["Content-Type"] = "application/json"
    let last = ""
    for (let attempt = 0; attempt < attempts; attempt++) {
      if (attempt > 0) await sleep(delay * 2 ** (attempt - 1))
      let res: Response
      try { res = await fetchImpl(root + path, { ...init, headers }) } catch (e) { last = e instanceof Error ? e.name : "network error"; continue }
      if (res.ok) return res
      await res.body?.cancel().catch(() => {})
      if (res.status === 409) throw new InvocationSuperseded("The engine superseded this invocation")
      if (res.status === 429 || res.status >= 500) { last = `HTTP ${res.status}`; continue }
      throw new CheckpointFailed(`workflows-live answered ${res.status} for ${path.split("?")[0]}`)
    }
    throw new CheckpointFailed(`workflows-live didn't answer ${path.split("?")[0]}: ${last}`)
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
