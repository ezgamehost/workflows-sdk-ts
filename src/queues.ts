import { toHex, utf8 } from "./protocol/bytes.ts"
import { keySourceFor, type KeySource } from "./instance.ts"
import type { FetchLike } from "./runtime/live.ts"

// EZGH Queues: a client for the public send API (queues-api), which step.sendToQueue uses.

// A message to send. body is UTF-8 text; body and attributes together are at most 256 KiB.
// groupId is required on FIFO queues and refused on standard ones; dedupId is FIFO only.
// delaySeconds (0–900) is standard queues only.
export interface QueueMessageInput {
  body: string
  attributes?: Record<string, string>
  delaySeconds?: number
  groupId?: string
  dedupId?: string
}
// One result per message, in order. seq: FIFO only. duplicate: true when a FIFO dedupId was seen
// in the last 5 minutes, so the message wasn't stored again (messageId is the original's).
export interface SentMessage { messageId: string; seq?: string; duplicate?: boolean }
export interface SentMessages { messages: SentMessage[] }

// An error answer from the Queues API: the HTTP status and the platform error code
// (status 0, code "network_error" when every attempt failed in transit).
// data ({ status, code }) is what a step failure records, so a replay's WorkflowError keeps them.
export class QueuesApiError extends Error {
  override name = "QueuesApiError"
  readonly data: { status: number; code: string }
  constructor(readonly status: number, readonly code: string, message: string, readonly retryAfterSeconds?: number) {
    super(message)
    this.data = { status, code }
  }
}

export interface QueuesClientOptions {
  // An EZGH API key (or other bearer credential) with queues.messages.send on the queues it sends
  // to. Omit it on an EZGH compute instance with a Bot attached: the client then uses the Bot's
  // short-lived key from the metadata service (Node 22.3+, Bun, Deno 2).
  apiKey?: string
  organizationId: string
  projectId: string
  // Default https://queues-api.ezghcloud.com.
  baseUrl?: string
  fetch?: FetchLike
  // How many times a 429, a 5xx, a 409 idempotency_in_progress or a network error is resent
  // (default 3), after Retry-After (at most maxRetryDelayMs, default 10 s), always with the same
  // Idempotency-Key. A 429 quota_exceeded isn't resent: it isn't a failure to retry on a timer.
  retries?: number
  maxRetryDelayMs?: number
  // Each request's timeout (default 30 s); a timed-out request is resent like a network error.
  timeoutMs?: number
}

export const DEFAULT_QUEUES_BASE = "https://queues-api.ezghcloud.com"
const DEFAULT_TIMEOUT_MS = 30_000
const IDEMPOTENCY_KEY = /^[\x21-\x7e]{1,200}$/
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const id = encodeURIComponent

function apiError(res: Response, data: unknown): QueuesApiError {
  const e = (data as { error?: { code?: unknown; message?: unknown } } | undefined)?.error
  if (res.status >= 300 && res.status < 400)
    return new QueuesApiError(res.status, "redirect", `The Queues API answered a redirect (${res.status}); requests are never redirected, so check baseUrl`)
  const raw = res.headers.get("Retry-After")
  const ra = raw === null ? NaN : Number(raw)
  return new QueuesApiError(res.status, typeof e?.code === "string" ? e.code : "unknown", typeof e?.message === "string" ? e.message : `HTTP ${res.status}`,
    Number.isFinite(ra) && ra >= 0 ? ra : undefined)
}

// resendable: answers a resend with the same Idempotency-Key may fix.
function resendable(err: QueuesApiError): boolean {
  if (err.status === 429) return err.code !== "quota_exceeded"
  if (err.status === 409) return err.code === "idempotency_in_progress"
  return err.status >= 500
}

// QueuesClient sends messages to EZGH Queues for one project. Pass it to step.sendToQueue, or call
// send directly outside a workflow.
export class QueuesClient {
  // ES private fields: the API key never shows in console.log, util.inspect or JSON.stringify.
  readonly #cred: KeySource
  readonly #base: string
  readonly #fetch: FetchLike
  readonly #retries: number
  readonly #maxDelay: number
  readonly #timeout: number
  constructor(opts: QueuesClientOptions) {
    if (typeof opts.organizationId !== "string" || opts.organizationId === "") throw new TypeError("QueuesClient needs an organizationId")
    if (typeof opts.projectId !== "string" || opts.projectId === "") throw new TypeError("QueuesClient needs a projectId")
    this.#base = `${(opts.baseUrl ?? DEFAULT_QUEUES_BASE).replace(/\/+$/, "")}/v1/organizations/${id(opts.organizationId)}/projects/${id(opts.projectId)}`
    this.#fetch = opts.fetch ?? ((u, i) => fetch(u, i))
    this.#cred = keySourceFor(opts.apiKey, "QueuesClient", this.#fetch)
    this.#retries = opts.retries ?? 3
    this.#maxDelay = opts.maxRetryDelayMs ?? 10_000
    this.#timeout = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS
    if (!Number.isInteger(this.#retries) || this.#retries < 0) throw new TypeError("retries is a whole number ≥ 0")
    if (!Number.isFinite(this.#maxDelay) || this.#maxDelay < 0) throw new TypeError("maxRetryDelayMs is a number ≥ 0")
    if (!Number.isFinite(this.#timeout) || this.#timeout <= 0) throw new TypeError("timeoutMs is a number > 0")
  }

  // send sends 1–10 messages to a queue (queues.messages.send) in one request: all are stored, or
  // none. Every attempt carries the same Idempotency-Key (yours, or a random one), which the API
  // honors for 48 hours: a resend answers the first response instead of sending again.
  async send(queueId: string, messages: QueueMessageInput[], opts: { idempotencyKey?: string } = {}): Promise<SentMessages> {
    if (typeof queueId !== "string" || queueId === "") throw new TypeError("send needs a queueId")
    if (!Array.isArray(messages) || messages.length < 1 || messages.length > 10) throw new TypeError("send takes 1 to 10 messages")
    const key = opts.idempotencyKey ?? crypto.randomUUID()
    if (typeof key !== "string" || !IDEMPOTENCY_KEY.test(key)) throw new TypeError("An idempotencyKey is 1–200 printable ASCII characters")
    const headers = { Authorization: "", Accept: "application/json", "Content-Type": "application/json", "Idempotency-Key": key }
    const body = JSON.stringify({ messages })
    const url = `${this.#base}/queues/${id(queueId)}/messages`
    let reauthed = false // an instance key the API refused is replaced once per call
    for (let attempt = 0; ; attempt++) {
      const apiKey = await this.#cred.key()
      headers.Authorization = `Bearer ${apiKey}`
      let res: Response
      try {
        res = await this.#fetch(url, { method: "POST", headers, body, redirect: "manual", signal: AbortSignal.timeout(this.#timeout) })
      } catch (e) {
        if (attempt >= this.#retries)
          throw new QueuesApiError(0, "network_error", `Sending to the queue failed in transit: ${e instanceof Error ? e.message : String(e)}`)
        await sleep(Math.min(this.#maxDelay, 500 * 2 ** attempt))
        continue
      }
      const text = await res.text()
      let data: unknown
      try { data = text ? JSON.parse(text) : undefined } catch { data = undefined }
      if (res.ok) {
        if (!Array.isArray((data as SentMessages | undefined)?.messages))
          throw new QueuesApiError(res.status, "invalid_response", "The Queues API answered without a messages list")
        return data as SentMessages
      }
      const err = apiError(res, data)
      if (res.status === 401 && !reauthed && this.#cred.refused(apiKey)) { reauthed = true; attempt--; continue }
      if (!resendable(err) || attempt >= this.#retries) throw err
      await sleep(Math.min(this.#maxDelay, err.retryAfterSeconds !== undefined ? err.retryAfterSeconds * 1000 : 500 * 2 ** attempt))
    }
  }
}

// sendToQueueDigest is step.sendToQueue's key material: lowercase hex of
// SHA-256(runId + ":" + stepId). The step sends Idempotency-Key "wfq_" + digest, and gives each
// FIFO message (one with a groupId) without a dedupId the dedupId digest + ":" + its index.
export async function sendToQueueDigest(runId: string, stepId: string): Promise<string> {
  return toHex(new Uint8Array(await crypto.subtle.digest("SHA-256", utf8(`${runId}:${stepId}`))))
}

// withDedupIds: the messages step.sendToQueue sends, a FIFO message (one with a groupId) given the
// dedupId digest:index unless it has its own. Standard queues refuse dedupId, so messages without
// a groupId are sent as they are.
export function withDedupIds(messages: QueueMessageInput[], digest: string): QueueMessageInput[] {
  return messages.map((m, i) => (m && typeof m === "object" && m.groupId !== undefined && m.dedupId === undefined ? { ...m, dedupId: `${digest}:${i}` } : m))
}
