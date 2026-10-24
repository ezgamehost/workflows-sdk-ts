// The pull worker (design-phase-2, "Pull workers"): it long-polls workflows-live for invocations
// of one pull app's task queue, runs them with the same runtime serve() uses, heartbeats while
// they run (a heartbeat's reply says whether the run is cancelling) and completes them. Runs on
// Node, Bun and Deno (anything with fetch and timers).
import type { HeartbeatResponse, Invocation, InvocationResult, WorkflowInfo } from "./protocol/index.ts"
import { PROTOCOL_VERSIONS } from "./protocol/index.ts"
import { keySourceFor } from "./instance.ts"
import { cleanDeployment, detectDeployment } from "./env.ts"
import { CancelledError } from "./runtime/errors.ts"
import { execute } from "./runtime/execution.ts"
import { parseInvocation } from "./runtime/invocation.ts"
import { InvocationSuperseded, httpLive, type FetchLike } from "./runtime/live.ts"
import { definitionOf, type Workflow } from "./runtime/workflow.ts"
import { SDK_NAME, SDK_VERSION } from "./version.ts"

export interface WorkerOptions {
  // An EZGH API key with workflows.workers.poll (and workflows.apps.sync, to announce workflows).
  // Omit it on an EZGH compute instance with a Bot attached: the worker then uses the Bot's
  // short-lived key from the metadata service (Node 22.3+, Bun, Deno 2).
  apiKey?: string
  organizationId: string
  projectId: string
  // The pull app's ID (Client.createApp(name, { kind: "pull" })).
  app: string
  // The app's task queue (default "default"; lowercase letters, digits and -, at most 63). dev
  // mode uses dev-<user> instead.
  queue?: string
  workflows: Workflow<any, any>[]
  // Tasks asked for per poll (1–10, default 4) and invocations run at once (default 10).
  maxTasks?: number
  concurrency?: number
  liveUrl?: string
  // Callback URLs when an invocation carries no endpoints.api.
  apiBaseUrl?: string
  workerId?: string
  // Dev mode: poll the queue dev-<user> (the given user, else $USER, $USERNAME or $LOGNAME). The
  // live host serves only an app's own queue, so app must be the developer's own pull app with
  // that queue: Client.devApp() creates or finds it. Developers sharing a project then never take
  // each other's runs.
  dev?: boolean | { user?: string }
  // How often a running invocation heartbeats (default 20 s; the engine's heartbeat timeout is 60 s).
  heartbeatIntervalMs?: number
  // The longest wait between failed polls (default 30 s).
  backoffMaxMs?: number
  maxStepsPerInvocation?: number
  deployment?: string
  fetch?: FetchLike
  // Errors the worker recovers from: a failed poll, a lost heartbeat, an invocation that couldn't
  // finish. Fatal ones (WorkerAuthError, WorkerConfigError) reject run() instead.
  onError?: (error: unknown, context: string) => void
}

export interface Worker {
  // Polls and runs invocations until stop(); rejects on a fatal error (the key, the app or the queue).
  run(): Promise<void>
  // Stops polling, waits up to graceMs (default 30 s) for running invocations, then resolves.
  stop(opts?: { graceMs?: number }): Promise<void>
}

// workflows-live refused the API key (401) or its permissions (403).
export class WorkerAuthError extends Error {
  override name = "WorkerAuthError"
  constructor(readonly status: number, message: string) { super(message) }
}
// workflows-live refused the poll for another reason: an unknown app or queue, a bad request.
export class WorkerConfigError extends Error {
  override name = "WorkerConfigError"
  constructor(readonly status: number, readonly code: string, message: string) { super(message) }
}
class Retryable extends Error { constructor(message: string, readonly retryAfterMs?: number) { super(message) } }

const DEFAULT_LIVE = "https://workflows-live.ezghcloud.com"
// internal/protocol PollRequest.Validate: queue names and worker IDs.
const QUEUE = /^[a-z0-9][a-z0-9-]{0,62}$/
const WORKER_ID = /^[A-Za-z0-9._:-]{1,128}$/
const POLL_TIMEOUT_MS = 70_000 // the gateway holds a poll at most 55 s
const MAX_HEARTBEAT_INTERVAL_MS = 30_000
const DEFAULT_GRACE_MS = 30_000
// After the grace period, running steps are cancelled and get this long to finish.
const CANCEL_WAIT_MS = 5000
const MAX_WORKFLOWS = 1000

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve()
    const t = setTimeout(resolve, ms)
    signal?.addEventListener("abort", () => { clearTimeout(t); resolve() }, { once: true })
  })
}
function anySignal(...signals: AbortSignal[]): AbortSignal {
  const c = new AbortController()
  for (const s of signals) {
    if (s.aborted) { c.abort(s.reason); break }
    s.addEventListener("abort", () => c.abort(s.reason), { once: true })
  }
  return c.signal
}
function env(name: string): string | undefined {
  try { return (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.[name] } catch { return undefined }
}
function hostName(): string {
  const h = env("HOSTNAME") ?? env("COMPUTERNAME") ?? "worker"
  return h.toLowerCase().replace(/[^a-z0-9-]+/g, "-").slice(0, 40) || "worker"
}
function retryAfterMs(res: Response): number | undefined {
  const v = res.headers.get("Retry-After")
  if (!v) return undefined
  const n = Number(v)
  if (Number.isFinite(n) && n >= 0) return n * 1000
  const at = Date.parse(v)
  return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now())
}
function errorCode(text: string): string | undefined {
  try { const c = (JSON.parse(text) as { error?: { code?: unknown } }).error?.code; return typeof c === "string" ? c : undefined } catch { return undefined }
}

// devQueue: dev-<user> in lowercase letters, digits and - (the user given, else $USER, $USERNAME
// or $LOGNAME, else "dev").
export function devQueue(user?: string): string {
  const raw = user || env("USER") || env("USERNAME") || env("LOGNAME") || "dev"
  const clean = raw.toLowerCase().replace(/[^a-z0-9]+/g, "-").slice(0, 58).replace(/^-+|-+$/g, "")
  return `dev-${clean || "dev"}`
}

export function worker(opts: WorkerOptions): Worker {
  for (const k of ["organizationId", "projectId", "app"] as const)
    if (typeof opts[k] !== "string" || opts[k] === "") throw new TypeError(`worker() needs ${k}`)
  const byName = new Map<string, Workflow<any, any>>()
  const announce: WorkflowInfo[] = []
  for (const wf of opts.workflows ?? []) {
    const d = definitionOf(wf)
    if (byName.has(d.name)) throw new TypeError(`Two workflows are named "${d.name}"`)
    byName.set(d.name, wf)
    announce.push({ name: d.name, ...(d.opts.retry ? { retry: d.opts.retry } : {}), ...(d.opts.timeoutMs ? { timeoutMs: d.opts.timeoutMs } : {}) })
  }
  if (byName.size === 0) throw new TypeError("worker() needs at least one workflow")
  // What an announcement may carry (internal/protocol MaxWorkflows): checked here, not at the first poll.
  if (byName.size > MAX_WORKFLOWS) throw new TypeError(`A worker serves at most ${MAX_WORKFLOWS} workflows; got ${byName.size}`)
  const queue = opts.dev ? devQueue(typeof opts.dev === "object" ? opts.dev.user : undefined) : (opts.queue ?? "default")
  if (!QUEUE.test(queue)) throw new TypeError(`A queue name matches ^[a-z0-9][a-z0-9-]{0,62}$; got "${queue}"`)
  const maxTasks = opts.maxTasks ?? 4
  if (!Number.isInteger(maxTasks) || maxTasks < 1 || maxTasks > 10) throw new TypeError("maxTasks is a whole number from 1 to 10")
  const concurrency = opts.concurrency ?? 10
  if (!Number.isInteger(concurrency) || concurrency < 1) throw new TypeError("concurrency is a whole number ≥ 1")
  const heartbeatMs = opts.heartbeatIntervalMs ?? 20_000
  // Well inside the engine's 60 s heartbeat timeout, so one lost heartbeat never costs the invocation.
  if (!Number.isFinite(heartbeatMs) || heartbeatMs <= 0 || heartbeatMs > MAX_HEARTBEAT_INTERVAL_MS)
    throw new TypeError(`heartbeatIntervalMs is a number from 1 to ${MAX_HEARTBEAT_INTERVAL_MS} (the engine's heartbeat timeout is 60 s)`)
  const backoffMax = opts.backoffMaxMs ?? 30_000
  if (!Number.isFinite(backoffMax) || backoffMax < 0) throw new TypeError("backoffMaxMs is a number ≥ 0")
  if (opts.maxStepsPerInvocation !== undefined && (!Number.isInteger(opts.maxStepsPerInvocation) || opts.maxStepsPerInvocation < 1))
    throw new TypeError("maxStepsPerInvocation is a whole number ≥ 1")
  const live = (opts.liveUrl ?? DEFAULT_LIVE).replace(/\/+$/, "")
  const pollUrl = `${live}/v1/organizations/${encodeURIComponent(opts.organizationId)}/projects/${encodeURIComponent(opts.projectId)}/workers/poll`
  const workerId = opts.workerId ?? `${hostName()}-${crypto.randomUUID().slice(0, 8)}`
  if (!WORKER_ID.test(workerId)) throw new TypeError(`A worker ID matches ^[A-Za-z0-9._:-]{1,128}$; got "${workerId}"`)
  const fetchImpl: FetchLike = opts.fetch ?? ((u, i) => fetch(u, i))
  const cred = keySourceFor(opts.apiKey, "worker()", fetchImpl)
  let reauthed = false // an instance key the API refused is replaced once
  const sdk = { name: SDK_NAME, version: SDK_VERSION }
  const deployment = opts.deployment !== undefined ? cleanDeployment(opts.deployment) : detectDeployment()
  const onError = opts.onError ?? (() => {})

  const active = new Set<Promise<void>>()
  const cancels = new Set<AbortController>()
  const stopped = new AbortController()
  let pollAbort: AbortController | undefined
  // The poll in flight and the handing of its invocations to handle(): stop() waits for it, so a
  // poll answer arriving as stop begins is run (within the grace period), never dropped.
  let cycle: Promise<void> | undefined
  let isRunning = false
  let release: () => void = () => {}
  const released = new Promise<void>((r) => (release = r))

  async function poll(want: number): Promise<{ invocations: unknown[]; receivedAt: number; serverDate?: number }> {
    pollAbort = new AbortController()
    const apiKey = await cred.key()
    const res = await fetchImpl(pollUrl, {
      method: "POST",
      headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ app: opts.app, queue, maxTasks: want, workerId, protocolVersions: [...PROTOCOL_VERSIONS], sdk, workflows: announce }),
      signal: anySignal(pollAbort.signal, AbortSignal.timeout(POLL_TIMEOUT_MS)), redirect: "manual",
    })
    // The answer has begun: the gateway may have started tasks for it, so stop() no longer aborts it.
    pollAbort = undefined
    const receivedAt = Date.now()
    if (res.status === 204) { await res.body?.cancel().catch(() => {}); return { invocations: [], receivedAt } }
    const text = await res.text()
    if (res.ok) {
      let body: { invocations?: unknown }
      try { body = JSON.parse(text) as { invocations?: unknown } } catch { throw new Retryable("workflows-live's poll answer isn't JSON") }
      const date = Date.parse(res.headers.get("Date") ?? "")
      return { invocations: Array.isArray(body.invocations) ? body.invocations : [], receivedAt, ...(Number.isNaN(date) ? {} : { serverDate: date }) }
    }
    const code = errorCode(text)
    if (res.status === 401 && !reauthed && cred.refused(apiKey)) { reauthed = true; throw new Retryable("workflows-live refused the instance key; fetching a new one") }
    if (res.status === 401 || res.status === 403)
      throw new WorkerAuthError(res.status, `workflows-live refused the poll (${res.status}${code ? ` ${code}` : ""}): check the API key and its workflows.workers.poll permission`)
    if (res.status === 429 || res.status >= 500) throw new Retryable(`workflows-live answered ${res.status}${code ? ` ${code}` : ""}`, retryAfterMs(res))
    throw new WorkerConfigError(res.status, code ?? "unknown", `workflows-live refused the poll: ${res.status}${code ? ` ${code}` : ""}` +
      (opts.dev && res.status === 400 ? ` (dev mode polls ${queue}: app must be your own pull app with that queue; Client.devApp() creates it)` : ""))
  }

  async function heartbeat(inv: Invocation, cancel: AbortController, superseded: () => void): Promise<void> {
    try {
      const res = await fetchImpl(`${inv.endpoints.live.replace(/\/+$/, "")}/v1/invocations/heartbeat`, {
        method: "POST", headers: { Authorization: `Bearer ${inv.token}`, "Content-Type": "application/json" }, body: "{}", signal: AbortSignal.timeout(10_000), redirect: "manual",
      })
      if (res.status === 409) {
        await res.body?.cancel().catch(() => {})
        superseded()
        if (!cancel.signal.aborted) cancel.abort(new CancelledError("This invocation was superseded"))
        return
      }
      if (!res.ok) {
        // 401/403: the invocation token was refused; 5xx: the engine is unreachable. The step keeps
        // running; without heartbeats the engine re-dispatches after its timeout.
        await res.body?.cancel().catch(() => {})
        onError(new Error(`workflows-live answered the heartbeat with ${res.status}`), "heartbeat")
        return
      }
      const body = (await res.json()) as HeartbeatResponse
      if (body?.cancel && !cancel.signal.aborted) cancel.abort(new CancelledError("The run is cancelling"))
    } catch (e) {
      onError(e, "heartbeat")
    }
  }

  async function complete(inv: Invocation, result: InvocationResult, deadline: number): Promise<void> {
    const url = `${inv.endpoints.live.replace(/\/+$/, "")}/v1/invocations/complete`
    for (let attempt = 0; ; attempt++) {
      const left = deadline + 30_000 - Date.now()
      if (left <= 0) throw new Error("No time left to complete the invocation; the engine re-dispatches it")
      let res: Response | undefined
      try {
        res = await fetchImpl(url, { method: "POST", headers: { Authorization: `Bearer ${inv.token}`, "Content-Type": "application/json" }, body: JSON.stringify(result), signal: AbortSignal.timeout(Math.min(30_000, left)), redirect: "manual" })
      } catch { /* network error: retried below */ }
      if (res) {
        await res.body?.cancel().catch(() => {})
        if (res.ok || res.status === 409) return // 409: superseded; the engine has moved on
        if (res.status !== 429 && res.status < 500) throw new Error(`workflows-live refused the result: ${res.status}`)
      }
      await sleep(Math.max(res ? (retryAfterMs(res) ?? 0) : 0, Math.min(5000, 200 * 2 ** attempt)))
    }
  }

  async function handle(raw: unknown, receivedAt: number, serverDate?: number): Promise<void> {
    let inv: Invocation
    try { inv = parseInvocation(JSON.stringify(raw)) } catch (e) { onError(e, "invalid invocation"); return }
    if (inv.kind === "sync") return
    const sent = Date.parse(inv.deadline)
    const deadline = serverDate === undefined ? sent : Math.min(sent, receivedAt + (sent - serverDate))
    const reserveMs = Math.max(5000, Math.max(0, deadline - receivedAt) * 0.1)
    const cancel = new AbortController()
    cancels.add(cancel)
    let superseded = false
    const beat = setInterval(() => { void heartbeat(inv, cancel, () => { superseded = true }) }, heartbeatMs)
    try {
      const wf = byName.get(inv.run!.workflow)
      const commands = wf
        ? await execute(inv, wf, httpLive(inv.endpoints.live, inv.token, fetchImpl, { deadline, reserveMs }), {
          deadline, cancelSignal: cancel.signal,
          ...(opts.apiBaseUrl ? { apiBaseUrl: opts.apiBaseUrl } : {}),
          ...(opts.maxStepsPerInvocation ? { maxStepsPerInvocation: opts.maxStepsPerInvocation } : {}),
        })
        : [{ type: "workflowNotFound" as const, workflow: inv.run!.workflow }]
      if (superseded) return
      await complete(inv, { commands, sdk, ...(deployment ? { deployment } : {}) }, deadline)
    } catch (e) {
      // CheckpointFailed or an SDK error: the engine's heartbeat and deadline timers re-dispatch.
      if (!(e instanceof InvocationSuperseded)) onError(e, `invocation ${inv.invocationId}`)
    } finally {
      clearInterval(beat)
      cancels.delete(cancel)
    }
  }

  // drain waits up to graceMs for running invocations, then cancels their steps (each step's signal
  // aborts, and no new step starts) and waits at most 5 s more. What's still running is abandoned:
  // the engine's heartbeat and deadline timers re-dispatch it.
  // Its timers are cleared once the invocations settle, so a stopped worker keeps no process alive.
  async function drain(graceMs: number): Promise<void> {
    if (active.size === 0) return
    await settledWithin(graceMs)
    if (active.size === 0) return
    for (const c of cancels) if (!c.signal.aborted) c.abort(new CancelledError("The worker is stopping"))
    await settledWithin(CANCEL_WAIT_MS)
  }
  async function settledWithin(ms: number): Promise<void> {
    const timer = new AbortController()
    try {
      await Promise.race([Promise.allSettled([...active]), sleep(ms, timer.signal)])
    } finally {
      timer.abort()
    }
  }

  return {
    async run() {
      if (isRunning) throw new Error("This worker is already running")
      isRunning = true
      let failures = 0
      try {
        while (!stopped.signal.aborted) {
          while (active.size >= concurrency && !stopped.signal.aborted) await Promise.race([...active, released])
          if (stopped.signal.aborted) break
          let done!: () => void
          cycle = new Promise<void>((r) => (done = r))
          try {
            let got
            try {
              got = await poll(Math.min(maxTasks, concurrency - active.size))
            } catch (e) {
              if (e instanceof WorkerAuthError || e instanceof WorkerConfigError) {
                // Fatal: stop polling, let running invocations finish (stop()'s grace), then reject.
                stopped.abort()
                await drain(DEFAULT_GRACE_MS)
                throw e
              }
              if (stopped.signal.aborted) break
              onError(e, "poll")
              const backoff = Math.min(backoffMax, 500 * 2 ** Math.min(failures++, 16)) * (0.5 + Math.random() / 2)
              await sleep(Math.max(e instanceof Retryable ? (e.retryAfterMs ?? 0) : 0, backoff), stopped.signal)
              continue
            }
            failures = 0
            reauthed = false
            // Even when stop() has begun: these tasks are started, and run within its grace period.
            for (const raw of got.invocations) {
              const p: Promise<void> = handle(raw, got.receivedAt, got.serverDate).finally(() => active.delete(p))
              active.add(p)
            }
          } finally {
            done()
          }
        }
        await Promise.race([Promise.allSettled([...active]), released])
      } finally {
        isRunning = false
      }
    },
    async stop({ graceMs = DEFAULT_GRACE_MS } = {}) {
      stopped.abort()
      pollAbort?.abort() // a poll still waiting for its answer
      await cycle?.catch(() => {})
      await drain(graceMs)
      release()
    },
  }
}
