import type { RetryPolicy, SDKInfo, WireError } from "./protocol/index.ts"
import type { FetchLike } from "./runtime/live.ts"
import { devQueue } from "./worker.ts"

// Open unions: a later API may add values, so handle the unknown case.
export type RunStatus = "running" | "cancelling" | "completed" | "failed" | "cancelled" | "timedOut" | (string & {})
export interface RunStep {
  seq: number; stepId: string; name: string; kind: string; status: string; attempt: number; hasOutput: boolean
  error: WireError | null
}
export interface Run {
  id: string; runKey: string | null; appId: string; workflow: string; status: RunStatus; attempt: number
  error: WireError | null; causeStepId: string | null
  steps: RunStep[]
  createdAt: string; updatedAt: string; endedAt: string | null
}
export interface RunEvent {
  id: string; at: string; type: string; stepId: string | null; invocationId: string | null; deployment: string | null; detail: unknown
}
export interface AppWorkflow { name: string; retry?: RetryPolicy; timeoutMs?: number }
export interface App {
  id: string; slug: string; resourceName: string; organizationId: string; projectId: string
  name: string; kind: "push" | "pull" | (string & {}); endpoint: string | null
  syncStatus: "unsynced" | "synced" | "failed" | (string & {})
  workflows: AppWorkflow[]; maxInvocationSeconds: number | null; protocolVersion: string | null
  sdk: SDKInfo | null; deployment: string | null; health: string; lastSyncAt: string | null
  createdAt: string; updatedAt: string
  // A pull app's task queue and partition count; null for push apps.
  queue?: string | null; partitions?: number | null
}
// Schedules: cron schedules that start runs of one workflow. A schedule never shows its input
// (hasInput says whether it has one; scheduleInput reads it).
export interface Schedule {
  id: string; resourceName: string; appId: string; workflow: string; name: string
  cron: string; timeZone: string; overlap: "skip" | "allow" | "bufferOne" | (string & {})
  jitterSeconds: number; catchupWindowSeconds: number; paused: boolean; revision: number
  nextFireAt: string | null; nextScheduledTime: string | null; lastScheduledTime: string | null; activeRunId: string | null
  missed: number; skipped: number; hasInput: boolean; createdAt: string; updatedAt: string
}
// A schedule in a list (from the project's index, about a second behind).
export interface ScheduleSummary {
  id: string; resourceName: string; appId: string; workflow: string; name: string
  cron: string; timeZone: string; paused: boolean; revision: number; updatedAt: string
}
export interface ScheduleInput {
  appId: string; workflow: string; name: string; cron: string; timeZone?: string; input?: unknown
  overlap?: "skip" | "allow" | "bufferOne"; jitterSeconds?: number; catchupWindowSeconds?: number; paused?: boolean
}
// What updateSchedule may change: anything but the app and paused (pause and resume do that).
export type ScheduleUpdate = Partial<Omit<ScheduleInput, "appId" | "paused">>
// A new or rotated signing secret is returned once; an Idempotency-Key replay answers null.
export interface AppWithSecret { app: App; signingSecret: string | null }

// An error answer from the Workflows API: the HTTP status and the platform error code. Status 0
// with code "outcome_unknown": the request may or may not have taken effect (a network error on
// createApp or rotateSecret, which aren't resent; see there).
export class WorkflowsApiError extends Error {
  override name = "WorkflowsApiError"
  constructor(readonly status: number, readonly code: string, message: string, readonly retryAfterSeconds?: number) { super(message) }
}

export interface ClientOptions {
  // An EZGH API key (or other bearer credential) with the workflows actions it uses.
  apiKey: string
  organizationId: string
  projectId: string
  baseUrl?: string
  fetch?: FetchLike
  // How many times a 429, 503 or network error is retried (default 3), after Retry-After (at
  // most maxRetryDelayMs, default 10 s). Writes are resent with the same Idempotency-Key.
  retries?: number
  maxRetryDelayMs?: number
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const id = encodeURIComponent

// Client calls the Workflows API for one project: apps (create, sync, rotate the secret) and runs
// (start, inspect, cancel, retry, payloads, events).
export class Client {
  // ES private fields: the API key never shows in console.log, util.inspect or JSON.stringify.
  readonly #apiKey: string
  readonly #base: string
  readonly #fetch: FetchLike
  readonly #retries: number
  readonly #maxDelay: number
  constructor(opts: ClientOptions) {
    this.#apiKey = opts.apiKey
    this.#base = `${(opts.baseUrl ?? "https://workflows-api.ezghcloud.com").replace(/\/+$/, "")}/v1/organizations/${id(opts.organizationId)}/projects/${id(opts.projectId)}`
    this.#fetch = opts.fetch ?? ((u, i) => fetch(u, i))
    this.#retries = opts.retries ?? 3
    this.#maxDelay = opts.maxRetryDelayMs ?? 10_000
    if (!Number.isInteger(this.#retries) || this.#retries < 0) throw new TypeError("retries is a whole number ≥ 0")
    if (!Number.isFinite(this.#maxDelay) || this.#maxDelay < 0) throw new TypeError("maxRetryDelayMs is a number ≥ 0")
  }

  // call sends one request, retrying 429, 503 and (unless resendOnNetworkError is false) network
  // errors with the same Idempotency-Key.
  private async call<T>(method: string, path: string, body?: unknown, idempotencyKey?: string, resendOnNetworkError = true, extraHeaders: Record<string, string> = {}): Promise<T> {
    const headers: Record<string, string> = { Authorization: `Bearer ${this.#apiKey}`, Accept: "application/json", ...extraHeaders }
    if (body !== undefined) headers["Content-Type"] = "application/json"
    if (method !== "GET") headers["Idempotency-Key"] = idempotencyKey ?? crypto.randomUUID()
    const retries = this.#retries
    const maxDelay = this.#maxDelay
    for (let attempt = 0; ; attempt++) {
      let res: Response
      try {
        res = await this.#fetch(this.#base + path, { method, headers, ...(body !== undefined ? { body: JSON.stringify(body) } : {}) })
      } catch (e) {
        if (!resendOnNetworkError)
          throw new WorkflowsApiError(0, "outcome_unknown", `The request failed in transit (${e instanceof Error ? e.message : String(e)}), so it may have taken effect: a resend would return no secret. Check with getApp, and rotate the app's secret if you didn't receive it.`)
        if (attempt >= retries) throw e
        await sleep(Math.min(maxDelay, 500 * 2 ** attempt))
        continue
      }
      const text = await res.text()
      let data: unknown
      try { data = text ? JSON.parse(text) : undefined } catch { data = undefined }
      if (res.ok) return data as T
      const e = (data as { error?: { code?: string; message?: string } } | undefined)?.error
      const ra = Number(res.headers.get("Retry-After"))
      const err = new WorkflowsApiError(res.status, e?.code ?? "unknown", e?.message ?? `HTTP ${res.status}`, Number.isFinite(ra) && ra > 0 ? ra : undefined)
      if ((res.status !== 429 && res.status !== 503) || attempt >= retries) throw err
      await sleep(Math.min(maxDelay, (err.retryAfterSeconds ?? 1) * 1000))
    }
  }

  // Apps
  // createApp returns the app and its signing secret, once. A network error isn't resent (the
  // replay of a created app answers signingSecret: null): it throws outcome_unknown.
  createApp(name: string, opts: { endpoint?: string; kind?: "push" | "pull"; queue?: string; partitions?: number; idempotencyKey?: string } = {}): Promise<AppWithSecret> {
    const kind = opts.kind ?? "push"
    return this.call("POST", "/apps", { name, kind, ...(opts.endpoint ? { endpoint: opts.endpoint } : {}), ...(opts.queue ? { queue: opts.queue } : {}), ...(opts.partitions !== undefined ? { partitions: opts.partitions } : {}) }, opts.idempotencyKey, false)
  }
  getApp(appId: string): Promise<App> { return this.call("GET", `/apps/${id(appId)}`) }
  listApps(opts: { cursor?: string; limit?: number } = {}): Promise<{ apps: App[]; nextCursor: string | null }> {
    const q = new URLSearchParams()
    if (opts.cursor) q.set("cursor", opts.cursor)
    if (opts.limit !== undefined) q.set("limit", String(opts.limit))
    const qs = q.toString()
    return this.call("GET", `/apps${qs ? `?${qs}` : ""}`)
  }
  // devApp creates, or finds, a developer's own pull app for dev mode: named and queued
  // devQueue(user) (dev-<user>), so worker({ app: app.id, dev: true }) polls it. The live host
  // serves only an app's own queue, so each developer needs an app of their own.
  async devApp(opts: { user?: string; name?: string; partitions?: number } = {}): Promise<App> {
    const queue = devQueue(opts.user)
    const name = opts.name ?? queue
    try {
      return (await this.createApp(name, { kind: "pull", queue, ...(opts.partitions !== undefined ? { partitions: opts.partitions } : {}) })).app
    } catch (e) {
      // It exists (409 app_exists), or the create's outcome is unknown: find it by name.
      if (!(e instanceof WorkflowsApiError && ((e.status === 409 && e.code === "app_exists") || e.code === "outcome_unknown"))) throw e
    }
    for (let cursor: string | undefined; ;) {
      const page = await this.listApps({ limit: 100, ...(cursor ? { cursor } : {}) })
      const app = page.apps.find((a) => a.name === name)
      if (app) {
        if (app.kind !== "pull" || app.queue !== queue)
          throw new WorkflowsApiError(409, "app_exists", `The app "${name}" exists but isn't a pull app with the queue ${queue}; pass devApp({ name }) another name`)
        return app
      }
      if (!page.nextCursor) throw new WorkflowsApiError(404, "not_found", `The dev app "${name}" was neither created nor found`)
      cursor = page.nextCursor
    }
  }
  // sync asks EZGH to call the app's endpoint and record its workflows; run it after each deploy.
  // A failed sync is a 409 sync_failed.
  sync(appId: string, opts: { endpoint?: string; deployment?: string } = {}): Promise<App> {
    return this.call("POST", `/apps/${id(appId)}/sync`, opts)
  }
  // rotateSecret returns a new signing secret; the old ones stay valid for 24 hours.
  // A network error isn't resent, as for createApp (outcome_unknown).
  rotateSecret(appId: string): Promise<AppWithSecret> { return this.call("POST", `/apps/${id(appId)}/rotate-secret`, {}, undefined, false) }

  // Runs
  // start a run: 201 for a new run. With runKey, starting again with the same workflow and input
  // returns the same run, and different input is a 409 run_key_conflict.
  start(appId: string, workflow: string, input?: unknown, opts: { runKey?: string; idempotencyKey?: string } = {}): Promise<Run> {
    return this.call("POST", "/runs", { appId, workflow, ...(input !== undefined ? { input } : {}), ...(opts.runKey ? { runKey: opts.runKey } : {}) }, opts.idempotencyKey)
  }
  get(runId: string): Promise<Run> { return this.call("GET", `/runs/${id(runId)}`) }
  // cancel: the run is cancelling; its code gets gracePeriodSeconds (0–3600, default 300) to clean up.
  cancel(runId: string, opts: { gracePeriodSeconds?: number } = {}): Promise<Run> {
    return this.call("POST", `/runs/${id(runId)}/cancel`, opts.gracePeriodSeconds === undefined ? {} : { gracePeriodSeconds: opts.gracePeriodSeconds })
  }
  // retry a failed run from its failed step; completed steps aren't run again.
  retry(runId: string): Promise<Run> { return this.call("POST", `/runs/${id(runId)}/retry`, {}) }
  // The run's output (workflows.runs.getPayload); 409 run_not_completed until it completes.
  output(runId: string): Promise<unknown> { return this.call("GET", `/runs/${id(runId)}/payloads/output`) }
  input(runId: string): Promise<unknown> { return this.call("GET", `/runs/${id(runId)}/payloads/input`) }
  stepOutput(runId: string, stepId: string): Promise<unknown> { return this.call("GET", `/runs/${id(runId)}/steps/${id(stepId)}/output`) }
  events(runId: string, opts: { cursor?: string; limit?: number } = {}): Promise<{ events: RunEvent[]; nextCursor: string | null }> {
    const q = new URLSearchParams()
    if (opts.cursor) q.set("cursor", opts.cursor)
    if (opts.limit !== undefined) q.set("limit", String(opts.limit))
    const qs = q.toString()
    return this.call("GET", `/runs/${id(runId)}/events${qs ? `?${qs}` : ""}`)
  }

  // Schedules: cron schedules that start runs of one workflow (design-phase-2 "Schedules"), with
  // the workflows.schedules.* actions.
  createSchedule(input: ScheduleInput, opts: { idempotencyKey?: string } = {}): Promise<Schedule> { return this.call("POST", "/schedules", input, opts.idempotencyKey) }
  getSchedule(scheduleId: string): Promise<Schedule> { return this.call("GET", `/schedules/${id(scheduleId)}`) }
  listSchedules(opts: { cursor?: string; limit?: number } = {}): Promise<{ schedules: ScheduleSummary[]; nextCursor: string | null }> {
    const q = new URLSearchParams()
    if (opts.cursor) q.set("cursor", opts.cursor)
    if (opts.limit !== undefined) q.set("limit", String(opts.limit))
    const qs = q.toString()
    return this.call("GET", `/schedules${qs ? `?${qs}` : ""}`)
  }
  // updateSchedule changes the given fields. revision is the schedule's revision you read
  // (If-Match): a change since then answers 412.
  updateSchedule(scheduleId: string, patch: ScheduleUpdate, opts: { revision: number }): Promise<Schedule> {
    return this.call("PATCH", `/schedules/${id(scheduleId)}`, patch, undefined, true, { "If-Match": `"${opts.revision}"` })
  }
  pauseSchedule(scheduleId: string): Promise<Schedule> { return this.call("POST", `/schedules/${id(scheduleId)}/pause`, {}) }
  // resumeSchedule fires again from now, with no catch-up.
  resumeSchedule(scheduleId: string): Promise<Schedule> { return this.call("POST", `/schedules/${id(scheduleId)}/resume`, {}) }
  async deleteSchedule(scheduleId: string): Promise<void> { await this.call("DELETE", `/schedules/${id(scheduleId)}`) }
  // triggerSchedule starts a run now. The Idempotency-Key names the run, so the same key always
  // answers the same run (without one, each call starts a new run).
  triggerSchedule(scheduleId: string, opts: { idempotencyKey?: string } = {}): Promise<Run> { return this.call("POST", `/schedules/${id(scheduleId)}/trigger`, {}, opts.idempotencyKey) }
  // The input every run of the schedule gets (workflows.runs.getPayload); null when none.
  scheduleInput(scheduleId: string): Promise<unknown> { return this.call("GET", `/schedules/${id(scheduleId)}/payloads/input`) }

  // Callbacks
  // completeCallback completes a waiting callback as an authenticated caller
  // (workflows.callbacks.complete, in the workflows:runs scope) and answers the run. The first
  // completion wins: the same value again succeeds, a different one is 409 callback_completed,
  // and after the timeout or the run's end it's 410 callback_expired. The public alternative is
  // POSTing the value to the callback's URL.
  completeCallback(runId: string, stepId: string, value: unknown): Promise<Run> { return this.call("POST", `/runs/${id(runId)}/callbacks/${id(stepId)}`, value === undefined ? null : value) }
}
