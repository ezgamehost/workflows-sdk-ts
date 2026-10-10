import type { FetchLike } from "./runtime/live.ts"

export interface WireErrorSummary { code: string; name: string; message: string }
export interface RunStep { seq: number; stepId: string; name: string; kind: string; status: string; attempt: number; hasOutput: boolean; error?: WireErrorSummary | null }
export interface Run {
  id: string; runKey?: string | null; appId: string; workflow: string
  status: "running" | "cancelling" | "completed" | "failed" | "cancelled" | "timedOut"
  attempt: number; createdAt: string; updatedAt: string; endedAt?: string | null
  error?: WireErrorSummary | null; causeStepId?: string | null
  steps?: RunStep[]
}
export interface App { id: string; name: string; kind: "push" | "pull"; endpoint?: string | null; syncStatus: string; workflows: { name: string }[] }

// An error answer from the Workflows API: the HTTP status and the platform error code.
export class WorkflowsApiError extends Error {
  override name = "WorkflowsApiError"
  constructor(readonly status: number, readonly code: string, message: string) { super(message) }
}

export interface ClientOptions {
  // An EZGH API key (or other bearer credential) with the workflows actions it uses.
  apiKey: string
  organizationId: string
  projectId: string
  baseUrl?: string
  fetch?: FetchLike
}

// Client calls the Workflows API for one project: start, inspect, cancel and retry runs, read a
// run's output, and sync an app after a deploy.
export class Client {
  private base: string
  private f: FetchLike
  constructor(private opts: ClientOptions) {
    this.base = `${(opts.baseUrl ?? "https://workflows-api.ezghcloud.com").replace(/\/+$/, "")}/v1/organizations/${encodeURIComponent(opts.organizationId)}/projects/${encodeURIComponent(opts.projectId)}`
    this.f = opts.fetch ?? ((u, i) => fetch(u, i))
  }

  private async call<T>(method: string, path: string, body?: unknown, idempotencyKey?: string): Promise<T> {
    const headers: Record<string, string> = { Authorization: `Bearer ${this.opts.apiKey}`, Accept: "application/json" }
    if (body !== undefined) headers["Content-Type"] = "application/json"
    if (method === "POST") headers["Idempotency-Key"] = idempotencyKey ?? crypto.randomUUID()
    const res = await this.f(this.base + path, { method, headers, ...(body !== undefined ? { body: JSON.stringify(body) } : {}) })
    const text = await res.text()
    let data: unknown
    try { data = text ? JSON.parse(text) : undefined } catch { data = undefined }
    if (!res.ok) {
      const e = (data as { error?: { code?: string; message?: string } } | undefined)?.error
      throw new WorkflowsApiError(res.status, e?.code ?? "unknown", e?.message ?? `HTTP ${res.status}`)
    }
    return data as T
  }

  // start a run. With runKey, starting again with the same input returns the same run (200), and
  // different input is a 409 run_key_conflict.
  start(appId: string, workflow: string, input?: unknown, opts: { runKey?: string; idempotencyKey?: string } = {}): Promise<Run> {
    return this.call("POST", "/runs", { appId, workflow, ...(input !== undefined ? { input } : {}), ...(opts.runKey ? { runKey: opts.runKey } : {}) }, opts.idempotencyKey)
  }
  get(runId: string): Promise<Run> { return this.call("GET", `/runs/${encodeURIComponent(runId)}`) }
  cancel(runId: string): Promise<Run> { return this.call("POST", `/runs/${encodeURIComponent(runId)}/cancel`, {}) }
  retry(runId: string): Promise<Run> { return this.call("POST", `/runs/${encodeURIComponent(runId)}/retry`, {}) }
  // The run's output (needs workflows.runs.getPayload); an error until the run completes.
  output(runId: string): Promise<unknown> { return this.call("GET", `/runs/${encodeURIComponent(runId)}/payloads/output`) }
  // sync asks EZGH to call the app's endpoint and record its workflows; run it after each deploy.
  sync(appId: string, opts: { endpoint?: string; deployment?: string } = {}): Promise<App> {
    return this.call("POST", `/apps/${encodeURIComponent(appId)}/sync`, opts)
  }
}
