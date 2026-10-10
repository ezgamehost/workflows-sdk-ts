import type { RetryPolicy } from "../protocol/index.ts"
import { DEFAULT_RETRY, validWorkflowName } from "../protocol/index.ts"
import type { WorkflowContext } from "./context.ts"

// Defaults for the workflow's steps (reported to EZGH at sync). A partial retry policy is merged
// with DEFAULT_RETRY.
export interface WorkflowOptions { retry?: Partial<RetryPolicy>; timeoutMs?: number }
export interface Workflow<I = unknown, O = unknown> {
  readonly name: string
  readonly fn: (ctx: WorkflowContext<I>) => Promise<O>
  readonly opts: { readonly retry?: RetryPolicy; readonly timeoutMs?: number }
}

export function workflow<I = unknown, O = unknown>(name: string, fn: (ctx: WorkflowContext<I>) => Promise<O>, opts: WorkflowOptions = {}): Workflow<I, O> {
  if (!validWorkflowName(name)) throw new TypeError(`Workflow names match ^[A-Za-z0-9_-]{1,64}$; got "${name}"`)
  if (typeof fn !== "function") throw new TypeError("A workflow needs a function")
  if (opts.timeoutMs !== undefined) checkTimeout(opts.timeoutMs)
  const retry = opts.retry === undefined ? undefined : mergeRetry(DEFAULT_RETRY, opts.retry)
  return Object.freeze({ name, fn, opts: Object.freeze({ ...(retry ? { retry: Object.freeze(retry) } : {}), ...(opts.timeoutMs !== undefined ? { timeoutMs: opts.timeoutMs } : {}) }) })
}

// mergeRetry overlays the defined fields of over on base and checks the result.
export function mergeRetry(base: RetryPolicy, over: Partial<RetryPolicy> | undefined): RetryPolicy {
  const merged = { ...base }
  for (const [k, v] of Object.entries(over ?? {})) if (v !== undefined && k in merged) (merged as Record<string, unknown>)[k] = v
  checkRetry(merged)
  return merged
}

// checkRetry enforces the protocol's bounds (protocol/schemas; the engine refuses others):
// maxAttempts 1–100, initialMs ≥ 0, maxMs 0–86,400,000, multiplier 1–10.
export function checkRetry(p: Partial<RetryPolicy>): void {
  const ok = (v: unknown, min: number, max: number, int: boolean) =>
    v === undefined || (typeof v === "number" && Number.isFinite(v) && v >= min && v <= max && (!int || Number.isInteger(v)))
  if (!ok(p.maxAttempts, 1, 100, true) || !ok(p.initialMs, 0, Number.MAX_SAFE_INTEGER, true) || !ok(p.maxMs, 0, 86_400_000, true) || !ok(p.multiplier, 1, 10, false))
    throw new TypeError("A retry policy has integer maxAttempts 1–100, integer initialMs ≥ 0, integer maxMs 0–86,400,000 and multiplier 1–10")
}

export function checkTimeout(ms: number): void {
  if (typeof ms !== "number" || !Number.isInteger(ms) || ms <= 0) throw new TypeError("timeoutMs is a positive integer")
}
