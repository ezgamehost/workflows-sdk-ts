import type { RetryPolicy } from "../protocol/index.ts"
import { validWorkflowName } from "../protocol/index.ts"
import type { WorkflowContext } from "./context.ts"

// Defaults for the workflow's steps (reported to EZGH at sync).
export interface WorkflowOptions { retry?: RetryPolicy; timeoutMs?: number }
export interface Workflow<I = unknown, O = unknown> {
  readonly name: string
  readonly fn: (ctx: WorkflowContext<I>) => Promise<O>
  readonly opts: WorkflowOptions
}

export function workflow<I = unknown, O = unknown>(name: string, fn: (ctx: WorkflowContext<I>) => Promise<O>, opts: WorkflowOptions = {}): Workflow<I, O> {
  if (!validWorkflowName(name)) throw new TypeError(`Workflow names match ^[A-Za-z0-9_-]{1,64}$; got "${name}"`)
  if (typeof fn !== "function") throw new TypeError("A workflow needs a function")
  if (opts.retry !== undefined) checkRetry(opts.retry)
  if (opts.timeoutMs !== undefined) checkTimeout(opts.timeoutMs)
  return Object.freeze({ name, fn, opts: Object.freeze({ ...opts }) })
}

export function checkRetry(p: Partial<RetryPolicy>): void {
  const ok = (v: unknown, min: number, int: boolean) => v === undefined || (typeof v === "number" && Number.isFinite(v) && v >= min && (!int || Number.isInteger(v)))
  if (!ok(p.maxAttempts, 1, true) || !ok(p.initialMs, 0, true) || !ok(p.maxMs, 0, true) || !ok(p.multiplier, 1, false))
    throw new TypeError("A retry policy has integer maxAttempts ≥ 1, integer initialMs and maxMs ≥ 0, and multiplier ≥ 1")
}

export function checkTimeout(ms: number): void {
  if (typeof ms !== "number" || !Number.isInteger(ms) || ms <= 0) throw new TypeError("timeoutMs is a positive integer")
}
