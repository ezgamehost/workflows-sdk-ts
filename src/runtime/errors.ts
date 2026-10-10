import type { WireError } from "../protocol/index.ts"

// A step error that retrying won't fix: the step fails at once (code workflows.non_retriable).
export class NonRetriableError extends Error { override name = "NonRetriableError" }
// A retryable step error with the delay before the next attempt.
export class RetryAfterError extends Error {
  override name = "RetryAfterError"
  constructor(message: string, readonly delayMs: number) { super(message) }
}
// Thrown into a workflow's pending call when its run is cancelled (code workflows.cancelled).
export class CancelledError extends Error { override name = "CancelledError" }
// Any other recorded error, rebuilt on replay with its wire code, name and data.
export class WorkflowError extends Error {
  code: string
  data?: unknown
  constructor(message: string, opts: { code?: string; name?: string; data?: unknown; cause?: unknown } = {}) {
    super(message, opts.cause === undefined ? undefined : { cause: opts.cause })
    this.code = opts.code ?? "user"
    this.name = opts.name ?? "WorkflowError"
    if (opts.data !== undefined) this.data = opts.data
  }
}
// Errors the runtime raises itself.
export class StepTimeout extends Error { override name = "StepTimeout" }
export class DuplicateStepName extends Error { override name = "DuplicateStepName" }
export class OutputTooLarge extends Error { override name = "OutputTooLarge" }

const MAX_STACK = 8192
const MAX_DEPTH = 8

function codeOf(e: Error): string {
  if (e instanceof NonRetriableError) return "workflows.non_retriable"
  if (e instanceof CancelledError) return "workflows.cancelled"
  if (e instanceof StepTimeout) return "workflows.step_timeout"
  if (e instanceof DuplicateStepName) return "workflows.duplicate_step_name"
  if (e instanceof OutputTooLarge) return "workflows.output_too_large"
  if (e instanceof WorkflowError) return e.code
  return "user"
}

function jsonData(v: unknown): unknown {
  try { return v === undefined ? undefined : JSON.parse(JSON.stringify(v)) } catch { return undefined }
}

// encodeError: the wire shape of anything a workflow throws (design, "Errors"). Causes nest at
// most 8 deep and stacks are cut at 8 KiB.
export function encodeError(e: unknown, depth = 0): WireError {
  if (!(e instanceof Error)) return { code: "user", name: "Error", message: typeof e === "string" ? e : safeString(e) }
  const w: WireError = { code: codeOf(e), name: e.name || "Error", message: String(e.message) }
  const data = jsonData((e as { data?: unknown }).data)
  if (data !== undefined) w.data = data
  if (typeof e.stack === "string" && e.stack) w.stack = e.stack.slice(0, MAX_STACK)
  if (e.cause !== undefined && depth < MAX_DEPTH) w.cause = encodeError(e.cause, depth + 1)
  return w
}

function safeString(v: unknown): string {
  try { return String(v) } catch { return "A non-Error value was thrown" }
}

// decodeError rebuilds the SDK's own classes from code, so instanceof works after replay.
// encodeError(decodeError(w)) gives back w.
export function decodeError(w: WireError): Error {
  const cause = w.cause ? decodeError(w.cause) : undefined
  let e: Error
  switch (w.code) {
    case "workflows.non_retriable": e = new NonRetriableError(w.message, cause ? { cause } : undefined); break
    case "workflows.cancelled": e = new CancelledError(w.message, cause ? { cause } : undefined); break
    default: e = new WorkflowError(w.message, { code: w.code, name: w.name, data: w.data, cause })
  }
  e.name = w.name
  if (w.data !== undefined) (e as { data?: unknown }).data = w.data
  if (w.stack !== undefined) e.stack = w.stack
  else delete (e as { stack?: string }).stack
  return e
}
