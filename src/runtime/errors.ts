import type { WireError } from "../protocol/index.ts"
import { truncateUtf8, utf8Length } from "../protocol/bytes.ts"

// A step error that retrying won't fix: the step fails at once (code workflows.non_retriable).
export class NonRetriableError extends Error { override name = "NonRetriableError" }
// A retryable step error with the delay before the next attempt.
export class RetryAfterError extends Error {
  override name = "RetryAfterError"
  constructor(message: string, readonly delayMs: number) { super(message) }
}
// Thrown into a workflow's pending call when its run is cancelled (code workflows.cancelled).
export class CancelledError extends Error { override name = "CancelledError" }
// WorkflowError is an error with a code and JSON data, and what a recorded failure is rebuilt as
// on replay. Codes on the wire are "user" or the platform's own "workflows.*" codes: any other
// code is sent as "user", with
// the custom code as the name (when no name was given) and as data.code (when data is absent or a
// plain object without a code), so a replay sees code "user" and finds the custom code there.
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

const MAX_STACK_BYTES = 8192
const MAX_MESSAGE_BYTES = 8192
const MAX_DATA_BYTES = 65_536
const MAX_DEPTH = 8
// The platform's own codes (internal/protocol); a customer error with any other code is sent as user.
const PLATFORM_CODES = new Set([
  "workflows.callback_timeout", "workflows.cancelled", "workflows.child_failed", "workflows.step_timeout",
  "workflows.invocation_lost", "workflows.non_retriable", "workflows.output_too_large", "workflows.duplicate_step_name",
  "workflows.workflow_not_found", "workflows.invocation_failed", "workflows.timed_out",
])
const wireCode = (code: string) => code === "user" || PLATFORM_CODES.has(code)
const isPlainObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v) && Object.getPrototypeOf(v) === Object.prototype

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

// encodeError: the wire shape of anything a workflow throws (design, "Errors"). Codes are "user"
// or "workflows.*" (see WorkflowError); messages and stacks are cut to 8 KiB of UTF-8 and data
// over 64 KiB of JSON is dropped; causes nest at most 8 deep.
export function encodeError(e: unknown, depth = 0): WireError {
  if (!(e instanceof Error)) return { code: "user", name: "Error", message: truncateUtf8(typeof e === "string" ? e : safeString(e), MAX_MESSAGE_BYTES) }
  let code = codeOf(e)
  let name = e.name || "Error"
  let data = jsonData((e as { data?: unknown }).data)
  if (!wireCode(code)) {
    if (e instanceof WorkflowError && name === "WorkflowError") name = code
    if (data === undefined) data = { code }
    else if (isPlainObject(data) && !("code" in data)) data = { ...data, code }
    code = "user"
  }
  const w: WireError = { code, name: truncateUtf8(String(name), 256), message: truncateUtf8(String(e.message), MAX_MESSAGE_BYTES) }
  if (data !== undefined && utf8Length(JSON.stringify(data)) <= MAX_DATA_BYTES) w.data = data
  if (typeof e.stack === "string" && e.stack) w.stack = truncateUtf8(e.stack, MAX_STACK_BYTES)
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
