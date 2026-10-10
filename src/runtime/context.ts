import type { RetryPolicy } from "../protocol/index.ts"

// Jsonify<T> is T after a JSON round trip: what a step's result is, live and on replay.
export type Jsonify<T> =
  T extends Date ? string
  : T extends undefined ? null
  : T extends (infer U)[] ? Jsonify<U>[]
  : T extends object ? { [K in keyof T]: Jsonify<T[K]> }
  : T
export interface StepRunOptions { retry?: Partial<RetryPolicy>; timeoutMs?: number; id?: string }
// What a step function is called with: signal aborts when the step passes its timeoutMs.
export interface StepRunContext { signal: AbortSignal; attempt: number; idempotencyKey: string }
export interface Step {
  // Runs fn at least once and records its JSON result; on replay the result returns without running fn.
  // Step names are 1–256 UTF-8 bytes; two calls pending at once need distinct names (or ids).
  run<T>(name: string, fn: (ctx: StepRunContext) => Promise<T> | T, opts?: StepRunOptions): Promise<Jsonify<T>>
  // Durable sleeps (at most a year): the invocation ends and EZGH invokes the workflow again when
  // the timer fires.
  sleep(name: string, duration: string | number, opts?: { id?: string }): Promise<void>
  sleepUntil(name: string, at: Date | string, opts?: { id?: string }): Promise<void>
  // Recorded values: the same on every replay.
  now(name: string): Promise<Date>
  uuid(name: string): Promise<string>
  // Inside step.run: `${runId}:${stepId}:${attempt}`, for idempotent calls to other systems.
  idempotencyKey(): string
}
export interface WorkflowContext<I> { input: I; run: { id: string; runKey?: string; attempt: number }; step: Step }
