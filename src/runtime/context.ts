import type { RetryPolicy } from "../protocol/index.ts"
import type { Workflow } from "./workflow.ts"

// Jsonify<T> is T after a JSON round trip: what a step's result is, live and on replay.
// Object fields that may be undefined become optional (JSON drops them); undefined in arrays
// becomes null.
export type Jsonify<T> =
  T extends Date ? string
  : T extends undefined ? null
  : T extends (infer U)[] ? Jsonify<U>[]
  : T extends object ? JsonifyObject<T>
  : T
type JsonifyObject<T> = Flatten<
  { [K in keyof T as undefined extends T[K] ? K : never]?: Jsonify<Exclude<T[K], undefined>> } &
  { [K in keyof T as undefined extends T[K] ? never : K]: Jsonify<T[K]> }
>
type Flatten<T> = { [K in keyof T]: T[K] }
export interface StepRunOptions { retry?: Partial<RetryPolicy>; timeoutMs?: number; id?: string }
// A callback (protocol 1.1): send url (or token) to whoever completes it. wait() suspends the run
// until it's completed (resolving with the JSON body) or times out (rejecting with
// CallbackTimeout). The URL is a credential: keep it private.
export interface Callback<T = unknown> { readonly url: string; readonly token: string; wait(): Promise<Jsonify<T>> }
// timeoutMs: default 7 days, from 1 s to a year.
export interface CreateCallbackOptions { timeoutMs?: number; id?: string }
// workflow: a workflow of the same app, by name or as workflow() returned it. input: JSON, at most 1 MiB.
// With a Workflow object, input is typed as that workflow's input; by name it's unknown.
export interface InvokeOptions<I = unknown, O = unknown> { workflow: string | Workflow<I, O>; input?: I; id?: string }
// What a step function is called with: signal aborts when the step passes its timeoutMs, or when
// the run is cancelling (a pull worker's heartbeat says so), with a CancelledError reason.
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
  // A callback (protocol 1.1): the token is created and recorded before url is visible; wait()
  // suspends until it's completed (POST url, or Client.completeCallback) or timeoutMs passes.
  createCallback<T = unknown>(name: string, opts?: CreateCallbackOptions): Promise<Callback<T>>
  // Starts a child run of a workflow in the same app and waits for it (protocol 1.1); resolves
  // with its output, or rejects with ChildFailed (cause: the child's error).
  invoke<I = unknown, O = unknown>(name: string, opts: InvokeOptions<I, O>): Promise<Jsonify<O>>
  // Inside step.run: `${runId}:${stepId}:${attempt}`, for idempotent calls to other systems.
  idempotencyKey(): string
}
export interface WorkflowContext<I> {
  input: I
  // schedule: set on a run a schedule started ({id, scheduledTime}, the cron instant).
  run: { id: string; runKey?: string; attempt: number; schedule?: { id: string; scheduledTime: string } }
  step: Step
}
