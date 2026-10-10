// Wire types: exactly workflows-stack's internal/protocol JSON (design, "The execution protocol").
// Optional fields are omitted, never null, except Invocation.cancel (null when absent) and
// stepCompleted's output (always present, possibly null).
export interface RetryPolicy { maxAttempts: number; initialMs: number; maxMs: number; multiplier: number }
export interface WireError { code: string; name: string; message: string; data?: unknown; stack?: string; cause?: WireError }
export interface JournalEntry {
  seq: number
  stepId: string
  name: string
  kind: "run" | "sleep" | "value" | (string & {})
  status: "running" | "retrying" | "completed" | "failed" | "cancelled" | "sleeping" | (string & {})
  attempt: number
  output?: unknown
  outputRef?: string
  error?: WireError
}
export interface Journal { complete: boolean; entries: JournalEntry[] }
export interface RunInfo { id: string; runKey?: string; workflow: string; input?: unknown; inputRef?: string; attempt: number; startedAt: string }
export interface Cancel { requestedAt: string; graceUntil: string }
export interface Endpoints { live: string }
export interface Invocation {
  protocolVersion: string
  kind: "orchestrate" | "step" | "sync"
  invocationId: string
  token: string
  endpoints: Endpoints
  run?: RunInfo
  journal?: Journal
  targetStepId?: string
  deadline: string
  cancel: Cancel | null
}
export type Command =
  | { type: "stepStarted"; stepId: string; name: string; attempt: number; retry: RetryPolicy; timeoutMs?: number }
  | { type: "stepCompleted"; stepId: string; name: string; output: unknown }
  | { type: "stepFailed"; stepId: string; name: string; attempt: number; error: WireError; retryable: boolean; retryAfterMs?: number }
  | { type: "valueRecorded"; stepId: string; name: string; value: unknown }
  | { type: "sleep"; stepId: string; name: string; until: string }
  | { type: "runCompleted"; output: unknown }
  | { type: "runFailed"; error: WireError; causeStepId?: string }
  | { type: "workflowNotFound"; workflow: string }
export interface Checkpoint { invocationId: string; seq: number; commands: Command[] }
export interface SDKInfo { name: string; version: string }
export interface InvocationResult { commands: Command[]; deployment?: string; sdk: SDKInfo }
export interface WorkflowInfo { name: string; retry?: RetryPolicy; timeoutMs?: number }
export interface SyncReply { protocolVersions: string[]; sdk: SDKInfo; maxInvocationSeconds: number; workflows: WorkflowInfo[]; deployment?: string }
