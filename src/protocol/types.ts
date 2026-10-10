// Wire types: exactly workflows-stack's internal/protocol JSON (design, "The execution protocol").
// Optional fields are omitted, never null, except Invocation.cancel (null when absent) and
// stepCompleted's output (always present, possibly null).
export interface RetryPolicy { maxAttempts: number; initialMs: number; maxMs: number; multiplier: number }
export interface WireError { code: string; name: string; message: string; data?: unknown; stack?: string; cause?: WireError }
export interface JournalEntry {
  seq: number
  stepId: string
  name: string
  kind: "run" | "sleep" | "value" | "callback" | "invoke" | (string & {})
  status: "running" | "retrying" | "completed" | "failed" | "cancelled" | "sleeping" | "waiting" | (string & {})
  attempt: number
  output?: unknown
  outputRef?: string
  error?: WireError
  // A callback entry's token (protocol 1.1): replays rebuild the same URL from it. A credential.
  token?: string
}
export interface Journal { complete: boolean; entries: JournalEntry[] }
// A scheduled run's schedule and scheduled time (the cron instant, not the jittered fire time).
export interface ScheduleInfo { id: string; scheduledTime: string }
export interface RunInfo { id: string; runKey?: string; workflow: string; input?: unknown; inputRef?: string; attempt: number; startedAt: string; schedule?: ScheduleInfo }
export interface Cancel { requestedAt: string; graceUntil: string }
// api: the public API base, for callback URLs (protocol 1.1).
export interface Endpoints { live: string; api?: string }
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
// One step of a planParallel command: the engine runs each in its own step invocation.
export interface PlannedStep { stepId: string; name: string; retry: RetryPolicy; timeoutMs?: number }
export type Command =
  | { type: "stepStarted"; stepId: string; name: string; attempt: number; retry: RetryPolicy; timeoutMs?: number }
  | { type: "stepCompleted"; stepId: string; name: string; output: unknown }
  | { type: "stepFailed"; stepId: string; name: string; attempt: number; error: WireError; retryable: boolean; retryAfterMs?: number }
  | { type: "valueRecorded"; stepId: string; name: string; value: unknown }
  | { type: "sleep"; stepId: string; name: string; until: string }
  | { type: "callbackCreated"; stepId: string; name: string; token: string; timeoutMs: number }
  | { type: "waitForCallback"; stepId: string }
  | { type: "invoke"; stepId: string; name: string; workflow: string; input?: unknown }
  | { type: "planParallel"; steps: PlannedStep[] }
  | { type: "runCompleted"; output: unknown }
  | { type: "runFailed"; error: WireError; causeStepId?: string }
  | { type: "workflowNotFound"; workflow: string }
export interface Checkpoint { invocationId: string; seq: number; commands: Command[] }
export interface SDKInfo { name: string; version: string }
export interface InvocationResult { commands: Command[]; deployment?: string; sdk: SDKInfo }
export interface WorkflowInfo { name: string; retry?: RetryPolicy; timeoutMs?: number }
export interface SyncReply { protocolVersions: string[]; sdk: SDKInfo; maxInvocationSeconds: number; workflows: WorkflowInfo[]; deployment?: string }
// Pull workers (protocol 1.1): a poll on workflows-live, its answer, and a heartbeat's answer.
export interface PollRequest { app: string; queue: string; maxTasks: number; workerId: string; protocolVersions: string[]; sdk: SDKInfo; workflows?: WorkflowInfo[] }
export interface PollResponse { invocations: Invocation[] }
export interface HeartbeatResponse { cancel: Cancel | null }
