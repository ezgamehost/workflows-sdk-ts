// What the engine accepts from an SDK (workflows-stack internal/protocol validate.go and
// protocol/schemas), checked on this side: the SDK's tests run every message it sends through
// these, against workflows-stack's vectors (protocol/vectors/messages.json). Internal.
import { utf8Length } from "./bytes.ts"
import { parseCallbackToken } from "./callback.ts"

const MAX_RETRY_ATTEMPTS = 100
const MAX_RETRY_MS = 86_400_000
const MAX_MULTIPLIER = 10
const MAX_STEP_NAME_BYTES = 256
const MAX_STACK_BYTES = 8192
const MAX_ERROR_TEXT_BYTES = 8192
const MAX_ERROR_DATA_BYTES = 64 << 10
export const MAX_ERROR_DEPTH = 8
const MAX_VALUE_BYTES = 64 << 10
const MAX_DEPLOYMENT_BYTES = 256
export const MAX_PLANNED_STEPS = 1000
const MAX_POLL_TASKS = 10
const MAX_WORKFLOWS = 1000
const MAX_SDK_NAME_BYTES = 200
const MAX_SDK_VERSION_BYTES = 100

const ERROR_CODE = /^(user|workflows\.[a-z_]+)$/
const STEP_ID = /^[A-Za-z0-9._:-]{1,128}$/
const PAYLOAD_ID = /^pay_[a-z2-7]{26}$/
const INVOCATION_ID = /^inv_[a-z2-7]{26}$/
const WORKFLOW = /^[A-Za-z0-9_-]{1,64}$/
export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const QUEUE = /^[a-z0-9][a-z0-9-]{0,62}$/
const WORKER_ID = /^[A-Za-z0-9._:-]{1,128}$/
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/
const COMMAND_TYPES = new Set([
  "stepStarted", "stepCompleted", "stepFailed", "valueRecorded", "sleep", "runCompleted", "runFailed",
  "workflowNotFound", "callbackCreated", "waitForCallback", "invoke", "planParallel", "messageWaitCreated", "waitForMessage",
])
// Message waits (protocol 1.2): the queue's ID, the key, and the timeout (1 s to 30 days).
export const MESSAGE_KEY = /^[A-Za-z0-9_.:-]{1,256}$/
export const MIN_MESSAGE_TIMEOUT_MS = 1000
export const MAX_MESSAGE_TIMEOUT_MS = 2_592_000_000

export class InvalidMessage extends Error { override name = "InvalidMessage" }
const fail = (msg: string): never => { throw new InvalidMessage(msg) }
type Obj = Record<string, unknown>
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v)
const isInt = (v: unknown): v is number => typeof v === "number" && Number.isInteger(v)
const str = (v: unknown): string => (typeof v === "string" ? v : "")
const jsonBytes = (v: unknown) => utf8Length(JSON.stringify(v))

export function validateRetry(p: unknown): void {
  if (!isObj(p)) fail("retry isn't an object")
  const r = p as Obj
  if (!isInt(r.maxAttempts) || r.maxAttempts < 1 || r.maxAttempts > MAX_RETRY_ATTEMPTS) fail(`retry.maxAttempts ${String(r.maxAttempts)}`)
  if (!isInt(r.initialMs) || r.initialMs < 0) fail(`retry.initialMs ${String(r.initialMs)}`)
  if (!isInt(r.maxMs) || r.maxMs < 0 || r.maxMs > MAX_RETRY_MS) fail(`retry.maxMs ${String(r.maxMs)}`)
  if (typeof r.multiplier !== "number" || Number.isNaN(r.multiplier) || r.multiplier < 1 || r.multiplier > MAX_MULTIPLIER) fail(`retry.multiplier ${String(r.multiplier)}`)
}

export function validateError(e: unknown): void {
  let cur: unknown = e
  for (let depth = 0; cur !== undefined; depth++) {
    if (depth >= MAX_ERROR_DEPTH) fail(`error causes nest deeper than ${MAX_ERROR_DEPTH}`)
    if (!isObj(cur)) fail("error isn't an object")
    const w = cur as Obj
    if (!ERROR_CODE.test(str(w.code))) fail(`error code ${JSON.stringify(w.code)} isn't user or workflows.*`)
    if (typeof w.name !== "string" || typeof w.message !== "string") fail("error needs name and message")
    if (utf8Length(str(w.name)) > MAX_ERROR_TEXT_BYTES || utf8Length(str(w.message)) > MAX_ERROR_TEXT_BYTES) fail("error name or message too long")
    if (w.stack !== undefined && (typeof w.stack !== "string" || utf8Length(w.stack) > MAX_STACK_BYTES)) fail("error stack too long")
    if (w.data !== undefined && jsonBytes(w.data) > MAX_ERROR_DATA_BYTES) fail("error data too long")
    cur = w.cause
  }
}

const validName = (n: unknown) => typeof n === "string" && n !== "" && utf8Length(n) <= MAX_STEP_NAME_BYTES && !n.includes("\u0000")

export function validateCommand(cmd: unknown): void {
  if (!isObj(cmd)) fail("command isn't an object")
  const c = cmd as Obj
  const type = str(c.type)
  if (!COMMAND_TYPES.has(type)) fail(`unknown command type ${JSON.stringify(c.type)}`)
  if (c.stepId !== undefined && !STEP_ID.test(str(c.stepId))) fail(`stepId ${JSON.stringify(c.stepId)}`)
  if (c.name !== undefined && (typeof c.name !== "string" || utf8Length(c.name) > MAX_STEP_NAME_BYTES || c.name.includes("\u0000"))) fail("name")
  if (c.causeStepId !== undefined && !STEP_ID.test(str(c.causeStepId))) fail("causeStepId")
  if (c.value !== undefined && jsonBytes(c.value) > MAX_VALUE_BYTES) fail("value too long")
  if (c.attempt !== undefined && (!isInt(c.attempt) || c.attempt < 0)) fail("attempt")
  if (c.retry !== undefined) validateRetry(c.retry)
  if (c.timeoutMs !== undefined && (!isInt(c.timeoutMs) || c.timeoutMs < 0)) fail("timeoutMs")
  if (c.retryAfterMs !== undefined && (!isInt(c.retryAfterMs) || c.retryAfterMs < 0)) fail("retryAfterMs")
  if (c.outputRef !== undefined && !PAYLOAD_ID.test(str(c.outputRef))) fail("outputRef")
  if (c.error !== undefined) validateError(c.error)
  if (c.workflow !== undefined && !WORKFLOW.test(str(c.workflow))) fail(`workflow ${JSON.stringify(c.workflow)}`)
  if (c.token !== undefined) {
    if (type !== "callbackCreated") fail(`token on ${type}`)
    try { parseCallbackToken(str(c.token)) } catch { fail("token isn't a callback token") }
  }
  if (c.inputRef !== undefined && !PAYLOAD_ID.test(str(c.inputRef))) fail("inputRef")
  if (c.input !== undefined && c.inputRef !== undefined) fail("input and inputRef are exclusive")
  if ((c.input !== undefined || c.inputRef !== undefined) && type !== "invoke") fail(`input on ${type}`)
  if (c.steps !== undefined && type !== "planParallel") fail(`steps on ${type}`)
  if ((c.queueId !== undefined || c.key !== undefined) && type !== "messageWaitCreated") fail(`queueId or key on ${type}`)
  const step = () => { if (!STEP_ID.test(str(c.stepId)) || !validName(c.name)) fail(`${type} needs stepId and name`) }
  switch (type) {
    case "stepStarted": case "stepFailed":
      step()
      if (!isInt(c.attempt) || c.attempt < 1) fail(`${type} needs attempt ≥ 1`)
      if (type === "stepStarted" && c.retry === undefined) fail("stepStarted needs retry")
      if (type === "stepFailed" && (c.error === undefined || typeof c.retryable !== "boolean")) fail("stepFailed needs error and retryable")
      break
    case "stepCompleted":
      step()
      if (!("output" in c) && c.outputRef === undefined) fail("stepCompleted needs output")
      break
    case "valueRecorded":
      step()
      if (!("value" in c)) fail("valueRecorded needs value")
      break
    case "sleep":
      step()
      if (!RFC3339.test(str(c.until)) || Number.isNaN(Date.parse(str(c.until)))) fail("sleep needs until (RFC 3339)")
      break
    case "runCompleted":
      if (!("output" in c) && c.outputRef === undefined) fail("runCompleted needs output")
      break
    case "runFailed":
      if (c.error === undefined) fail("runFailed needs error")
      break
    case "workflowNotFound":
      if (c.workflow === undefined) fail("workflowNotFound needs workflow")
      break
    case "callbackCreated":
      step()
      if (c.token === undefined || !isInt(c.timeoutMs) || c.timeoutMs < 1) fail("callbackCreated needs token and timeoutMs ≥ 1")
      break
    case "waitForCallback": case "waitForMessage":
      if (!STEP_ID.test(str(c.stepId))) fail(`${type} needs stepId`)
      break
    case "messageWaitCreated":
      step()
      if (!UUID.test(str(c.queueId))) fail("messageWaitCreated needs queueId (a UUID)")
      if (!MESSAGE_KEY.test(str(c.key))) fail("messageWaitCreated needs key (^[A-Za-z0-9_.:-]{1,256}$)")
      if (!isInt(c.timeoutMs) || c.timeoutMs < MIN_MESSAGE_TIMEOUT_MS || c.timeoutMs > MAX_MESSAGE_TIMEOUT_MS) fail("messageWaitCreated needs timeoutMs from 1,000 to 2,592,000,000")
      break
    case "invoke":
      step()
      if (c.workflow === undefined) fail("invoke needs workflow")
      break
    case "planParallel": {
      const steps = c.steps
      if (!Array.isArray(steps) || steps.length === 0 || steps.length > MAX_PLANNED_STEPS) fail(`planParallel needs 1-${MAX_PLANNED_STEPS} steps`)
      const seen = new Set<string>()
      for (const [i, s] of (steps as unknown[]).entries()) {
        if (!isObj(s) || !STEP_ID.test(str(s.stepId)) || !validName(s.name)) fail(`steps[${i}]: stepId and name`)
        const p = s as Obj
        if (seen.has(str(p.stepId))) fail(`steps[${i}]: stepId twice`)
        seen.add(str(p.stepId))
        if (p.retry !== undefined) validateRetry(p.retry)
        if (p.timeoutMs !== undefined && (!isInt(p.timeoutMs) || p.timeoutMs < 0)) fail(`steps[${i}]: timeoutMs`)
      }
    }
  }
}

function validateSDK(s: unknown): void {
  if (!isObj(s)) fail("sdk is required")
  const sdk = s as Obj
  if (str(sdk.name).trim() === "" || str(sdk.version).trim() === "") fail("sdk name and version are required")
  if (utf8Length(str(sdk.name)) > MAX_SDK_NAME_BYTES || utf8Length(str(sdk.version)) > MAX_SDK_VERSION_BYTES) fail("sdk name or version too long")
}

function validateWorkflows(ws: unknown): void {
  if (ws === undefined) return
  if (!Array.isArray(ws) || ws.length > MAX_WORKFLOWS) fail(`at most ${MAX_WORKFLOWS} workflows`)
  const seen = new Set<string>()
  for (const [i, w] of (ws as unknown[]).entries()) {
    if (!isObj(w) || !WORKFLOW.test(str(w.name)) || seen.has(str(w.name))) fail(`workflows[${i}].name is invalid or repeated`)
    const wf = w as Obj
    seen.add(str(wf.name))
    if (wf.timeoutMs !== undefined && (!isInt(wf.timeoutMs) || wf.timeoutMs < 0)) fail(`workflow ${str(wf.name)}: timeoutMs`)
    if (wf.retry !== undefined) validateRetry(wf.retry)
  }
}

export function validateCheckpoint(cp: unknown): void {
  if (!isObj(cp)) fail("checkpoint isn't an object")
  const c = cp as Obj
  if (!INVOCATION_ID.test(str(c.invocationId))) fail("invocationId")
  if (!isInt(c.seq) || c.seq < 1) fail("seq")
  if (!Array.isArray(c.commands)) fail("commands")
  ;(c.commands as unknown[]).forEach(validateCommand)
}

export function validateInvocationResult(r: unknown): void {
  if (!isObj(r)) fail("result isn't an object")
  const res = r as Obj
  if (!Array.isArray(res.commands)) fail("commands")
  validateSDK(res.sdk)
  if (res.deployment !== undefined && (typeof res.deployment !== "string" || utf8Length(res.deployment) > MAX_DEPLOYMENT_BYTES || res.deployment.includes("\u0000"))) fail("deployment")
  ;(res.commands as unknown[]).forEach(validateCommand)
}

export function validatePollRequest(r: unknown): void {
  if (!isObj(r)) fail("poll isn't an object")
  const p = r as Obj
  if (!UUID.test(str(p.app))) fail(`app ${JSON.stringify(p.app)}`)
  if (!QUEUE.test(str(p.queue))) fail(`queue ${JSON.stringify(p.queue)}`)
  if (!isInt(p.maxTasks) || p.maxTasks < 1 || p.maxTasks > MAX_POLL_TASKS) fail(`maxTasks ${String(p.maxTasks)}`)
  if (!WORKER_ID.test(str(p.workerId))) fail("workerId")
  if (!Array.isArray(p.protocolVersions) || p.protocolVersions.length === 0 || !p.protocolVersions.every((v) => /^\d+\.\d+$/.test(str(v)))) fail("protocolVersions")
  validateSDK(p.sdk)
  validateWorkflows(p.workflows)
}
