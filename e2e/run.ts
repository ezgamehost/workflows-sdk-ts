// The end-to-end driver: each check through the public API (Client) against a local
// workflows-stack; exits 1 at the first failure.
import { Client, WorkflowsApiError, type Run } from "@ezgh/workflows"
import { childRunId, scheduledRunId, triggeredRunId } from "../src/protocol/derived.ts"

const api = process.env.E2E_API ?? "http://127.0.0.1:6100"
const app = process.env.E2E_APP ?? "http://127.0.0.1:3200"
// A fresh engine leases its shards over about 30 s and answers 503 until then: retry generously.
const client = new Client({ apiKey: process.env.E2E_API_KEY!, organizationId: process.env.E2E_ORG!, projectId: process.env.E2E_PROJECT!, baseUrl: api, retries: 90, maxRetryDelayMs: 2000 })
const appId = process.env.E2E_APP_ID!

// An API error anywhere fails the run with its status and code (not just a stack trace).
process.on("uncaughtException", (e) => {
  if (e instanceof WorkflowsApiError) console.error(`FAILED: the API answered ${e.status} ${e.code}: ${e.message}`)
  else console.error("FAILED:", e)
  process.exit(1)
})
process.on("unhandledRejection", (e) => { throw e })

function check(cond: unknown, what: string, detail?: unknown): asserts cond {
  if (!cond) { console.error(`FAILED: ${what}`, JSON.stringify(detail, null, 2)); process.exit(1) }
}
// E2E_TIME_SCALE stretches every wait (not any check) for a slow, shared database: a local run
// against the remote test fixture sets it; CI's local services don't need it.
const scale = Math.max(1, Number(process.env.E2E_TIME_SCALE ?? "1") || 1)
async function until(id: string, done: (r: Run) => boolean, seconds = 60): Promise<Run> {
  const end = Date.now() + seconds * 1000 * scale
  let r = await client.get(id)
  while (!done(r) && Date.now() < end) { await new Promise((s) => setTimeout(s, 500)); r = await client.get(id) }
  check(done(r), `run ${r.workflow} reached the expected state (it's ${r.status})`, r)
  return r
}
const terminal = (r: Run) => ["completed", "failed", "cancelled", "timedOut"].includes(r.status)
const stepNamed = (r: Run, name: string) => r.steps?.find((s) => s.name === name)
const state = async () => (await (await fetch(`${app}/__e2e/state`)).json()) as { cleanup: number; prepare: number; callbackUrl?: string }

const synced = await client.sync(appId)
check(synced.syncStatus === "synced" && synced.workflows.length === 12 && synced.deployment === "e2e" && synced.sdk?.name === "@ezgh/workflows", "sync records the app's workflows", synced)
console.log("ok   sync")

const complete = await until((await client.start(appId, "e2e-complete")).id, terminal)
check(complete.status === "completed" && JSON.stringify(await client.output(complete.id)) === '{"sum":3}', "e2e-complete completes", complete)
console.log("ok   completion")

const runKey = `key-${Date.now()}`
const keyed = await client.start(appId, "e2e-complete", { a: 1 }, { runKey })
const again = await client.start(appId, "e2e-complete", { a: 1 }, { runKey })
check(again.id === keyed.id, "the same runKey and input return the same run", { keyed, again })
const conflict = await client.start(appId, "e2e-complete", { a: 2 }, { runKey }).then(() => null, (e: unknown) => e)
check(conflict instanceof WorkflowsApiError && conflict.status === 409 && conflict.code === "run_key_conflict", "the same runKey with different input is 409 run_key_conflict", conflict)
console.log("ok   idempotent start")

const slept = await until((await client.start(appId, "e2e-sleep")).id, terminal)
check(slept.status === "completed" && (await client.output(slept.id)) === 100_000 && stepNamed(slept, "nap")?.status === "completed", "e2e-sleep resumes after its timer, with the large payload replayed", slept)
console.log("ok   sleep and payload replay")

const retried = await until((await client.start(appId, "e2e-retry")).id, terminal)
check(retried.status === "completed" && (await client.output(retried.id)) === 3 && stepNamed(retried, "flaky")?.attempt === 3, "e2e-retry succeeds on attempt 3", retried)
console.log("ok   step retries")

const crashed = await until((await client.start(appId, "e2e-crash")).id, terminal, 90)
check(crashed.status === "completed" && (await client.output(crashed.id)) === "survived" && stepNamed(crashed, "crashy")?.attempt === 2, "a crash mid-step counts as an attempt of that step", crashed)
console.log("ok   crash mid-step is a step attempt")

const cancelling = await client.start(appId, "e2e-cancel")
await until(cancelling.id, (r) => stepNamed(r, "wait")?.status === "sleeping")
await client.cancel(cancelling.id)
const cancelled = await until(cancelling.id, terminal)
const state1 = await state()
check(cancelled.status === "cancelled" && stepNamed(cancelled, "cleanup")?.status === "completed" && state1.cleanup === 1, "cancel runs the cleanup step and ends cancelled", { cancelled, state1 })
console.log("ok   cancel with cleanup")

const fixable = await until((await client.start(appId, "e2e-fixable")).id, terminal)
check(fixable.status === "failed", "e2e-fixable fails before the fix", fixable)
await fetch(`${app}/__e2e/fix`)
await client.retry(fixable.id)
const fixed = await until(fixable.id, (r) => r.status === "completed" || (r.status === "failed" && r.attempt > fixable.attempt))
const state2 = await state()
check(fixed.status === "completed" && (await client.output(fixed.id)) === "fixed" && state2.prepare === 1, "retry resumes from the failed step without re-running completed ones", { fixed, state2 })
console.log("ok   retry from failure")

if (process.env.E2E_EXACT_JSON === "1") {
  const exact = await until((await client.start(appId, "e2e-exact")).id, terminal)
  const text = JSON.stringify(await client.output(exact.id))
  check(exact.status === "completed" && text === '{"b":1,"a":"x\\u0000y","c":{"z":[2,1],"y":null}}', "recorded JSON replays with its key order and NUL characters", { exact, text })
  console.log("ok   exact JSON replay")
} else console.log("skip exact JSON replay (this workflows-stack has no json-column migration 0005 yet)")

const sleepMs = (ms: number) => new Promise((r) => setTimeout(r, ms))
// exists waits for a run created asynchronously (a scheduled firing, a child) to answer GET.
async function exists(id: string, seconds = 60): Promise<Run> {
  const end = Date.now() + seconds * 1000 * scale
  for (;;) {
    try { return await client.get(id) } catch (e) {
      if (!(e instanceof WorkflowsApiError && e.status === 404) || Date.now() > end) throw e
      await sleepMs(500)
    }
  }
}

// Callback completed over the public URL.
const approvalRun = await client.start(appId, "e2e-approval")
let url: string | undefined
for (const end = Date.now() + 60_000 * scale; !url && Date.now() < end; await sleepMs(250)) url = (await state() as { callbackUrl?: string }).callbackUrl
check(url && url.startsWith(`${api}/public/v1/callbacks/`), "e2e-approval published a callback URL on the API host", { url })
const post = (body: unknown) => fetch(url!, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
const firstPost = await post({ approved: true })
check(firstPost.ok, "completing the callback succeeds", firstPost.status)
check((await post({ approved: true })).ok, "repeating the same completion answers success")
const different = await post({ approved: false })
check(different.status === 409, "a different completion is 409 callback_completed", different.status)
const approved = await until(approvalRun.id, terminal)
check(approved.status === "completed" && JSON.stringify(await client.output(approved.id)) === '{"approved":true}', "e2e-approval resumes with the callback's body", approved)
console.log("ok   callback completed over HTTP")

// Parent and child.
const parentRun = await until((await client.start(appId, "e2e-parent", { n: 4 })).id, terminal)
check(parentRun.status === "completed" && (await client.output(parentRun.id)) === 41, "e2e-parent completes with its child's output plus one", parentRun)
const childId = await childRunId(parentRun.id, stepNamed(parentRun, "child")!.stepId)
const childRun = await exists(childId)
check(childRun.status === "completed" && childRun.workflow === "e2e-child" && (await client.output(childId)) === 40, "the child run exists under its derived ID and completed", childRun)
console.log("ok   parent and child")

// Parallel fan-out.
const fan = await until((await client.start(appId, "e2e-fanout")).id, terminal)
check(fan.status === "completed" && (await client.output(fan.id)) === 10 && [0, 1, 2, 3, 4].every((i) => stepNamed(fan, `part-${i}`)?.status === "completed"), "e2e-fanout's five steps complete", fan)
console.log("ok   parallel fan-out")

// Pull worker: the worker announces e2e-pulled on its first poll; then a run of the pull app completes.
const pullAppId = process.env.E2E_PULL_APP_ID!
for (const end = Date.now() + 60_000 * scale; ; await sleepMs(500)) {
  const a = await client.getApp(pullAppId)
  if (a.workflows.some((w) => w.name === "e2e-pulled")) break
  check(Date.now() < end, "the pull worker announced e2e-pulled", a)
}
const pulledRun = await until((await client.start(pullAppId, "e2e-pulled", { n: 7 })).id, terminal)
check(pulledRun.status === "completed" && (await client.output(pulledRun.id)) === 49, "the pull worker runs e2e-pulled", pulledRun)
console.log("ok   pull worker")

// Schedule: every minute; the first firing starts a run whose ID derives from the scheduled time.
const sched = await client.createSchedule({ appId, workflow: "e2e-scheduled", name: `e2e-${Date.now()}`, cron: "* * * * *", timeZone: "UTC" })
let fired = sched
for (const end = Date.now() + 90_000 * scale; fired.lastScheduledTime === null; await sleepMs(1000)) {
  fired = await client.getSchedule(sched.id)
  check(Date.now() < end || fired.lastScheduledTime !== null, "the schedule fired within 90 s", fired)
}
const scheduledId = await scheduledRunId(sched.id, fired.lastScheduledTime!)
await exists(scheduledId)
const scheduledRun = await until(scheduledId, terminal)
const note = (await client.output(scheduledRun.id)) as { id: string; scheduledTime: string } | null
check(scheduledRun.status === "completed" && note?.id === sched.id && Date.parse(note.scheduledTime) === Date.parse(fired.lastScheduledTime!), "the scheduled run sees its schedule and scheduled time", { scheduledRun, note, fired })
const key = `e2e-trigger-${sched.id}`
const triggered = await client.triggerSchedule(sched.id, { idempotencyKey: key })
const again2 = await client.triggerSchedule(sched.id, { idempotencyKey: key })
check(triggered.id === (await triggeredRunId(sched.id, key)) && again2.id === triggered.id, "trigger derives its run ID from the key and is idempotent", { triggered, again2 })
check((await until(triggered.id, terminal)).status === "completed", "the triggered run completes")
const paused = await client.pauseSchedule(sched.id)
check(paused.paused === true, "pause pauses", paused)
await client.deleteSchedule(sched.id)
const gone = await client.getSchedule(sched.id).then(() => null, (e: unknown) => e)
check(gone instanceof WorkflowsApiError && gone.status === 404, "a deleted schedule is gone", gone)
console.log("ok   schedule fired, trigger, pause, delete")

console.log("PASSED")
