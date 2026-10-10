// The end-to-end driver: each check through the public API (Client) against a local
// workflows-stack; exits 1 at the first failure.
import { Client, WorkflowsApiError, type Run } from "@ezgh/workflows"

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
async function until(id: string, done: (r: Run) => boolean, seconds = 60): Promise<Run> {
  const end = Date.now() + seconds * 1000
  let r = await client.get(id)
  while (!done(r) && Date.now() < end) { await new Promise((s) => setTimeout(s, 500)); r = await client.get(id) }
  check(done(r), `run ${r.workflow} reached the expected state (it's ${r.status})`, r)
  return r
}
const terminal = (r: Run) => ["completed", "failed", "cancelled", "timedOut"].includes(r.status)
const stepNamed = (r: Run, name: string) => r.steps?.find((s) => s.name === name)
const state = async () => (await (await fetch(`${app}/__e2e/state`)).json()) as { cleanup: number; prepare: number }

const synced = await client.sync(appId)
check(synced.syncStatus === "synced" && synced.workflows.length === 7 && synced.deployment === "e2e" && synced.sdk?.name === "@ezgh/workflows", "sync records the app's workflows", synced)
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

console.log("PASSED")
