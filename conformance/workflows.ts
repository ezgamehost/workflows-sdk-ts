// The conformance workflows (workflows-stack protocol/conformance/workflows.md), shared by the
// push app (app.ts) and the pull worker (worker.ts).
import { type CallbackTimeout, NonRetriableError, RetryAfterError, WorkflowError, workflow } from "@ezgh/workflows"

const sequence = workflow("sequence", async ({ input, step }) => {
  let sum = 0
  for (let i = 0; i < (input as { n: number }).n; i++) sum += (await step.run("add", () => i)) as number
  return sum
})
const echo = workflow("echo", async ({ input, step }) => {
  const same = await step.run("echo", () => input)
  const when = await step.run("date", () => new Date(0))
  const nothing = await step.run("nothing", () => undefined)
  return { same, when, nothing }
})
const race = workflow("race", async ({ step }) => {
  const w = (await Promise.race([step.run("a", () => "a"), step.run("b", () => "b")])) as string
  return step.run(`after-${w}`, () => w)
})
const flaky = workflow("flaky", async ({ step }) =>
  step.run("flaky", () => { throw Object.assign(new Error("boom"), { data: { n: 1 } }) }, { retry: { maxAttempts: 3, initialMs: 10 } }))
const fatal = workflow("fatal", async ({ step }) => step.run("fatal", () => { throw new NonRetriableError("nope") }))
const slow = workflow("slow", async ({ step }) => step.run("slow", () => { throw new RetryAfterError("slow down", 1500) }))
const sleepy = workflow("sleepy", async ({ step }) => { await step.sleep("nap", "2s"); return step.run("after", () => "woke") })
const values = workflow("values", async ({ step }) => {
  const t = await step.now("t")
  const id = await step.uuid("id")
  return step.run("use", () => ({ year: t.getUTCFullYear() >= 2026, idLength: id.length }))
})
const dupe = workflow("dupe", async ({ step }) => Promise.all([step.run("x", () => 1), step.run("x", () => 2)]))
const big = workflow("big", async ({ step }) => step.run("big", () => "x".repeat(1_048_577)))
const cancellable = workflow("cancellable", async ({ step }) => {
  try { await step.sleep("wait", "1h"); return "finished" } finally { await step.run("cleanup", () => "cleaned") }
})
const caught = workflow("caught", async ({ step }) => {
  try { await step.run("bad", () => 1); return "unreachable" } catch (e) {
    const w = e as WorkflowError
    const cause = w.cause as Error | undefined
    return { isWorkflowError: e instanceof WorkflowError, code: w.code, name: w.name, data: w.data, causeName: cause?.name, causeMessage: cause?.message }
  }
})
const explicitid = workflow("explicitid", async ({ step }) => step.run("x", () => 1, { id: "custom-1" }))
const tuned = workflow("tuned", async ({ step }) => step.run("t", () => 1), { retry: { maxAttempts: 2 } })
const effect = workflow("effect", async ({ input, step }) =>
  step.run("effect", async () => { await (await fetch((input as { url: string }).url, { method: "POST" })).body?.cancel(); return "done" }))
const coded = workflow("coded", async ({ step }) => step.run("coded", () => { throw new WorkflowError("teapot", { code: "http.418", data: { status: 418 } }) }))
const widestack = workflow("widestack", async ({ step }) => step.run("wide", () => { const e = new Error("wide"); e.stack = "é".repeat(5000); throw e }))
const widename = workflow("widename", async ({ step }) => step.run("é".repeat(200), () => 1))
const dupeid = workflow("dupeid", async ({ step }) => Promise.all([step.run("a", () => 1, { id: "same" }), step.run("b", () => 2, { id: "same" })]))

// Phase 2 (protocol 1.1).
const approval = workflow("approval", async ({ input, step }) => {
  const cb = await step.createCallback("approval", { timeoutMs: 60_000 })
  const urlOk = await step.run("notify", () => cb.url === `${(input as { apiBase: string }).apiBase}/public/v1/callbacks/${cb.token}`)
  return { value: await cb.wait(), urlOk }
})
const approvaltimeout = workflow("approvaltimeout", async ({ step }) => {
  const cb = await step.createCallback("approval", { timeoutMs: 1000 })
  try { await cb.wait(); return "completed" } catch (e) { return { code: (e as CallbackTimeout).code } }
})
const parent = workflow("parent", async ({ step }) => step.invoke("child", { workflow: "sequence", input: { n: 3 } }))
const parentcatch = workflow("parentcatch", async ({ step }) => {
  try { await step.invoke("child", { workflow: "fatal" }); return "unreachable" } catch (e) {
    const w = e as WorkflowError
    return { code: w.code, causeCode: (w.cause as { code?: string } | undefined)?.code }
  }
})
const fanout = workflow("fanout", async ({ step }) =>
  ((await Promise.all([0, 1, 2].map((i) => step.run(`p${i}`, () => i)))) as number[]).reduce((a, b) => a + b, 0))
const racetimeout = workflow("racetimeout", async ({ step }) =>
  Promise.race([step.run("work", () => "work"), step.sleep("limit", "1m").then(() => "limit")]))
const scheduled = workflow("scheduled", async ({ run }) => run.schedule ?? null)
const pulled = workflow("pulled", async ({ input, step }) => step.run("compute", () => (input as { x: number }).x * 2))
const longstep = workflow("longstep", async ({ step }) => step.run("long", async ({ signal }) => {
  const end = Date.now() + 10_000
  while (!signal.aborted && Date.now() < end) await new Promise((r) => setTimeout(r, 50))
  return signal.aborted ? "aborted" : "done"
}))
const branches = workflow("branches", async ({ step }) => {
  const [a, b] = await Promise.all([
    (async () => { const cb = await step.createCallback("approval", { timeoutMs: 60_000 }); const l = await step.run("log", () => "A"); await cb.wait(); return l })(),
    (async () => { await step.run("prep", () => "P"); return step.run("log", () => "B") })(),
  ])
  return { a, b }
})

// In workflows.md's order (the sync reply's order).
export const all = [sequence, echo, race, flaky, fatal, slow, sleepy, values, dupe, big, cancellable, caught, explicitid, tuned, effect, coded, widestack, widename, dupeid,
  approval, approvaltimeout, parent, parentcatch, fanout, racetimeout, scheduled, pulled, longstep, branches]
