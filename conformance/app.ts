// The conformance app: workflows-stack protocol/conformance/workflows.md, served for
// cmd/workflows-conformance (scripts/conformance.sh).
import { NonRetriableError, RetryAfterError, WorkflowError, serve, workflow } from "@ezgh/workflows"

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

const handler = serve({
  workflows: [sequence, echo, race, flaky, fatal, slow, sleepy, values, dupe, big, cancellable, caught, explicitid, tuned, effect, coded, widestack, widename, dupeid],
  signingSecrets: [process.env.CONFORMANCE_SECRET!], maxInvocationSeconds: 300, deployment: "conformance",
})
Bun.serve({ hostname: "127.0.0.1", port: Number(process.env.PORT ?? 3100), fetch: handler })
