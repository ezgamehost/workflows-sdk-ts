// The end-to-end app: served on 127.0.0.1:3200 under a supervisor (scripts/e2e.sh), driven by
// e2e/run.ts through the real workflows-stack.
import { readFileSync, writeFileSync } from "node:fs"
import { NonRetriableError, serve, workflow } from "@ezgh/workflows"

const STATE = process.env.E2E_STATE ?? "/tmp/workflows-e2e-state.json"
type State = { fixed: boolean; prepare: number; cleanup: number; callbackUrl?: string }
const read = (): State => { try { return JSON.parse(readFileSync(STATE, "utf8")) as State } catch { return { fixed: false, prepare: 0, cleanup: 0 } } }
const bump = (k: "prepare" | "cleanup") => { const s = read(); s[k]++; writeFileSync(STATE, JSON.stringify(s)) }
const attemptOf = (key: string) => Number(key.split(":").at(-1))

const complete = workflow("e2e-complete", async ({ step }) => {
  const a = (await step.run("one", () => 1)) as number
  const b = (await step.run("two", () => 2)) as number
  return { sum: a + b }
})
const sleepy = workflow("e2e-sleep", async ({ step }) => {
  const large = (await step.run("large", () => "y".repeat(100_000))) as string
  await step.sleep("nap", "2s")
  return step.run("after", () => large.length)
})
const retry = workflow("e2e-retry", async ({ step }) =>
  step.run("flaky", () => { const n = attemptOf(step.idempotencyKey()); if (n < 3) throw new Error(`attempt ${n}`); return n }, { retry: { maxAttempts: 5, initialMs: 200 } }))
const crash = workflow("e2e-crash", async ({ step }) =>
  step.run("crashy", () => { if (attemptOf(step.idempotencyKey()) === 1) process.exit(1); return "survived" }, { retry: { maxAttempts: 3, initialMs: 200 } }))
const cancel = workflow("e2e-cancel", async ({ step }) => {
  try { await step.sleep("wait", "1h"); return "finished" } finally { await step.run("cleanup", () => { bump("cleanup"); return "cleaned" }) }
})
const fixable = workflow("e2e-fixable", async ({ step }) => {
  await step.run("prepare", () => { bump("prepare"); return true })
  return step.run("work", () => { if (!read().fixed) throw new NonRetriableError("not fixed yet"); return "fixed" })
})

// Recorded JSON replays exactly: key order and NUL characters survive the journal (a sleep forces
// the replay).
const exact = workflow("e2e-exact", async ({ step }) => {
  const v = await step.run("make", () => JSON.parse('{"b":1,"a":"x\\u0000y","c":{"z":[2,1],"y":null}}') as unknown)
  await step.sleep("nap", "1s")
  return step.run("echo", () => v)
})

// Phase 2 (protocol 1.1): a callback, a parent and child, a parallel fan-out and a scheduled run.
const approval = workflow("e2e-approval", async ({ step }) => {
  const cb = await step.createCallback("approval", { timeoutMs: 120_000 })
  await step.run("publish", () => { const s = read(); s.callbackUrl = cb.url; writeFileSync(STATE, JSON.stringify(s)); return true })
  return cb.wait()
})
const child = workflow("e2e-child", async ({ input, step }) => step.run("times-ten", () => (input as { n: number }).n * 10))
const parent = workflow("e2e-parent", async ({ input, step }) => {
  const v = (await step.invoke("child", { workflow: "e2e-child", input })) as number
  return step.run("plus-one", () => v + 1)
})
const fanout = workflow("e2e-fanout", async ({ step }) =>
  ((await Promise.all([0, 1, 2, 3, 4].map((i) => step.run(`part-${i}`, async () => { await new Promise((r) => setTimeout(r, 200)); return i })))) as number[]).reduce((a, b) => a + b, 0))
const scheduled = workflow("e2e-scheduled", async ({ run, step }) => step.run("note", () => run.schedule ?? null))

const handler = serve({
  workflows: [complete, sleepy, retry, crash, cancel, fixable, exact, approval, child, parent, fanout, scheduled],
  signingSecrets: [process.env.EZGH_WORKFLOWS_SIGNING_SECRET!], maxInvocationSeconds: 60, deployment: "e2e",
})
Bun.serve({
  hostname: "127.0.0.1", port: Number(process.env.PORT ?? 3200),
  async fetch(req) {
    const path = new URL(req.url).pathname
    if (path === "/__e2e/fix") { const s = read(); s.fixed = true; writeFileSync(STATE, JSON.stringify(s)); return new Response("ok") }
    if (path === "/__e2e/state") return Response.json(read())
    return handler(req)
  },
})
