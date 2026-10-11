// Fixes from the SDK-TS chunk review (phase 2): callback replay order, step invocations that
// never report the run's outcome, the planParallel cap, cleanup under cancel.
import { describe, expect, test } from "bun:test"
import type { Command, Invocation, JournalEntry } from "../src/protocol/index.ts"
import { validateCommand } from "../src/protocol/validate.ts"
import { execute } from "../src/runtime/execution.ts"
import { workflow } from "../src/runtime/workflow.ts"
import { FakeLive, entry, invocation, sid } from "./helpers.ts"

const RUN = "0199a3d0-2b3c-7012-9f50-6b7c8d9e0f1a"
const v11 = (wf: string, entries: JournalEntry[] = [], extra: Partial<Invocation> = {}): Invocation =>
  invocation(wf, entries, {
    protocolVersion: "1.1", endpoints: { live: "https://live.test", api: "https://api.test" },
    run: { id: RUN, workflow: wf, input: {}, attempt: 1, startedAt: "2026-10-09T12:00:00Z" }, ...extra,
  })
const cmds = (xs: Command[]) => { xs.forEach(validateCommand); return JSON.parse(JSON.stringify(xs)) as Record<string, unknown>[] }

// Branch A creates a callback, logs and waits; branch B prepares and logs. Live, A's callback is
// created first, so A's log is log#0. Its journal entry's seq then moves to the completion.
const branches = workflow("branches", async ({ step }) => {
  const [a, b] = await Promise.all([
    (async () => { const cb = await step.createCallback("approval"); const l = await step.run("log", () => "A"); await cb.wait(); return l })(),
    (async () => { await step.run("prep", () => "P"); return step.run("log", () => "B") })(),
  ])
  return { a, b }
})

describe("callback replay order (createdSeq)", () => {
  const token = "012." + "A".repeat(43)
  const journal = async (createdSeq?: number) => [
    { seq: 5, ...(createdSeq !== undefined ? { createdSeq } : {}), stepId: await sid("approval"), name: "approval", kind: "callback", status: "completed", attempt: 1, token, output: { ok: true } },
    // Live: the callback was created (seq 1), A's log#0 and B's prep were planned together, log#0
    // completed (2), then prep (3), then B's log#1 (4); the callback completed last (5).
    await entry("log", 0, 2, { output: "A" }),
    await entry("prep", 0, 3, { output: "P" }),
    await entry("log", 1, 4, { output: "B" }),
  ] as JournalEntry[]

  test("the first execution creates the callback before planning A's log and B's prep", async () => {
    const live = new FakeLive()
    const out = cmds(await execute(v11("branches"), branches, live))
    expect(live.commandsOf(0).map((c) => c.type)).toEqual(["callbackCreated"])
    // B's prep was called first, A's log once the callback existed: planned in call order.
    expect(out).toEqual([{ type: "planParallel", steps: [
      { stepId: await sid("prep"), name: "prep", retry: expect.any(Object) },
      { stepId: await sid("log", 0), name: "log", retry: expect.any(Object) },
    ] }])
  })

  test("replay settles the creation at createdSeq and wait() at seq: each branch keeps its own log", async () => {
    expect(cmds(await execute(v11("branches", await journal(1)), branches, new FakeLive()))).toEqual([{ type: "runCompleted", output: { a: "A", b: "B" } }])
  })

  test("without createdSeq (an older engine) the creation settles at seq", async () => {
    // The swap the review found: documented here so the fix's dependency on createdSeq is visible.
    expect(cmds(await execute(v11("branches", await journal()), branches, new FakeLive()))).toEqual([{ type: "runCompleted", output: { a: "B", b: "A" } }])
  })
})

describe("step invocations never report the run's outcome", () => {
  test("a race its replay already decided ends with nothing", async () => {
    const racer = workflow("racer", async ({ step }) => Promise.race([step.run("a", () => "a"), step.run("b", () => "b")]))
    const journal = [
      await entry("a", 0, 1, { output: "a" }),
      { seq: 2, stepId: await sid("b"), name: "b", kind: "run", status: "running", attempt: 0 },
    ] as JournalEntry[]
    const live = new FakeLive()
    expect(cmds(await execute(v11("racer", journal, { kind: "step", targetStepId: await sid("b") }), racer, live))).toEqual([])
    expect(live.checkpoints).toEqual([])
  })

  test("a run failure its replay reaches ends with nothing", async () => {
    const wf = workflow("fails", async ({ step }) => {
      const [x] = await Promise.all([step.run("x", () => 1), step.run("y", () => 2)])
      return x
    })
    const journal = [
      { seq: 1, stepId: await sid("x"), name: "x", kind: "run", status: "failed", attempt: 5, error: { code: "user", name: "Error", message: "gone" } },
      { seq: 2, stepId: await sid("y"), name: "y", kind: "run", status: "running", attempt: 0 },
    ] as JournalEntry[]
    expect(cmds(await execute(v11("fails", journal, { kind: "step", targetStepId: await sid("y") }), wf, new FakeLive()))).toEqual([])
  })

  test("a fatal error (a duplicate name) ends with nothing", async () => {
    const wf = workflow("dupes", async ({ step }) => Promise.all([step.run("x", () => 1), step.run("x", () => 2)]))
    expect(cmds(await execute(v11("dupes", [], { kind: "step", targetStepId: await sid("x") }), wf, new FakeLive()))).toEqual([])
  })
})

describe("planParallel is capped at 1,000 steps", () => {
  const wide = (n: number) => workflow("wide", async ({ step }) => Promise.all(Array.from({ length: n }, (_, i) => step.run(`s${i}`, () => i))))
  test("1,000 steps are planned together", async () => {
    const out = cmds(await execute(v11("wide"), wide(1000), new FakeLive()))
    expect(out).toHaveLength(1)
    expect((out[0]!.steps as unknown[]).length).toBe(1000)
  })
  test("1,001 fail the run with a NonRetriableError", async () => {
    expect(cmds(await execute(v11("wide"), wide(1001), new FakeLive()))).toEqual([
      { type: "runFailed", error: expect.objectContaining({ code: "workflows.non_retriable" }) },
    ])
  })
})

describe("cancel", () => {
  test("a cleanup step runs at once, not behind siblings the engine is still running", async () => {
    const wf = workflow("cleanup", async ({ step }) => {
      try { await Promise.all([step.run("p0", () => 0), step.run("p1", () => 1)]); return "done" } finally { await step.run("cleanup", () => "cleaned") }
    })
    const journal = [
      { seq: 1, stepId: await sid("p0"), name: "p0", kind: "run", status: "running", attempt: 1 },
      { seq: 2, stepId: await sid("p1"), name: "p1", kind: "run", status: "running", attempt: 0 },
    ] as JournalEntry[]
    const cancel = { requestedAt: "2026-10-10T00:00:00Z", graceUntil: "2026-10-10T00:05:00Z" }
    const live = new FakeLive()
    const out = cmds(await execute(v11("cleanup", journal, { cancel }), wf, live))
    expect(live.checkpoints.map((c) => c.commands.map((x) => x.type))).toEqual([["stepStarted"]])
    expect(live.commandsOf(0)[0]).toMatchObject({ stepId: await sid("cleanup") })
    expect(out[0]).toEqual({ type: "stepCompleted", stepId: await sid("cleanup"), name: "cleanup", output: "cleaned" })
  })
})
