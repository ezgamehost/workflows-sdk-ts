import { describe, expect, test } from "bun:test"
import { CallbackTimeout, CancelledError, ChildFailed, NonRetriableError, WorkflowError, serve, workflow } from "../src/index.ts"
import type { Command, Invocation, JournalEntry, WireError } from "../src/protocol/index.ts"
import { sign } from "../src/protocol/index.ts"
import { callbackTokenHash, parseCallbackToken } from "../src/protocol/callback.ts"
import { execute } from "../src/runtime/execution.ts"
import { validateCommand } from "../src/protocol/validate.ts"
import { decodeError, encodeError } from "../src/runtime/errors.ts"
import { FakeLive, entry, invocation, sid } from "./helpers.ts"

// A run ID whose rand_a carries shard 18 (0x012): callback tokens must start "012.".
const RUN = "0199a3d0-2b3c-7012-9f50-6b7c8d9e0f1a"
const v11 = (wf: string, entries: JournalEntry[] = [], extra: Partial<Invocation> = {}): Invocation =>
  invocation(wf, entries, {
    protocolVersion: "1.1", endpoints: { live: "https://live.test", api: "https://api.test" },
    run: { id: RUN, workflow: wf, input: {}, attempt: 1, startedAt: "2026-10-09T12:00:00Z" }, ...extra,
  })
const cmds = (xs: Command[]) => { xs.forEach(validateCommand); return JSON.parse(JSON.stringify(xs)) as Record<string, unknown>[] }

describe("callbacks", () => {
  test("callbackCreated is acknowledged before the URL is visible; wait() suspends with waitForCallback", async () => {
    const live = new FakeLive()
    let acked = 0
    live.ack = async () => { acked++ }
    let sawBeforeAck = false
    const wf = workflow("cb", async ({ step }) => {
      const cb = await step.createCallback("approval", { timeoutMs: 60_000 })
      if (acked === 0) sawBeforeAck = true
      await step.run("notify", () => cb.url)
      return cb.wait()
    })
    const out = await execute(v11("cb"), wf, live)
    const created = live.commandsOf(0)[0] as Extract<Command, { type: "callbackCreated" }>
    expect(created).toMatchObject({ type: "callbackCreated", stepId: await sid("approval"), name: "approval", timeoutMs: 60_000 })
    expect(created.token).toMatch(/^012\.[A-Za-z0-9_-]{43}$/)
    expect(parseCallbackToken(created.token).shard).toBe(18)
    expect(sawBeforeAck).toBe(false)
    expect(cmds(out)).toEqual([
      { type: "stepCompleted", stepId: await sid("notify"), name: "notify", output: `https://api.test/public/v1/callbacks/${created.token}` },
      { type: "waitForCallback", stepId: await sid("approval") },
    ])
  })

  test("replay: the journaled token rebuilds the same URL, and a completed callback resolves wait()", async () => {
    const token = "012." + "A".repeat(43)
    const journal = [
      { seq: 3, stepId: await sid("approval"), name: "approval", kind: "callback", status: "completed", attempt: 1, token, output: { ok: true } },
      await entry("notify", 0, 2, { output: `https://api.test/public/v1/callbacks/${token}` }),
    ] as JournalEntry[]
    const wf = workflow("cb", async ({ step }) => {
      const cb = await step.createCallback("approval")
      const url = await step.run("notify", () => cb.url)
      return { url, same: url === cb.url, value: await cb.wait() }
    })
    const live = new FakeLive()
    expect(cmds(await execute(v11("cb", journal), wf, live))).toEqual([{ type: "runCompleted", output: { url: `https://api.test/public/v1/callbacks/${token}`, same: true, value: { ok: true } } }])
    expect(live.checkpoints).toEqual([])
  })

  test("a waiting callback re-sends waitForCallback (the engine treats it as idempotent)", async () => {
    const token = "012." + "B".repeat(43)
    const journal = [{ seq: 1, stepId: await sid("approval"), name: "approval", kind: "callback", status: "waiting", attempt: 1, token }] as JournalEntry[]
    const wf = workflow("cb", async ({ step }) => (await step.createCallback("approval")).wait())
    expect(cmds(await execute(v11("cb", journal), wf, new FakeLive()))).toEqual([{ type: "waitForCallback", stepId: await sid("approval") }])
  })

  test("a timed-out callback rejects wait() with CallbackTimeout (code workflows.callback_timeout)", async () => {
    const token = "012." + "C".repeat(43)
    const journal = [{ seq: 1, stepId: await sid("approval"), name: "approval", kind: "callback", status: "failed", attempt: 1, token,
      error: { code: "workflows.callback_timeout", name: "CallbackTimeout", message: "No completion within 1000 ms" } }] as JournalEntry[]
    const wf = workflow("cb", async ({ step }) => {
      const cb = await step.createCallback("approval", { timeoutMs: 1000 })
      try { await cb.wait(); return "completed" } catch (e) { return { timeout: e instanceof CallbackTimeout, code: (e as WorkflowError).code } }
    })
    expect(cmds(await execute(v11("cb", journal), wf, new FakeLive()))).toEqual([{ type: "runCompleted", output: { timeout: true, code: "workflows.callback_timeout" } }])
  })

  test("callback options are checked, and the token hash is SHA-256 of the secret", async () => {
    const wf = workflow("cb", async ({ step }) => step.createCallback("approval", { timeoutMs: 10 }))
    const out = cmds(await execute(v11("cb"), wf, new FakeLive()))
    expect(out[0]).toMatchObject({ type: "runFailed", error: { code: "user" } }) // an argument error: checked by code (D2-21)
    const hash = await callbackTokenHash("012." + "A".repeat(43))
    expect(hash).toMatch(/^[0-9a-f]{64}$/)
  })

  test("protocol 1.0: createCallback fails the run (the engine wouldn't understand it)", async () => {
    const wf = workflow("cb", async ({ step }) => step.createCallback("approval"))
    const out = cmds(await execute(v11("cb", [], { protocolVersion: "1.0" }), wf, new FakeLive()))
    expect(out[0]).toMatchObject({ type: "runFailed", error: { code: "workflows.non_retriable" } })
  })
})

describe("invoke", () => {
  const parent = workflow("parent", async ({ step }) => step.invoke("child", { workflow: "sequence", input: { n: 3 } }))
  test("the first execution sends invoke and ends", async () => {
    expect(cmds(await execute(v11("parent"), parent, new FakeLive()))).toEqual([
      { type: "invoke", stepId: await sid("child"), name: "child", workflow: "sequence", input: { n: 3 } },
    ])
  })
  test("a waiting child ends the invocation with nothing", async () => {
    const journal = [{ seq: 1, stepId: await sid("child"), name: "child", kind: "invoke", status: "waiting", attempt: 1 }] as JournalEntry[]
    expect(cmds(await execute(v11("parent", journal), parent, new FakeLive()))).toEqual([])
  })
  test("a completed child resolves with its output", async () => {
    const journal = [{ seq: 1, stepId: await sid("child"), name: "child", kind: "invoke", status: "completed", attempt: 1, output: 3 }] as JournalEntry[]
    expect(cmds(await execute(v11("parent", journal), parent, new FakeLive()))).toEqual([{ type: "runCompleted", output: 3 }])
  })
  test("a failed child rejects with ChildFailed, whose cause is the child's own error class", async () => {
    const wf = workflow("parentcatch", async ({ step }) => {
      try { await step.invoke("child", { workflow: "fatal" }); return "unreachable" } catch (e) {
        const w = e as ChildFailed
        return { childFailed: e instanceof ChildFailed, code: w.code, causeCode: (w.cause as NonRetriableError).code, causeIsNonRetriable: w.cause instanceof NonRetriableError }
      }
    })
    const journal = [{ seq: 1, stepId: await sid("child"), name: "child", kind: "invoke", status: "failed", attempt: 1,
      error: { code: "workflows.child_failed", name: "ChildFailed", message: "The child run failed", cause: { code: "workflows.non_retriable", name: "NonRetriableError", message: "nope" } } }] as JournalEntry[]
    expect(cmds(await execute(v11("parentcatch", journal), wf, new FakeLive()))).toEqual([
      { type: "runCompleted", output: { childFailed: true, code: "workflows.child_failed", causeCode: "workflows.non_retriable", causeIsNonRetriable: true } },
    ])
  })
  test("invoke checks the workflow name and accepts a Workflow object", async () => {
    const child = workflow("sequence", async () => 1)
    const byObject = workflow("parent", async ({ step }) => step.invoke("child", { workflow: child }))
    expect(cmds(await execute(v11("parent"), byObject, new FakeLive()))[0]).toMatchObject({ type: "invoke", workflow: "sequence" })
    const bad = workflow("parent", async ({ step }) => step.invoke("child", { workflow: "no spaces allowed" }))
    expect(cmds(await execute(v11("parent"), bad, new FakeLive()))[0]).toMatchObject({ type: "runFailed", error: { code: "user" } })
  })
})

describe("parallel steps", () => {
  const fanout = workflow("fanout", async ({ step }) => {
    const xs = await Promise.all([0, 1, 2].map((i) => step.run(`p${i}`, () => i)))
    return (xs as number[]).reduce((a, b) => a + b, 0)
  })
  const retry = { maxAttempts: 5, initialMs: 1000, maxMs: 600000, multiplier: 2 }

  test("1.1: steps discovered together are planned together, in call order, with their policies", async () => {
    const live = new FakeLive()
    expect(cmds(await execute(v11("fanout"), fanout, live))).toEqual([
      { type: "planParallel", steps: [
        { stepId: await sid("p0"), name: "p0", retry }, { stepId: await sid("p1"), name: "p1", retry }, { stepId: await sid("p2"), name: "p2", retry },
      ] },
    ])
    expect(live.checkpoints).toEqual([])
  })

  test("1.0: the same workflow runs its steps one after another (phase 1)", async () => {
    const live = new FakeLive()
    const out = cmds(await execute(v11("fanout", [], { protocolVersion: "1.0" }), fanout, live))
    expect(live.checkpoints.map((c) => c.commands.at(-1)!.type)).toEqual(["stepStarted", "stepStarted", "stepStarted"])
    expect(out.at(-1)).toEqual({ type: "runCompleted", output: 3 })
  })

  test("a step invocation runs only its target, at the next attempt, and reports it in the result", async () => {
    const journal = [
      { seq: 1, stepId: await sid("p0"), name: "p0", kind: "run", status: "running", attempt: 0 },
      { seq: 2, stepId: await sid("p1"), name: "p1", kind: "run", status: "running", attempt: 0 },
      { seq: 3, stepId: await sid("p2"), name: "p2", kind: "run", status: "running", attempt: 0 },
    ] as JournalEntry[]
    const live = new FakeLive()
    const out = cmds(await execute(v11("fanout", journal, { kind: "step", targetStepId: await sid("p1") }), fanout, live))
    expect(live.checkpoints.map((c) => c.commands)).toEqual([[{ type: "stepStarted", stepId: await sid("p1"), name: "p1", attempt: 1, retry }]])
    expect(out).toEqual([{ type: "stepCompleted", stepId: await sid("p1"), name: "p1", output: 1 }])
  })

  test("an orchestration while siblings still run ends with nothing; once all are recorded the run completes", async () => {
    const some = [
      await entry("p0", 0, 1, { output: 0 }),
      { seq: 2, stepId: await sid("p1"), name: "p1", kind: "run", status: "running", attempt: 1 },
      await entry("p2", 0, 3, { output: 2 }),
    ] as JournalEntry[]
    expect(cmds(await execute(v11("fanout", some), fanout, new FakeLive()))).toEqual([])
    const all = [await entry("p0", 0, 1, { output: 0 }), await entry("p1", 0, 2, { output: 1 }), await entry("p2", 0, 3, { output: 2 })] as JournalEntry[]
    expect(cmds(await execute(v11("fanout", all), fanout, new FakeLive()))).toEqual([{ type: "runCompleted", output: 3 }])
  })

  test("a run raced against a sleep plans the run and sends the sleep; the winner is the lower seq", async () => {
    const racer = workflow("racetimeout", async ({ step }) =>
      Promise.race([step.run("work", () => "work"), step.sleep("limit", "1m").then(() => "limit")]))
    const first = cmds(await execute(v11("racetimeout"), racer, new FakeLive()))
    expect(first[0]).toEqual({ type: "planParallel", steps: [{ stepId: await sid("work"), name: "work", retry }] })
    expect(first[1]).toMatchObject({ type: "sleep", stepId: await sid("limit"), name: "limit" })
    const sleptFirst = [
      { seq: 1, stepId: await sid("limit"), name: "limit", kind: "sleep", status: "completed", attempt: 1 },
      { seq: 2, stepId: await sid("work"), name: "work", kind: "run", status: "running", attempt: 1 },
    ] as JournalEntry[]
    expect(cmds(await execute(v11("racetimeout", sleptFirst), racer, new FakeLive()))).toEqual([{ type: "runCompleted", output: "limit" }])
  })

  test("one new step beside a step the engine is already running is planned, not run inline (D2-19)", async () => {
    const wf = workflow("two", async ({ step }) => Promise.all([step.run("a", () => "a"), step.run("b", () => "b")]))
    const journal = [{ seq: 1, stepId: await sid("a"), name: "a", kind: "run", status: "running", attempt: 1 }] as JournalEntry[]
    const live = new FakeLive()
    const out = cmds(await execute(v11("two", journal), wf, live))
    expect(live.checkpoints).toEqual([])
    expect(out).toEqual([{ type: "planParallel", steps: [{ stepId: await sid("b"), name: "b", retry }] }])
  })

  test("a lone retrying step runs inline at the next attempt; beside other work it waits", async () => {
    const one = workflow("one", async ({ step }) => step.run("a", () => "a"))
    const live = new FakeLive()
    const journal = [{ seq: 1, stepId: await sid("a"), name: "a", kind: "run", status: "retrying", attempt: 2 }] as JournalEntry[]
    expect(cmds(await execute(v11("one", journal), one, live))).toEqual([{ type: "stepCompleted", stepId: await sid("a"), name: "a", output: "a" }, { type: "runCompleted", output: "a" }])
    expect(live.checkpoints[0]!.commands).toEqual([{ type: "stepStarted", stepId: await sid("a"), name: "a", attempt: 3, retry }])
    const two = workflow("two", async ({ step }) => Promise.all([step.run("a", () => "a"), step.sleep("nap", "1m")]))
    const out = cmds(await execute(v11("two", journal), two, new FakeLive()))
    expect(out).toEqual([{ type: "sleep", stepId: await sid("nap"), name: "nap", until: expect.any(String) }])
  })

  test("a value is recorded at once, even while steps are with the engine", async () => {
    const wf = workflow("v", async ({ step }) => Promise.all([step.run("a", () => 1), step.now("t")]))
    const journal = [{ seq: 1, stepId: await sid("a"), name: "a", kind: "run", status: "running", attempt: 0 }] as JournalEntry[]
    const out = cmds(await execute(v11("v", journal), wf, new FakeLive()))
    expect(out).toEqual([{ type: "valueRecorded", stepId: await sid("t"), name: "t", value: expect.stringMatching(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/) }])
  })

  test("several invokes discovered together are sent together", async () => {
    const wf = workflow("children", async ({ step }) =>
      Promise.all([step.invoke("c0", { workflow: "sequence", input: { n: 1 } }), step.invoke("c1", { workflow: "sequence", input: { n: 2 } })]))
    expect(cmds(await execute(v11("children"), wf, new FakeLive()))).toEqual([
      { type: "invoke", stepId: await sid("c0"), name: "c0", workflow: "sequence", input: { n: 1 } },
      { type: "invoke", stepId: await sid("c1"), name: "c1", workflow: "sequence", input: { n: 2 } },
    ])
  })
})

describe("schedule metadata, cancellation and errors", () => {
  test("ctx.traceparent is the invocation's traceparent, and \"\" when it's absent or not a string", async () => {
    const wf = workflow("traced", async ({ traceparent }) => traceparent)
    const tp = "00-0199a3d02b3c7d4e9f506b7c8d9e0f1a-0123456789abcdef-01"
    expect(cmds(await execute(v11("traced", [], { traceparent: tp }), wf, new FakeLive()))).toEqual([{ type: "runCompleted", output: tp }])
    expect(cmds(await execute(v11("traced"), wf, new FakeLive()))).toEqual([{ type: "runCompleted", output: "" }])
    expect(cmds(await execute(v11("traced", [], { traceparent: 7 as never }), wf, new FakeLive()))).toEqual([{ type: "runCompleted", output: "" }])
  })
  test("run.schedule is the invocation's schedule, exactly as sent, and absent otherwise", async () => {
    const wf = workflow("scheduled", async ({ run }) => run.schedule ?? null)
    const schedule = { id: "0199a3d0-0000-7012-8000-000000000001", scheduledTime: "2026-10-10T00:00:00Z" }
    const withIt = v11("scheduled", [], { run: { id: RUN, workflow: "scheduled", attempt: 1, startedAt: "2026-10-10T00:00:01Z", schedule } })
    expect(cmds(await execute(withIt, wf, new FakeLive()))).toEqual([{ type: "runCompleted", output: schedule }])
    expect(cmds(await execute(v11("scheduled"), wf, new FakeLive()))).toEqual([{ type: "runCompleted", output: null }])
  })

  test("cancelSignal aborts a running step's signal with a CancelledError", async () => {
    const cancel = new AbortController()
    let reason: unknown
    const wf = workflow("long", async ({ step }) => step.run("long", async ({ signal }) => {
      setTimeout(() => cancel.abort(), 20)
      await new Promise<void>((r) => signal.addEventListener("abort", () => r(), { once: true }))
      reason = signal.reason
      return "aborted"
    }))
    const out = cmds(await execute(v11("long"), wf, new FakeLive(), { cancelSignal: cancel.signal }))
    expect(reason).toBeInstanceOf(CancelledError)
    expect(out).toEqual([{ type: "stepCompleted", stepId: await sid("long"), name: "long", output: "aborted" }, { type: "runCompleted", output: "aborted" }])
  })

  test("every SDK error class has its wire code", () => {
    expect(new NonRetriableError("x").code).toBe("workflows.non_retriable")
    expect(new CancelledError("x").code).toBe("workflows.cancelled")
    expect(new CallbackTimeout("x").code).toBe("workflows.callback_timeout")
    expect(new ChildFailed("x").code).toBe("workflows.child_failed")
  })
})

describe("sync", () => {
  test("the sync reply declares protocol 1.0, 1.1 and 1.2", async () => {
    const secret = "whsec_" + "A".repeat(43)
    const handler = serve({ workflows: [workflow("hello", async () => 1)], signingSecrets: [secret] })
    const body = JSON.stringify({ protocolVersion: "1.1", kind: "sync", invocationId: "inv_aaaaaaaaaaaaaaaaaaaaaaaaaa", token: "", endpoints: { live: "https://live.test" }, deadline: new Date(Date.now() + 60_000).toISOString(), cancel: null })
    const bytes = new TextEncoder().encode(body)
    const { decodeSecret } = await import("../src/protocol/index.ts")
    const res = await handler(new Request("https://app.test/", { method: "POST", body: bytes, headers: { "EZGH-Signature": await sign([decodeSecret(secret)], new Date(), bytes) } }))
    expect(((await res.json()) as { protocolVersions: string[] }).protocolVersions).toEqual(["1.0", "1.1", "1.2"])
  })
})

describe("error cause chains", () => {
  test("a 20-deep cause chain encodes to exactly 8 levels (internal/protocol MaxErrorDepth)", () => {
    let e: Error = new Error("level 20")
    for (let i = 19; i >= 1; i--) e = new Error(`level ${i}`, { cause: e })
    let w: WireError | undefined = encodeError(e)
    let levels = 0
    while (w) { levels++; w = w.cause }
    expect(levels).toBe(8)
    // what validate.go checks: depth < 8 for every level
    let again: WireError | undefined = encodeError(decodeError(encodeError(e)))
    levels = 0
    while (again) { levels++; again = again.cause }
    expect(levels).toBe(8)
  })
})
