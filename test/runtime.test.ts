import { describe, expect, test } from "bun:test"
import codec from "./vectors/codec.json"
import replayOrder from "./vectors/replay-order.json"
import { DEFAULT_RETRY, type JournalEntry } from "../src/protocol/index.ts"
import { CancelledError, NonRetriableError, RetryAfterError, WorkflowError, decodeError, encodeError } from "../src/runtime/errors.ts"
import { execute } from "../src/runtime/execution.ts"
import { CheckpointFailed, InvocationSuperseded, httpLive } from "../src/runtime/live.ts"
import { workflow } from "../src/runtime/workflow.ts"
import { FakeLive, codecValues, entry, invocation, sid } from "./helpers.ts"

const sequence = workflow("sequence", async ({ input, step }) => {
  let sum = 0
  for (let i = 0; i < (input as { n: number }).n; i++) sum += (await step.run("add", () => i)) as number
  return sum
})

describe("first execution", () => {
  test("acknowledged stepStarted before each step, outcomes pipelined into the next checkpoint", async () => {
    const live = new FakeLive()
    const commands = await execute(invocation("sequence"), sequence, live)
    expect(live.checkpoints.map((c) => c.seq)).toEqual([1, 2])
    expect(live.checkpoints.map((c) => c.invocationId)).toEqual(["inv_aaaaaaaaaaaaaaaaaaaaaaaaaa", "inv_aaaaaaaaaaaaaaaaaaaaaaaaaa"])
    expect(live.commandsOf(0)).toEqual([{ type: "stepStarted", stepId: await sid("add", 0), name: "add", attempt: 1, retry: DEFAULT_RETRY }])
    expect(live.commandsOf(1)).toEqual([
      { type: "stepCompleted", stepId: await sid("add", 0), name: "add", output: 0 },
      { type: "stepStarted", stepId: await sid("add", 1), name: "add", attempt: 1, retry: DEFAULT_RETRY },
    ])
    expect(commands).toEqual([
      { type: "stepCompleted", stepId: await sid("add", 1), name: "add", output: 1 },
      { type: "runCompleted", output: 1 },
    ])
  })

  test("a step doesn't run before its stepStarted is acknowledged", async () => {
    const live = new FakeLive()
    let release!: () => void
    const acked = new Promise<void>((r) => (release = r))
    live.ack = () => acked
    let ran = false
    const wf = workflow("one", async ({ step }) => step.run("x", () => { ran = true; return 1 }))
    const p = execute(invocation("one"), wf, live)
    await new Promise((r) => setTimeout(r, 20))
    expect(ran).toBe(false)
    release()
    await p
    expect(ran).toBe(true)
  })

  test("a superseded or failed checkpoint aborts the invocation before the step runs", async () => {
    let ran = false
    const wf = workflow("one", async ({ step }) => step.run("x", () => { ran = true; return 1 }))
    const superseded = new FakeLive()
    superseded.ack = async () => { throw new InvocationSuperseded("superseded") }
    await expect(execute(invocation("one"), wf, superseded)).rejects.toBeInstanceOf(InvocationSuperseded)
    const failed = new FakeLive()
    failed.ack = async () => { throw new CheckpointFailed("down") }
    await expect(execute(invocation("one"), wf, failed)).rejects.toBeInstanceOf(CheckpointFailed)
    expect(ran).toBe(false)
  })

  test("live values are JSON round-tripped, the same as on replay", async () => {
    const seen: unknown[] = []
    const wf = workflow("dates", async ({ step }) => { const d = await step.run("d", () => new Date(0)); seen.push(d); return d })
    const commands = await execute(invocation("dates"), wf, new FakeLive())
    expect(seen).toEqual(["1970-01-01T00:00:00.000Z"])
    expect(commands.at(-1)).toEqual({ type: "runCompleted", output: "1970-01-01T00:00:00.000Z" })
  })

  test("codec vectors: step results encode as the vectors say, and unencodable ones are non-retriable", async () => {
    for (const c of codec.cases) {
      const wf = workflow("codec", async ({ step }) => step.run("v", () => codecValues[c.tag]))
      const commands = await execute(invocation("codec"), wf, new FakeLive())
      if ("error" in c) expect([c.tag, commands[0]]).toMatchObject([c.tag, { type: "stepFailed", retryable: false, error: { code: c.error } }])
      else expect([c.tag, commands]).toEqual([c.tag, [{ type: "stepCompleted", stepId: await sid("v"), name: "v", output: c.encoded }, { type: "runCompleted", output: c.encoded }]])
    }
  })

  test("an output over 1 MiB fails the step without retries", async () => {
    const wf = workflow("big", async ({ step }) => step.run("big", () => "x".repeat(1_048_577)))
    const commands = await execute(invocation("big"), wf, new FakeLive())
    expect(commands).toEqual([{ type: "stepFailed", stepId: await sid("big", 0), name: "big", attempt: 1, retryable: false,
      error: { code: "workflows.output_too_large", name: "OutputTooLarge", message: expect.any(String), stack: expect.any(String) } }])
  })

  test("errors: retryable, non-retriable, retry-after", async () => {
    const mk = (e: Error) => workflow("e", async ({ step }) => step.run("e", () => { throw e }))
    const plain = await execute(invocation("e"), mk(Object.assign(new Error("boom"), { data: { n: 1 } })), new FakeLive())
    expect(plain[0]).toMatchObject({ type: "stepFailed", retryable: true, error: { code: "user", name: "Error", message: "boom", data: { n: 1 } } })
    expect(plain[0]).not.toHaveProperty("retryAfterMs")
    const fatal = await execute(invocation("e"), mk(new NonRetriableError("nope")), new FakeLive())
    expect(fatal[0]).toMatchObject({ retryable: false, error: { code: "workflows.non_retriable", name: "NonRetriableError" } })
    const later = await execute(invocation("e"), mk(new RetryAfterError("slow down", 1500)), new FakeLive())
    expect(later[0]).toMatchObject({ retryable: true, retryAfterMs: 1500, error: { code: "user", name: "RetryAfterError" } })
  })

  test("step options merge over the workflow's and the default retry policy; explicit IDs are used verbatim", async () => {
    const wf = workflow("opts", async ({ step }) => step.run("x", () => 1, { id: "custom-1", retry: { maxAttempts: 3, initialMs: 10 }, timeoutMs: 5000 }), { retry: { maxAttempts: 9, initialMs: 1, maxMs: 7, multiplier: 3 } })
    const live = new FakeLive()
    await execute(invocation("opts"), wf, live)
    expect(live.commandsOf(0)).toEqual([{ type: "stepStarted", stepId: "custom-1", name: "x", attempt: 1,
      retry: { maxAttempts: 3, initialMs: 10, maxMs: 7, multiplier: 3 }, timeoutMs: 5000 }])
  })

  test("explicit IDs don't advance the name's occurrence counter", async () => {
    const wf = workflow("ids", async ({ step }) => { await step.run("x", () => 1, { id: "custom" }); return step.run("x", () => 2) })
    const live = new FakeLive()
    await execute(invocation("ids"), wf, live)
    expect(live.commandsOf(1).at(-1)).toMatchObject({ type: "stepStarted", stepId: await sid("x", 0) })
  })

  test("a step over its timeout is a retryable step_timeout", async () => {
    const wf = workflow("slow", async ({ step }) => step.run("s", () => new Promise((r) => setTimeout(r, 200)), { timeoutMs: 20 }))
    const commands = await execute(invocation("slow"), wf, new FakeLive())
    expect(commands[0]).toMatchObject({ type: "stepFailed", retryable: true, error: { code: "workflows.step_timeout" } })
  })

  test("two concurrently pending steps with one name fail the run", async () => {
    const wf = workflow("dupe", async ({ step }) => Promise.all([step.run("x", () => 1), step.run("x", () => 2)]))
    const live = new FakeLive()
    const commands = await execute(invocation("dupe"), wf, live)
    expect(live.checkpoints).toEqual([])
    expect(commands).toEqual([{ type: "runFailed", error: { code: "workflows.duplicate_step_name", name: "DuplicateStepName", message: expect.any(String), stack: expect.any(String) } }])
  })

  test("now and uuid are recorded before the next step starts", async () => {
    const wf = workflow("values", async ({ step }) => { const t = await step.now("t"); const id = await step.uuid("id"); return step.run("use", () => [t.getTime() > 0, id.length]) })
    const live = new FakeLive()
    await execute(invocation("values"), wf, live)
    const first = live.commandsOf(0)
    expect(first.map((c) => c.type)).toEqual(["valueRecorded", "valueRecorded", "stepStarted"])
    expect((first[1] as { value: string }).value).toMatch(/^[0-9a-f-]{36}$/)
  })

  test("sleep ends the invocation", async () => {
    const wf = workflow("sleepy", async ({ step }) => { await step.sleep("nap", "2s"); return 1 })
    const now = new Date("2026-10-09T12:00:00.000Z")
    const commands = await execute(invocation("sleepy"), wf, new FakeLive(), { now: () => now })
    expect(commands).toEqual([{ type: "sleep", stepId: await sid("nap", 0), name: "nap", until: "2026-10-09T12:00:02.000Z" }])
  })

  test("the step budget ends the invocation without a terminal command", async () => {
    const live = new FakeLive()
    const commands = await execute(invocation("sequence", [], { run: { id: "0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a", workflow: "sequence", input: { n: 3 }, attempt: 1, startedAt: "2026-10-09T12:00:00Z" } }), sequence, live, { maxStepsPerInvocation: 2 })
    expect(commands).toEqual([{ type: "stepCompleted", stepId: await sid("add", 1), name: "add", output: 1 }])
  })

  test("the time budget ends the invocation without a terminal command", async () => {
    let t = Date.parse("2026-10-09T12:00:00Z")
    const wf = workflow("timed", async ({ step }) => { await step.run("a", () => { t += 95_000; return 1 }); return step.run("b", () => 2) })
    const inv = invocation("timed", [], { deadline: "2026-10-09T12:01:40Z" }) // 100 s
    const commands = await execute(inv, wf, new FakeLive(), { now: () => new Date(t) })
    expect(commands).toEqual([{ type: "stepCompleted", stepId: await sid("a"), name: "a", output: 1 }])
  })

  test("a call nobody awaits doesn't become an unhandled rejection", async () => {
    let unhandled = 0
    const onUnhandled = () => { unhandled++ }
    process.on("unhandledRejection", onUnhandled)
    try {
      const wf = workflow("loser", async ({ step }) => { void step.sleep("ignored", "1h"); await step.sleep("wait", "1h") })
      await execute(invocation("loser", [], { cancel: { requestedAt: "2026-10-09T12:00:00Z", graceUntil: "2026-10-09T12:05:00Z" } }), wf, new FakeLive())
      await new Promise((r) => setTimeout(r, 10))
    } finally { process.off("unhandledRejection", onUnhandled) }
    expect(unhandled).toBe(0)
  })
})

describe("replay", () => {
  test("recorded steps return without running", async () => {
    let calls = 0
    const wf = workflow("counted", async ({ step }) => step.run("a", () => { calls++; return 7 }))
    const commands = await execute(invocation("counted", [await entry("a", 0, 1, { output: 7 })]), wf, new FakeLive())
    expect(calls).toBe(0)
    expect(commands).toEqual([{ type: "runCompleted", output: 7 }])
  })

  test("race resolves by seq on replay", async () => {
    const race = workflow("race", async ({ step }) => {
      const w = (await Promise.race([step.run("a", () => "a"), step.run("b", () => "b")])) as string
      return step.run(`after-${w}`, () => w)
    })
    const journal = [await entry("b", 0, 1, { output: "b" }), await entry("a", 0, 2, { output: "a" })]
    const live = new FakeLive()
    const commands = await execute(invocation("race", journal), race, live)
    expect(live.commandsOf(0)).toEqual([{ type: "stepStarted", stepId: await sid("after-b", 0), name: "after-b", attempt: 1, retry: DEFAULT_RETRY }])
    expect(commands).toEqual([
      { type: "stepCompleted", stepId: await sid("after-b", 0), name: "after-b", output: "b" },
      { type: "runCompleted", output: "b" },
    ])
  })

  test("replay-order vector", async () => {
    const race = workflow(replayOrder.workflow, async ({ step }) => {
      const w = (await Promise.race([step.run("a", () => "a"), step.run("b", () => "b")])) as string
      return step.run(`after-${w}`, () => w)
    })
    const live = new FakeLive()
    await execute(invocation(replayOrder.workflow, replayOrder.journal.entries as JournalEntry[]), race, live)
    expect(live.commandsOf(0)[0]).toMatchObject({ type: "stepStarted", name: replayOrder.expectNextStep })
  })

  test("a retrying entry runs as the next attempt", async () => {
    const wf = workflow("flaky", async ({ step }) => step.run("f", () => "ok"))
    const live = new FakeLive()
    await execute(invocation("flaky", [await entry("f", 0, 1, { status: "retrying", attempt: 2, error: { code: "user", name: "Error", message: "boom" } })]), wf, live)
    expect(live.commandsOf(0)[0]).toMatchObject({ type: "stepStarted", attempt: 3 })
  })

  test("idempotencyKey is run, step and attempt", async () => {
    let key = ""
    const wf = workflow("idem", async ({ step }) => step.run("f", () => { key = step.idempotencyKey(); return 1 }))
    await execute(invocation("idem", [await entry("f", 0, 1, { status: "retrying", attempt: 1 })]), wf, new FakeLive())
    expect(key).toBe(`0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a:${await sid("f")}:2`)
  })

  test("a recorded failure is rebuilt as the SDK's own class, with its cause chain", async () => {
    const wf = workflow("caught", async ({ step }) => {
      try { await step.run("bad", () => 1); return null } catch (e) {
        const w = e as WorkflowError
        return { workflowError: e instanceof WorkflowError, code: w.code, name: w.name, data: w.data, cause: (w.cause as Error).message }
      }
    })
    const error = { code: "user", name: "TypeError", message: "inner", data: { k: 1 }, cause: { code: "user", name: "Error", message: "root" } }
    const commands = await execute(invocation("caught", [await entry("bad", 0, 1, { status: "failed", attempt: 5, error })]), wf, new FakeLive())
    expect(commands).toEqual([{ type: "runCompleted", output: { workflowError: true, code: "user", name: "TypeError", data: { k: 1 }, cause: "root" } }])
    expect(decodeError({ code: "workflows.non_retriable", name: "NonRetriableError", message: "x" })).toBeInstanceOf(NonRetriableError)
    expect(decodeError({ code: "workflows.cancelled", name: "CancelledError", message: "x" })).toBeInstanceOf(CancelledError)
  })

  test("an uncaught recorded failure fails the run with causeStepId", async () => {
    const wf = workflow("fails", async ({ step }) => step.run("bad", () => 1))
    const error = { code: "user", name: "Error", message: "boom" }
    const commands = await execute(invocation("fails", [await entry("bad", 0, 1, { status: "failed", attempt: 5, error })]), wf, new FakeLive())
    expect(commands).toEqual([{ type: "runFailed", causeStepId: await sid("bad", 0), error: { code: "user", name: "Error", message: "boom" } }])
  })

  test("recorded values replay unchanged", async () => {
    const wf = workflow("values", async ({ step }) => [await step.now("t"), await step.uuid("id")])
    const journal = [
      await entry("t", 0, 1, { kind: "value", output: "2026-10-09T00:00:00.000Z" }),
      await entry("id", 0, 2, { kind: "value", output: "0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a" }),
    ]
    const commands = await execute(invocation("values", journal), wf, new FakeLive())
    expect(commands).toEqual([{ type: "runCompleted", output: ["2026-10-09T00:00:00.000Z", "0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a"] }])
  })

  test("incomplete journals are paged and outputRefs and inputRef fetched before the code runs", async () => {
    const live = new FakeLive()
    live.pages = [{ complete: true, entries: [await entry("add", 1, 2, { output: 1 })] }]
    live.payloads.set("pay_aaaaaaaaaaaaaaaaaaaaaaaaaa", 0)
    live.payloads.set("pay_bbbbbbbbbbbbbbbbbbbbbbbbbb", { n: 2 })
    const inv = invocation("sequence", [await entry("add", 0, 1, { outputRef: "pay_aaaaaaaaaaaaaaaaaaaaaaaaaa" })])
    inv.journal!.complete = false
    inv.run = { ...inv.run!, input: undefined, inputRef: "pay_bbbbbbbbbbbbbbbbbbbbbbbbbb" }
    expect(await execute(inv, sequence, live)).toEqual([{ type: "runCompleted", output: 1 }])
  })

  test("a step invocation runs only its target step", async () => {
    const wf = workflow("two", async ({ step }) => { await step.run("a", () => 1); return step.run("b", () => 2) })
    const live = new FakeLive()
    const commands = await execute(invocation("two", [await entry("a", 0, 1, { status: "retrying", attempt: 1 })], { kind: "step", targetStepId: await sid("a") }), wf, live)
    expect(live.commandsOf(0)).toEqual([{ type: "stepStarted", stepId: await sid("a"), name: "a", attempt: 2, retry: DEFAULT_RETRY }])
    expect(commands).toEqual([{ type: "stepCompleted", stepId: await sid("a"), name: "a", output: 1 }])
  })
})

describe("cancellation", () => {
  const cancellable = workflow("cancellable", async ({ step }) => {
    try { await step.sleep("wait", "1h"); return "finished" } finally { await step.run("cleanup", () => "cleaned") }
  })
  const cancel = { requestedAt: "2026-10-09T12:00:00Z", graceUntil: "2026-10-09T12:05:00Z" }

  test("the pending step throws CancelledError and cleanup steps run", async () => {
    const live = new FakeLive()
    const journal = [await entry("wait", 0, 1, { kind: "sleep", status: "sleeping" })]
    const commands = await execute(invocation("cancellable", journal, { cancel }), cancellable, live)
    expect(live.commandsOf(0)).toEqual([{ type: "stepStarted", stepId: await sid("cleanup", 0), name: "cleanup", attempt: 1, retry: DEFAULT_RETRY }])
    expect(commands).toEqual([
      { type: "stepCompleted", stepId: await sid("cleanup", 0), name: "cleanup", output: "cleaned" },
      { type: "runFailed", error: expect.objectContaining({ code: "workflows.cancelled", name: "CancelledError" }) },
    ])
  })

  test("replaying a cancelled run takes the same path", async () => {
    const journal = [
      await entry("wait", 0, 1, { kind: "sleep", status: "cancelled" }),
      await entry("cleanup", 0, 2, { output: "cleaned" }),
    ]
    const live = new FakeLive()
    const commands = await execute(invocation("cancellable", journal, { cancel }), cancellable, live)
    expect(live.checkpoints).toEqual([])
    expect(commands).toEqual([{ type: "runFailed", error: expect.objectContaining({ code: "workflows.cancelled" }) }])
  })

  test("sleeping during cleanup is refused", async () => {
    const wf = workflow("c2", async ({ step }) => { try { await step.sleep("a", "1h") } catch { await step.sleep("b", "1s") } })
    const commands = await execute(invocation("c2", [], { cancel }), wf, new FakeLive())
    expect(commands).toEqual([{ type: "runFailed", error: expect.objectContaining({ code: "workflows.cancelled" }) }])
  })
})

describe("errors", () => {
  test("encode and decode keep code, name, data and causes", () => {
    const w = encodeError(new WorkflowError("outer", { code: "user", name: "HttpError", data: { status: 503 }, cause: new Error("inner") }))
    expect(w).toMatchObject({ code: "user", name: "HttpError", message: "outer", data: { status: 503 }, cause: { code: "user", name: "Error", message: "inner" } })
    expect((w.stack ?? "").length).toBeLessThanOrEqual(8192)
  })
  test("cause chains stop at 8 levels: the error and 7 causes", () => {
    let e: Error = new Error("0")
    for (let i = 1; i < 20; i++) e = new Error(String(i), { cause: e })
    let w = encodeError(e)
    let depth = 0
    while (w.cause) { w = w.cause; depth++ }
    expect(depth).toBe(7)
  })
  test("thrown non-Errors are user errors", () => {
    expect(encodeError("plain")).toEqual({ code: "user", name: "Error", message: "plain" })
  })
})

describe("httpLive", () => {
  const cp = { invocationId: "inv_aaaaaaaaaaaaaaaaaaaaaaaaaa", seq: 1, commands: [] }
  const fake = (statuses: number[]) => {
    const calls: { url: string; init: RequestInit }[] = []
    const f = (async (url: string, init: RequestInit) => { calls.push({ url, init }); return new Response("{}", { status: statuses.shift() ?? 204 }) }) as unknown as typeof fetch
    return { calls, f }
  }
  test("posts checkpoints with the invocation token; 204 is applied", async () => {
    const { calls, f } = fake([204])
    await httpLive("https://live.test/", "tok", f, { retryDelayMs: 0 }).checkpoint(cp)
    expect(calls[0]!.url).toBe("https://live.test/v1/invocations/checkpoint")
    expect((calls[0]!.init.headers as Record<string, string>).Authorization).toBe("Bearer tok")
    expect(JSON.parse(String(calls[0]!.init.body))).toEqual(cp)
  })
  test("409 is superseded, 401/400/413 fail at once, 503 retries then fails", async () => {
    await expect(httpLive("https://l", "t", fake([409]).f, { retryDelayMs: 0 }).checkpoint(cp)).rejects.toBeInstanceOf(InvocationSuperseded)
    for (const s of [400, 401, 413]) {
      const { calls, f } = fake([s])
      await expect(httpLive("https://l", "t", f, { retryDelayMs: 0 }).checkpoint(cp)).rejects.toBeInstanceOf(CheckpointFailed)
      expect(calls.length).toBe(1)
    }
    const retried = fake([503, 503, 204])
    await httpLive("https://l", "t", retried.f, { retryDelayMs: 0 }).checkpoint(cp)
    expect(retried.calls.length).toBe(3)
    const down = fake([503, 503, 503, 503, 503])
    await expect(httpLive("https://l", "t", down.f, { retryDelayMs: 0 }).checkpoint(cp)).rejects.toBeInstanceOf(CheckpointFailed)
  })
  test("journal pages and payloads", async () => {
    const calls: string[] = []
    const f = (async (url: string) => { calls.push(url); return Response.json(url.includes("journal") ? { complete: true, entries: [] } : { big: 1 }) }) as unknown as typeof fetch
    const io = httpLive("https://l", "t", f, { retryDelayMs: 0 })
    expect(await io.journalPage(3)).toEqual({ complete: true, entries: [] })
    expect(await io.payload("pay_aaaaaaaaaaaaaaaaaaaaaaaaaa")).toEqual({ big: 1 })
    expect(calls).toEqual(["https://l/v1/invocations/journal?afterSeq=3", "https://l/v1/invocations/payloads/pay_aaaaaaaaaaaaaaaaaaaaaaaaaa"])
  })
})
