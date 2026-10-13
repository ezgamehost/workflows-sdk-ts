import { describe, expect, test } from "bun:test"
import { CancelledError, workflow } from "../src/index.ts"
import type { Command, Invocation, JournalEntry } from "../src/protocol/index.ts"
import { PROTOCOL_VERSIONS } from "../src/protocol/index.ts"
import { validateCommand } from "../src/protocol/validate.ts"
import { execute } from "../src/runtime/execution.ts"
import { FakeLive, invocation, sid } from "./helpers.ts"

const Q = "0199a3d0-1111-7000-8000-00000000aaaa"
const v12 = (wf: string, entries: JournalEntry[] = [], extra: Partial<Invocation> = {}) => invocation(wf, entries, { protocolVersion: "1.2", ...extra })
const cmds = (xs: Command[]) => { xs.forEach(validateCommand); return xs }
const MSG = { messageId: "0199a3d0-2222-7000-8000-000000000001", body: "paid", attributes: { orderId: "order:o-1" }, sentAt: "2026-10-13T09:00:00.123Z", queueId: Q }
const message = (status: string, rest: Partial<JournalEntry> = {}): Promise<JournalEntry> =>
  sid("paid").then((stepId) => ({ seq: 2, createdSeq: 1, stepId, name: "paid", kind: "message", status, attempt: 1, queueId: Q, key: "order:o-1", ...rest }))

const waitmessage = workflow("waitmessage", async ({ step }) => {
  const m = await step.waitForMessage("paid", { queueId: Q, key: "order:o-1", timeoutSeconds: 3600 })
  return m === null ? { received: false } : { received: true, body: m.body, messageId: m.messageId }
})

describe("step.waitForMessage (protocol 1.2)", () => {
  test("SDKs declare 1.2", () => expect(PROTOCOL_VERSIONS).toEqual(["1.0", "1.1", "1.2"]))

  test("messageWaitCreated is acknowledged in a checkpoint, then the invocation suspends with waitForMessage", async () => {
    const live = new FakeLive()
    const out = cmds(await execute(v12("waitmessage"), waitmessage, live))
    expect(live.checkpoints.map((c) => cmds(c.commands))).toEqual([[{ type: "messageWaitCreated", stepId: await sid("paid"), name: "paid", queueId: Q, key: "order:o-1", timeoutMs: 3_600_000 }]])
    expect(out).toEqual([{ type: "waitForMessage", stepId: await sid("paid") }])
  })

  test("replay: a waiting entry suspends again without creating it twice", async () => {
    const live = new FakeLive()
    const out = await execute(v12("waitmessage", [await message("waiting", { seq: 1 })]), waitmessage, live)
    expect(live.checkpoints).toEqual([])
    expect(out).toEqual([{ type: "waitForMessage", stepId: await sid("paid") }])
  })

  test("replay: a completed entry returns the message; timedOut returns null", async () => {
    const got = await execute(v12("waitmessage", [await message("completed", { output: MSG })]), waitmessage, new FakeLive())
    expect(got).toEqual([{ type: "runCompleted", output: { received: true, body: "paid", messageId: MSG.messageId } }])
    const timedOut = await execute(v12("waitmessage", [await message("completed", { timedOut: true })]), waitmessage, new FakeLive())
    expect(timedOut).toEqual([{ type: "runCompleted", output: { received: false } }])
  })

  test("replay: an offloaded message is fetched from the payload store", async () => {
    const live = new FakeLive()
    live.payloads.set("pay_aaaaaaaaaaaaaaaaaaaaaaaaaa", MSG)
    const got = await execute(v12("waitmessage", [await message("completed", { outputRef: "pay_aaaaaaaaaaaaaaaaaaaaaaaaaa" })]), waitmessage, live)
    expect(got).toEqual([{ type: "runCompleted", output: { received: true, body: "paid", messageId: MSG.messageId } }])
  })

  test("a cancelled wait throws the cancellation error", async () => {
    let caught: unknown
    const wf = workflow("waitmessage", async ({ step }) => {
      try { return await step.waitForMessage("paid", { queueId: Q, key: "order:o-1", timeoutSeconds: 3600 }) } catch (e) { caught = e; throw e }
    })
    const entry = await message("cancelled", { error: { code: "workflows.cancelled", name: "CancelledError", message: "The run was cancelled" } })
    const out = await execute(v12("waitmessage", [entry], { cancel: { requestedAt: "2026-10-13T09:00:00Z", graceUntil: "2099-01-01T00:00:00Z" } }), wf, new FakeLive())
    expect(caught).toBeInstanceOf(CancelledError)
    expect(out).toMatchObject([{ type: "runFailed", error: { code: "workflows.cancelled" } }])
  })

  test("a wait beside a running step: the run goes inline, then the wait is created and suspends", async () => {
    const wf = workflow("both", async ({ step }) => Promise.all([
      step.run("work", () => 1),
      step.waitForMessage("paid", { queueId: Q, key: "k", timeoutSeconds: 1 }),
    ]))
    const live = new FakeLive()
    const out = cmds(await execute(v12("both"), wf, live))
    expect(live.checkpoints.flatMap((c) => c.commands.map((x) => x.type))).toEqual(["stepStarted", "stepCompleted", "messageWaitCreated"])
    expect(out).toEqual([{ type: "waitForMessage", stepId: await sid("paid") }])
  })

  test("before 1.2 the run fails non-retriably; bad options reject", async () => {
    const out = await execute(invocation("waitmessage", [], { protocolVersion: "1.1" }), waitmessage, new FakeLive())
    expect(out).toMatchObject([{ type: "runFailed", error: { code: "workflows.non_retriable", message: expect.stringContaining("needs protocol 1.2") } }])
    for (const opts of [
      { queueId: "que_abc", key: "k", timeoutSeconds: 60 },
      { queueId: Q, key: "has space", timeoutSeconds: 60 },
      { queueId: Q, key: "k", timeoutSeconds: 0.5 },
      { queueId: Q, key: "k", timeoutSeconds: 2_592_001 },
    ]) {
      const wf = workflow("bad", async ({ step }) => step.waitForMessage("w", opts))
      const res = await execute(v12("bad"), wf, new FakeLive())
      expect([opts, res]).toMatchObject([opts, [{ type: "runFailed", error: { name: "TypeError" } }]])
    }
  })
})
