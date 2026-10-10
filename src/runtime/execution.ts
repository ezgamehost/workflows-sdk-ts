import type { Command, Invocation, JournalEntry, PlannedStep, RetryPolicy, WireError } from "../protocol/index.ts"
import { DEFAULT_RETRY, MAX_OUTPUT_BYTES, encodeValue, jsonByteLength, mintCallbackToken, parseDuration, shardOf, stepId, supportsParallel, validStepIdOverride, validWorkflowName } from "../protocol/index.ts"
import { utf8Length } from "../protocol/bytes.ts"
import type { Callback, Step, StepRunContext, StepRunOptions, WorkflowContext } from "./context.ts"
import { CancelledError, DuplicateStepName, NonRetriableError, OutputTooLarge, RetryAfterError, StepTimeout, decodeError, encodeError } from "./errors.ts"
import type { LiveIO } from "./live.ts"
import { checkTimeout, definitionOf, mergeRetry, type Workflow, type WorkflowDefinition } from "./workflow.ts"

export interface ExecOptions {
  now?: () => Date
  maxStepsPerInvocation?: number
  // The invocation's deadline on this machine's clock (epoch ms). serve() passes receipt time +
  // (invocation.deadline − the signature's t), so clock skew doesn't matter; default: deadline as sent.
  deadline?: number
  // The public API base for callback URLs when the invocation carries no endpoints.api.
  apiBaseUrl?: string
  // Aborted when the run is cancelling (a pull worker's heartbeat says so): every running step's
  // signal aborts with a CancelledError.
  cancelSignal?: AbortSignal
}

// The longest sleep: a run times out after a year at most.
export const MAX_SLEEP_MS = 31_536_000_000
// The longest retryAfterMs the engine takes (a day).
export const MAX_RETRY_AFTER_MS = 86_400_000
// Callback timeouts: default 7 days, from 1 s to a year.
export const DEFAULT_CALLBACK_TIMEOUT_MS = 604_800_000
export const MIN_CALLBACK_TIMEOUT_MS = 1_000
export const DEFAULT_API_BASE = "https://workflows-api.ezghcloud.com"

// run, sleep, value: phase 1. callback: creating a callback; wait: Callback.wait() (the callback's
// own step ID); invoke: a child run.
type Kind = "run" | "sleep" | "value" | "callback" | "wait" | "invoke"
interface Call {
  order: number; name: string; kind: Kind; id: Promise<string>; stepId?: string; settled: boolean; explicit?: string
  resolve(v: unknown): void; reject(e: unknown): void
  fn?: (ctx: StepRunContext) => unknown; retry?: RetryPolicy; timeoutMs?: number; untilMs?: number; produce?: () => unknown
  workflow?: string; input?: unknown
}
// What the frontier does next: run one step inline, or end the invocation with these commands.
type Next = { kind: "inline"; call: Call; attempt: number } | { kind: "end"; commands: Command[] }

const RECORDED = new Set(["completed", "failed", "cancelled"])
const idle = () => new Promise<void>((r) => setTimeout(r, 0))
// quiet marks a promise handled, so a call the workflow never awaits (a race's loser, say) isn't an
// unhandled rejection; whoever awaits it still sees the rejection.
const quiet = <T>(p: Promise<T>): Promise<T> => { p.catch(() => {}); return p }

// Execution replays a workflow against its journal and runs it forward (plan S2 and T1 "Semantics"):
//  - recorded outcomes resolve one at a time, lowest seq first, with a macrotask yield before each;
//  - protocol 1.0: at the frontier the oldest pending call (by call order) decides: run sends
//    stepStarted and awaits its acknowledgement before running; sleep ends the invocation; value
//    is recorded;
//  - protocol 1.1 (the frontier rule D2-19, the same in every SDK): the oldest new call decides: a
//    value is recorded, a callback is created (acknowledged before its URL is visible);
//    otherwise one lone actionable step runs inline and anything more is planned (planParallel)
//    and suspended (sleep, invoke, waitForCallback) in one result;
//  - a step invocation runs exactly its target step;
//  - a failed attempt ends the invocation with stepFailed; the step and time budgets end it with
//    no terminal command; the workflow's return or throw ends it with runCompleted or runFailed.
class Execution {
  private calls: Call[] = []
  private counters = new Map<string, number>()
  private journal = new Map<string, JournalEntry>()
  private outbox: Command[] = []
  private cpSeq = 0
  private executed = 0
  private cancelThrown = false
  private fatal?: WireError
  private finished?: { ok: true; output: unknown } | { ok: false; error: unknown }
  private stepOfError = new WeakMap<object, string>()
  private explicitIds = new Set<string>()
  private currentKey?: string
  private wake?: () => void
  private readonly started: number
  private readonly deadline: number
  private readonly parallel: boolean
  private readonly apiBase: string

  constructor(private inv: Invocation, private wf: WorkflowDefinition, private io: LiveIO, private now: () => Date, private maxSteps: number, private opts: ExecOptions) {
    this.started = now().getTime()
    this.deadline = opts.deadline ?? Date.parse(inv.deadline)
    this.parallel = supportsParallel(inv.protocolVersion)
    this.apiBase = (inv.endpoints.api || opts.apiBaseUrl || DEFAULT_API_BASE).replace(/\/+$/, "")
  }

  private register(kind: Kind, name: string, explicitId: string | undefined, extra: Partial<Call>): Promise<unknown> {
    if (typeof name !== "string" || name.length === 0 || utf8Length(name) > 256 || name.includes("\u0000"))
      return quiet(Promise.reject(new TypeError("A step name is 1–256 bytes of UTF-8, without NUL")))
    if (explicitId !== undefined && !validStepIdOverride(explicitId)) return quiet(Promise.reject(new TypeError(`Step IDs match ^[A-Za-z0-9._:-]{1,128}$; got "${explicitId}"`)))
    // The engine wouldn't understand these at protocol 1.0: the run fails rather than misbehaves.
    if ((kind === "callback" || kind === "invoke") && !this.parallel && !this.fatal)
      this.fatal = encodeError(new NonRetriableError(`step.${kind === "callback" ? "createCallback" : "invoke"} needs protocol 1.1; this invocation is ${this.inv.protocolVersion}`))
    let id: Promise<string>
    if (explicitId !== undefined) {
      id = Promise.resolve(explicitId)
      // An explicit id names one step for the whole run: reusing it, even after the first step
      // settled, would replay the first step's outcome as the second's.
      if (!this.fatal && this.explicitIds.has(explicitId))
        this.fatal = encodeError(new DuplicateStepName(`The step id "${explicitId}" is used twice; give each step its own id`))
      this.explicitIds.add(explicitId)
    } else {
      const n = this.counters.get(name) ?? 0
      this.counters.set(name, n + 1)
      id = stepId(name, n)
      if (!this.fatal && this.calls.some((c) => !c.settled && !c.explicit && c.kind !== "wait" && c.name === name))
        this.fatal = encodeError(new DuplicateStepName(`Two steps named "${name}" are pending at once; give each a distinct name or an { id }`))
    }
    return quiet(new Promise((resolve, reject) => {
      this.calls.push({ order: this.calls.length, name, kind, id, settled: false, ...(explicitId !== undefined ? { explicit: explicitId } : {}), resolve, reject, ...extra })
      this.wake?.()
    }))
  }

  // waitFor registers Callback.wait(): a pending call on the callback's own step ID that neither
  // advances the name's counter nor counts as a duplicate.
  private waitFor(sid: string, name: string): Promise<unknown> {
    return quiet(new Promise((resolve, reject) => {
      this.calls.push({ order: this.calls.length, name, kind: "wait", id: Promise.resolve(sid), stepId: sid, settled: false, resolve, reject })
      this.wake?.()
    }))
  }

  private callbackOf(sid: string, name: string, token: string): Callback<unknown> {
    const url = `${this.apiBase}/public/v1/callbacks/${token}`
    return Object.freeze({ url, token, wait: () => this.waitFor(sid, name) as Promise<never> })
  }

  context(): WorkflowContext<unknown> {
    const self = this
    const step: Step = {
      run(name, fn, opts: StepRunOptions = {}) {
        let retry: RetryPolicy
        try {
          if (typeof fn !== "function") throw new TypeError("step.run needs a function")
          if (opts.timeoutMs !== undefined) checkTimeout(opts.timeoutMs)
          retry = mergeRetry(self.wf.opts.retry ?? DEFAULT_RETRY, opts.retry)
        } catch (e) { return quiet(Promise.reject(e)) }
        const timeoutMs = opts.timeoutMs ?? self.wf.opts.timeoutMs
        return self.register("run", name, opts.id, { fn, retry, ...(timeoutMs ? { timeoutMs } : {}) }) as never
      },
      sleep(name, duration, opts = {}) {
        let ms: number
        try {
          ms = parseDuration(duration)
          if (ms > MAX_SLEEP_MS) throw new TypeError("A sleep is at most a year")
        } catch (e) { return quiet(Promise.reject(e)) }
        return self.register("sleep", name, opts.id, { untilMs: self.now().getTime() + ms }) as Promise<void>
      },
      sleepUntil(name, at, opts = {}) {
        const t = typeof at === "string" ? Date.parse(at) : at instanceof Date ? at.getTime() : NaN
        if (!Number.isFinite(t)) return quiet(Promise.reject(new TypeError("sleepUntil needs a valid time")))
        const now = self.now().getTime()
        if (t - now > MAX_SLEEP_MS) return quiet(Promise.reject(new TypeError("A sleep is at most a year")))
        // A time in the past sleeps until now (and stays a valid RFC 3339 time).
        return self.register("sleep", name, opts.id, { untilMs: Math.max(t, now) }) as Promise<void>
      },
      now(name) {
        return quiet(self.register("value", name, undefined, { produce: () => self.now().toISOString() }).then((v) => new Date(v as string)))
      },
      uuid(name) {
        return quiet(self.register("value", name, undefined, { produce: () => crypto.randomUUID() }) as Promise<string>)
      },
      createCallback(name, opts = {}) {
        const timeoutMs = opts?.timeoutMs ?? DEFAULT_CALLBACK_TIMEOUT_MS
        if (!Number.isInteger(timeoutMs) || timeoutMs < MIN_CALLBACK_TIMEOUT_MS || timeoutMs > MAX_SLEEP_MS)
          return quiet(Promise.reject(new TypeError("A callback's timeoutMs is a whole number from 1,000 to 31,536,000,000")))
        return self.register("callback", name, opts?.id, { timeoutMs }) as never
      },
      invoke(name, opts) {
        let workflow: string
        let input: unknown
        try {
          const w = opts?.workflow
          workflow = typeof w === "string" ? w : typeof w === "object" && w !== null ? w.name : ""
          if (!validWorkflowName(workflow)) throw new TypeError(`step.invoke needs a workflow name matching ^[A-Za-z0-9_-]{1,64}$; got "${workflow}"`)
          if (opts.input !== undefined) {
            try { input = encodeValue(opts.input) } catch (e) { throw new TypeError(`step.invoke's input isn't JSON: ${(e as Error).message}`) }
            if (jsonByteLength(input) > MAX_OUTPUT_BYTES) throw new TypeError("step.invoke's input is at most 1 MiB of JSON")
          }
        } catch (e) { return quiet(Promise.reject(e)) }
        return self.register("invoke", name, opts.id, { workflow, ...(input !== undefined ? { input } : {}) }) as never
      },
      idempotencyKey() {
        if (!self.currentKey) throw new Error("idempotencyKey() is only available inside step.run")
        return self.currentKey
      },
    }
    const run = this.inv.run!
    const schedule = run.schedule && typeof run.schedule === "object" ? { id: run.schedule.id, scheduledTime: run.schedule.scheduledTime } : undefined
    return { input: run.input, run: { id: run.id, ...(run.runKey ? { runKey: run.runKey } : {}), attempt: run.attempt, ...(schedule ? { schedule } : {}) }, step }
  }

  // load pages the rest of an incomplete journal and fetches offloaded outputs and input, so the
  // workflow code never waits on I/O during replay.
  private async load(): Promise<void> {
    const entries = this.inv.journal!.entries.map((e) => ({ ...e }))
    let complete = this.inv.journal!.complete
    while (!complete) {
      const last = entries.reduce((m, e) => Math.max(m, e.seq), 0)
      const page = await this.io.journalPage(last)
      entries.push(...page.entries.map((e) => ({ ...e })))
      complete = page.complete || page.entries.length === 0
    }
    const refs = entries.filter((e) => e.outputRef)
    for (let i = 0; i < refs.length; i += 8)
      await Promise.all(refs.slice(i, i + 8).map(async (e) => { e.output = await this.io.payload(e.outputRef!); delete e.outputRef }))
    if (this.inv.run!.inputRef) this.inv.run = { ...this.inv.run!, input: await this.io.payload(this.inv.run!.inputRef) }
    for (const e of entries) this.journal.set(e.stepId, e)
  }

  private timeLeftOk(): boolean {
    const total = this.deadline - this.started
    return this.deadline - this.now().getTime() > Math.max(5000, total * 0.1)
  }

  private end(commands: Command[]): Command[] {
    return [...this.outbox.splice(0), ...commands]
  }

  private async checkpoint(commands: Command[]): Promise<void> {
    this.cpSeq++
    await this.io.checkpoint({ invocationId: this.inv.invocationId, seq: this.cpSeq, commands })
  }

  // recordedEntry: the journal entry that settles c, if any. A callback's creation is recorded once
  // its token is journaled (whatever the callback's status); every other call once its step has an
  // outcome.
  private recordedEntry(c: Call): JournalEntry | undefined {
    const e = this.journal.get(c.stepId!)
    if (!e) return undefined
    if (c.kind === "callback") return e.token ? e : undefined
    return RECORDED.has(e.status) ? e : undefined
  }

  private settleRecorded(c: Call, e: JournalEntry): void {
    c.settled = true
    if (c.kind === "callback") return c.resolve(this.callbackOf(c.stepId!, c.name, e.token!))
    if (e.status === "completed") return c.resolve(e.kind === "sleep" ? undefined : (e.output ?? null))
    if (e.status === "cancelled") { this.cancelThrown = true; return c.reject(new CancelledError("The run was cancelled")) }
    const err = decodeError(e.error ?? { code: "user", name: "Error", message: "Step failed" })
    this.stepOfError.set(err, c.stepId!)
    c.reject(err)
  }

  private async runStep(c: Call, attempt: number): Promise<Command | undefined> {
    const sid = c.stepId!
    await this.checkpoint([...this.outbox.splice(0), { type: "stepStarted", stepId: sid, name: c.name, attempt, retry: c.retry!, ...(c.timeoutMs ? { timeoutMs: c.timeoutMs } : {}) }])
    const key = `${this.inv.run!.id}:${sid}:${attempt}`
    this.currentKey = key
    const abort = new AbortController()
    const onCancel = () => { if (!abort.signal.aborted) abort.abort(new CancelledError("The run is cancelling")) }
    if (this.opts.cancelSignal?.aborted) onCancel()
    else this.opts.cancelSignal?.addEventListener("abort", onCancel, { once: true })
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      const work = Promise.resolve().then(() => c.fn!({ signal: abort.signal, attempt, idempotencyKey: key }))
      const raw = c.timeoutMs
        ? await Promise.race([work, new Promise((_, rej) => {
          timer = setTimeout(() => {
            const e = new StepTimeout(`Step "${c.name}" ran past ${c.timeoutMs} ms`)
            abort.abort(e)
            rej(e)
          }, c.timeoutMs)
        })])
        : await work
      let value: unknown
      try { value = encodeValue(raw) } catch (e) { throw new NonRetriableError((e as Error).message) }
      if (jsonByteLength(value) > MAX_OUTPUT_BYTES) throw new OutputTooLarge(`Step "${c.name}" returned more than 1 MiB of JSON`)
      this.executed++
      c.settled = true
      c.resolve(value)
      this.outbox.push({ type: "stepCompleted", stepId: sid, name: c.name, output: value })
      return undefined
    } catch (e) {
      const retryable = !(e instanceof NonRetriableError || e instanceof OutputTooLarge)
      const cmd: Command = { type: "stepFailed", stepId: sid, name: c.name, attempt, error: encodeError(e), retryable }
      if (e instanceof RetryAfterError && typeof e.delayMs === "number" && !Number.isNaN(e.delayMs))
        cmd.retryAfterMs = Math.min(MAX_RETRY_AFTER_MS, Math.max(0, Math.floor(e.delayMs)))
      return cmd
    } finally {
      if (timer !== undefined) clearTimeout(timer)
      this.opts.cancelSignal?.removeEventListener("abort", onCancel)
      this.currentKey = undefined
    }
  }

  // createCallback mints the token (the run's shard), records it in an acknowledged checkpoint,
  // and only then resolves the Callback, so its URL is never seen before the engine knows it.
  private async createCallback(c: Call): Promise<void> {
    const shard = shardOf(this.inv.run!.id)
    if (shard === undefined) throw new TypeError("The run ID carries no shard; callbacks need a v7 or v8 run ID")
    const token = mintCallbackToken(shard)
    await this.checkpoint([...this.outbox.splice(0), { type: "callbackCreated", stepId: c.stepId!, name: c.name, token, timeoutMs: c.timeoutMs! }])
    // A wait() later in this invocation finds the callback waiting.
    this.journal.set(c.stepId!, { seq: Number.MAX_SAFE_INTEGER, stepId: c.stepId!, name: c.name, kind: "callback", status: "waiting", attempt: 1, token })
    c.settled = true
    c.resolve(this.callbackOf(c.stepId!, c.name, token))
  }

  private planned(c: Call): PlannedStep {
    return { stepId: c.stepId!, name: c.name, retry: c.retry!, ...(c.timeoutMs ? { timeoutMs: c.timeoutMs } : {}) }
  }

  // plan (protocol 1.1, the frontier rule D2-19, identical in every SDK): one actionable step with
  // nothing else pending runs inline; otherwise every new step is planned together and every
  // suspension sent with it, in call order. A retrying step alongside other work waits
  // (planParallel skips existing steps); a running entry (a planned step at attempt 0 too) is
  // awaited, never re-run.
  private plan(pending: Call[]): Next {
    const runs: Call[] = []
    const retrying: Call[] = []
    let busy = 0
    const suspends: Command[] = []
    const waited = new Set<string>()
    for (const p of pending) {
      const e = this.journal.get(p.stepId!)
      switch (p.kind) {
        case "run":
          if (!e) runs.push(p)
          else if (e.status === "retrying") retrying.push(p)
          else busy++
          break
        case "sleep":
          if (!e) suspends.push({ type: "sleep", stepId: p.stepId!, name: p.name, until: new Date(p.untilMs!).toISOString() })
          else busy++
          break
        case "invoke":
          if (!e) suspends.push({ type: "invoke", stepId: p.stepId!, name: p.name, workflow: p.workflow!, ...(p.input !== undefined ? { input: p.input } : {}) })
          else busy++
          break
        case "wait":
          // Sent for every pending wait, even one already waiting: a no-op for the engine.
          if (!waited.has(p.stepId!)) { waited.add(p.stepId!); suspends.push({ type: "waitForCallback", stepId: p.stepId! }) }
          break
        // value and callback calls are handled when they're the oldest new call.
      }
    }
    if (busy === 0 && suspends.length === 0 && runs.length + retrying.length === 1) {
      const c = runs[0] ?? retrying[0]!
      const e = this.journal.get(c.stepId!)
      return { kind: "inline", call: c, attempt: e?.status === "retrying" ? e.attempt + 1 : 1 }
    }
    return { kind: "end", commands: [...(runs.length > 0 ? [{ type: "planParallel" as const, steps: runs.map((r) => this.planned(r)) }] : []), ...suspends] }
  }

  // sequential (protocol 1.0, and while cancelling): phase 1, exactly.
  private sequential(c: Call): Next {
    const entry = this.journal.get(c.stepId!)
    // Any other recorded status (sleeping, running, waiting, or one a later version adds) means
    // the engine is still waiting on it: end the invocation; only a retrying step runs again.
    if (entry && entry.status !== "retrying") return { kind: "end", commands: [] }
    if (c.kind === "sleep") return { kind: "end", commands: [{ type: "sleep", stepId: c.stepId!, name: c.name, until: new Date(c.untilMs!).toISOString() }] }
    return { kind: "inline", call: c, attempt: entry?.status === "retrying" ? entry.attempt + 1 : 1 }
  }

  // stepInvocation runs exactly the target step, at its journal attempt + 1, and ends. It never
  // records values, plans or suspends.
  private async stepInvocation(pending: Call[]): Promise<Command[]> {
    const target = pending.find((c) => c.kind === "run" && c.stepId === this.inv.targetStepId)
    if (!target || !this.timeLeftOk()) return this.end([])
    const entry = this.journal.get(target.stepId!)
    const failed = await this.runStep(target, (entry?.attempt ?? 0) + 1)
    return this.end(failed ? [failed] : [])
  }

  private terminal(): Command[] {
    const f = this.finished!
    if (f.ok) {
      let output: unknown
      try { output = encodeValue(f.output) } catch (e) { return [{ type: "runFailed", error: encodeError(new NonRetriableError((e as Error).message)) }] }
      if (jsonByteLength(output) > MAX_OUTPUT_BYTES) return [{ type: "runFailed", error: encodeError(new OutputTooLarge("The workflow returned more than 1 MiB of JSON")) }]
      return [{ type: "runCompleted", output }]
    }
    const cause = typeof f.error === "object" && f.error !== null ? this.stepOfError.get(f.error) : undefined
    return [{ type: "runFailed", error: encodeError(f.error), ...(cause ? { causeStepId: cause } : {}) }]
  }

  async run(): Promise<Command[]> {
    await this.load()
    const done = Promise.resolve()
      .then(() => this.wf.fn(this.context()))
      .then((output) => { this.finished = { ok: true, output } }, (error: unknown) => { this.finished = { ok: false, error } })
      .finally(() => this.wake?.())
    for (;;) {
      await idle()
      if (this.fatal) return this.end([{ type: "runFailed", error: this.fatal }])
      if (this.finished) return this.end(this.terminal())
      const pending = this.calls.filter((c) => !c.settled)
      if (pending.length === 0) {
        await Promise.race([done, new Promise<void>((r) => (this.wake = r))])
        this.wake = undefined
        continue
      }
      for (const c of pending) c.stepId ??= await c.id
      const recorded = pending
        .map((c) => [c, this.recordedEntry(c)] as const)
        .filter(([, e]) => e !== undefined)
        .sort((a, b) => a[1]!.seq - b[1]!.seq || a[0].order - b[0].order)
      if (recorded.length > 0) { this.settleRecorded(recorded[0]![0], recorded[0]![1]!); continue }
      pending.sort((a, b) => a.order - b.order)
      if (this.inv.kind === "step") return this.stepInvocation(pending)
      const c = pending[0]!
      if (this.inv.cancel && !this.cancelThrown) { this.cancelThrown = true; c.settled = true; c.reject(new CancelledError("The run was cancelled")); continue }
      if (this.cancelThrown && c.kind !== "run") { c.settled = true; c.reject(new CancelledError("The run is cancelling: only step.run cleanup steps can run")); continue }
      // The deciding call: at 1.1 the oldest call with no journal entry (D2-19; calls the engine
      // owns don't hold values back), at 1.0 and while cancelling the oldest pending call (phase 1).
      const head = this.parallel && !this.cancelThrown ? pending.find((p) => !this.journal.has(p.stepId!)) : c
      if (head?.kind === "value" && !(this.journal.has(head.stepId!))) {
        const value = encodeValue(head.produce!())
        this.outbox.push({ type: "valueRecorded", stepId: head.stepId!, name: head.name, value })
        head.settled = true
        head.resolve(value)
        continue
      }
      if (head?.kind === "callback" && !this.journal.has(head.stepId!)) {
        try { await this.createCallback(head) } catch (e) {
          if (e instanceof TypeError) { head.settled = true; head.reject(e); continue }
          throw e // InvocationSuperseded, CheckpointFailed: the invocation stops
        }
        continue
      }
      const next = this.parallel && !this.cancelThrown ? this.plan(pending) : this.sequential(c)
      if (next.kind === "end") return this.end(next.commands)
      if (this.executed >= this.maxSteps || !this.timeLeftOk()) return this.end([])
      const failed = await this.runStep(next.call, next.attempt)
      if (failed) return this.end([failed])
    }
  }
}

// execute runs one invocation of wf and returns the commands of its InvocationResult. It throws
// InvocationSuperseded or CheckpointFailed when a checkpoint isn't applied.
export function execute(inv: Invocation, wf: Workflow<any, any>, io: LiveIO, opts: ExecOptions = {}): Promise<Command[]> {
  return new Execution(inv, definitionOf(wf), io, opts.now ?? (() => new Date()), opts.maxStepsPerInvocation ?? 50, opts).run()
}
