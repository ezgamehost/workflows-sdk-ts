import type { Command, Invocation, JournalEntry, RetryPolicy, WireError } from "../protocol/index.ts"
import { DEFAULT_RETRY, MAX_OUTPUT_BYTES, encodeValue, jsonByteLength, parseDuration, stepId, validStepIdOverride } from "../protocol/index.ts"
import { utf8Length } from "../protocol/bytes.ts"
import type { Step, StepRunContext, StepRunOptions, WorkflowContext } from "./context.ts"
import { CancelledError, DuplicateStepName, NonRetriableError, OutputTooLarge, RetryAfterError, StepTimeout, decodeError, encodeError } from "./errors.ts"
import type { LiveIO } from "./live.ts"
import { checkTimeout, mergeRetry, type Workflow } from "./workflow.ts"

export interface ExecOptions {
  now?: () => Date
  maxStepsPerInvocation?: number
  // The invocation's deadline on this machine's clock (epoch ms). serve() passes receipt time +
  // (invocation.deadline − the signature's t), so clock skew doesn't matter; default: deadline as sent.
  deadline?: number
}

// The longest sleep: a run times out after a year at most.
export const MAX_SLEEP_MS = 31_536_000_000

type Kind = "run" | "sleep" | "value"
interface Call {
  order: number; name: string; kind: Kind; id: Promise<string>; stepId?: string; settled: boolean; explicit?: string
  resolve(v: unknown): void; reject(e: unknown): void
  fn?: (ctx: StepRunContext) => unknown; retry?: RetryPolicy; timeoutMs?: number; untilMs?: number; produce?: () => unknown
}

const RECORDED = new Set(["completed", "failed", "cancelled"])
const idle = () => new Promise<void>((r) => setTimeout(r, 0))
// quiet marks a promise handled, so a call the workflow never awaits (a race's loser, say) isn't an
// unhandled rejection; whoever awaits it still sees the rejection.
const quiet = <T>(p: Promise<T>): Promise<T> => { p.catch(() => {}); return p }

// Execution replays a workflow against its journal and runs it forward (plan S2 "Semantics"):
//  - recorded outcomes resolve one at a time, lowest seq first, with a macrotask yield before each;
//  - at the frontier the oldest pending call (by call order) decides: run sends stepStarted and
//    awaits its acknowledgement before running; sleep ends the invocation; value is recorded;
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
  private currentKey?: string
  private wake?: () => void
  private readonly started: number
  private readonly deadline: number

  constructor(private inv: Invocation, private wf: Workflow, private io: LiveIO, private now: () => Date, private maxSteps: number, deadline?: number) {
    this.started = now().getTime()
    this.deadline = deadline ?? Date.parse(inv.deadline)
  }

  private register(kind: Kind, name: string, explicitId: string | undefined, extra: Partial<Call>): Promise<unknown> {
    if (typeof name !== "string" || name.length === 0 || utf8Length(name) > 256) return quiet(Promise.reject(new TypeError("A step name is 1–256 bytes of UTF-8")))
    if (explicitId !== undefined && !validStepIdOverride(explicitId)) return quiet(Promise.reject(new TypeError(`Step IDs match ^[A-Za-z0-9._:-]{1,128}$; got "${explicitId}"`)))
    let id: Promise<string>
    if (explicitId !== undefined) {
      id = Promise.resolve(explicitId)
      if (!this.fatal && this.calls.some((c) => !c.settled && c.explicit === explicitId))
        this.fatal = encodeError(new DuplicateStepName(`Two steps with the id "${explicitId}" are pending at once; give each its own id`))
    } else {
      const n = this.counters.get(name) ?? 0
      this.counters.set(name, n + 1)
      id = stepId(name, n)
      if (!this.fatal && this.calls.some((c) => !c.settled && !c.explicit && c.name === name))
        this.fatal = encodeError(new DuplicateStepName(`Two steps named "${name}" are pending at once; give each a distinct name or an { id }`))
    }
    return quiet(new Promise((resolve, reject) => {
      this.calls.push({ order: this.calls.length, name, kind, id, settled: false, ...(explicitId !== undefined ? { explicit: explicitId } : {}), resolve, reject, ...extra })
      this.wake?.()
    }))
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
        if (t - self.now().getTime() > MAX_SLEEP_MS) return quiet(Promise.reject(new TypeError("A sleep is at most a year")))
        return self.register("sleep", name, opts.id, { untilMs: t }) as Promise<void>
      },
      now(name) {
        return quiet(self.register("value", name, undefined, { produce: () => self.now().toISOString() }).then((v) => new Date(v as string)))
      },
      uuid(name) {
        return quiet(self.register("value", name, undefined, { produce: () => crypto.randomUUID() }) as Promise<string>)
      },
      idempotencyKey() {
        if (!self.currentKey) throw new Error("idempotencyKey() is only available inside step.run")
        return self.currentKey
      },
    }
    const run = this.inv.run!
    return { input: run.input, run: { id: run.id, ...(run.runKey ? { runKey: run.runKey } : {}), attempt: run.attempt }, step }
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

  private settleRecorded(c: Call, e: JournalEntry): void {
    c.settled = true
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
      if (e instanceof RetryAfterError && Number.isFinite(e.delayMs)) cmd.retryAfterMs = Math.max(0, Math.floor(e.delayMs))
      return cmd
    } finally {
      if (timer !== undefined) clearTimeout(timer)
      this.currentKey = undefined
    }
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
        .map((c) => [c, this.journal.get(c.stepId!)] as const)
        .filter(([, e]) => e !== undefined && RECORDED.has(e.status))
        .sort((a, b) => a[1]!.seq - b[1]!.seq)
      if (recorded.length > 0) { this.settleRecorded(recorded[0]![0], recorded[0]![1]!); continue }
      const c = pending.sort((a, b) => a.order - b.order)[0]!
      if (this.inv.cancel && !this.cancelThrown) { this.cancelThrown = true; c.settled = true; c.reject(new CancelledError("The run was cancelled")); continue }
      if (this.inv.kind === "step" && c.stepId !== this.inv.targetStepId) return this.end([])
      const entry = this.journal.get(c.stepId!)
      // Any other recorded status (sleeping, running, waiting, or one a later version adds) means
      // the engine is still waiting on it: end the invocation; only a retrying step runs again.
      if (entry && entry.status !== "retrying") return this.end([])
      if (this.cancelThrown && c.kind !== "run") { c.settled = true; c.reject(new CancelledError("The run is cancelling: only step.run cleanup steps can run")); continue }
      if (c.kind === "value") {
        const value = encodeValue(c.produce!())
        this.outbox.push({ type: "valueRecorded", stepId: c.stepId!, name: c.name, value })
        c.settled = true
        c.resolve(value)
        continue
      }
      if (c.kind === "sleep") return this.end([{ type: "sleep", stepId: c.stepId!, name: c.name, until: new Date(c.untilMs!).toISOString() }])
      if (this.executed >= this.maxSteps || !this.timeLeftOk()) return this.end([])
      const attempt = entry?.status === "retrying" ? entry.attempt + 1 : 1
      const failed = await this.runStep(c, attempt)
      if (failed) return this.end([failed])
      if (this.inv.kind === "step") return this.end([])
    }
  }
}

// execute runs one invocation of wf and returns the commands of its InvocationResult. It throws
// InvocationSuperseded or CheckpointFailed when a checkpoint isn't applied.
export function execute(inv: Invocation, wf: Workflow, io: LiveIO, opts: ExecOptions = {}): Promise<Command[]> {
  return new Execution(inv, wf, io, opts.now ?? (() => new Date()), opts.maxStepsPerInvocation ?? 50, opts.deadline).run()
}
