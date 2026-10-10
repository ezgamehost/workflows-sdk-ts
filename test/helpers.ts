import type { Checkpoint, Command, Invocation, Journal, JournalEntry } from "../src/protocol/index.ts"
import { stepId } from "../src/protocol/index.ts"
import type { LiveIO } from "../src/runtime/live.ts"

export const sid = (name: string, n = 0) => stepId(name, n)

export function invocation(workflow: string, entries: JournalEntry[] = [], extra: Partial<Invocation> = {}): Invocation {
  return {
    protocolVersion: "1.0", kind: "orchestrate", invocationId: "inv_aaaaaaaaaaaaaaaaaaaaaaaaaa", token: "t",
    endpoints: { live: "https://live.test" },
    run: { id: "0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a", workflow, input: { n: 2 }, attempt: 1, startedAt: "2026-10-09T12:00:00Z" },
    journal: { complete: true, entries },
    deadline: new Date(Date.now() + 300_000).toISOString(), cancel: null, ...extra,
  }
}

export async function entry(name: string, n: number, seq: number, rest: Partial<JournalEntry>): Promise<JournalEntry> {
  return { seq, stepId: await sid(name, n), name, kind: "run", status: "completed", attempt: 1, ...rest }
}

export class FakeLive implements LiveIO {
  checkpoints: Checkpoint[] = []
  pages: Journal[] = []
  payloads = new Map<string, unknown>()
  ack: () => Promise<void> = async () => {}
  async checkpoint(cp: Checkpoint) { this.checkpoints.push(structuredClone(cp)); await this.ack() }
  async journalPage(_afterSeq: number) { const p = this.pages.shift(); if (!p) throw new Error("no page"); return p }
  async payload(id: string) { if (!this.payloads.has(id)) throw new Error("no payload " + id); return this.payloads.get(id) }
  commandsOf(i: number): Command[] { return this.checkpoints[i]?.commands ?? [] }
}

// codec.json names JS values by tag; these are the values.
export const codecValues: Record<string, unknown> = {
  "date-epoch": new Date(0),
  undefined: undefined,
  "object-with-undefined-field": { gone: undefined },
  "array-with-undefined": [1, undefined],
  "nested-date": { at: new Date("2026-10-09T12:00:00.000Z") },
  bigint: 1n,
  function: () => 1,
}
