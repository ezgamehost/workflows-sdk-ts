import type { Invocation } from "../protocol/index.ts"

export class InvalidInvocation extends Error { override name = "InvalidInvocation" }

const KINDS = new Set(["orchestrate", "step", "sync"])
const isObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v)

// parseInvocation checks what the runtime relies on and ignores unknown fields (protocol minor
// versions add fields).
export function parseInvocation(text: string): Invocation {
  let v: unknown
  try { v = JSON.parse(text) } catch { throw new InvalidInvocation("The body isn't JSON") }
  if (!isObject(v)) throw new InvalidInvocation("The body isn't an invocation")
  const inv = v as Partial<Invocation> & Record<string, unknown>
  if (typeof inv.protocolVersion !== "string" || inv.protocolVersion.split(".")[0] !== "1")
    throw new InvalidInvocation(`Unsupported protocol version ${String(inv.protocolVersion)}`)
  if (typeof inv.kind !== "string" || !KINDS.has(inv.kind)) throw new InvalidInvocation(`Unknown invocation kind ${String(inv.kind)}`)
  if (typeof inv.invocationId !== "string" || typeof inv.deadline !== "string" || Number.isNaN(Date.parse(inv.deadline)))
    throw new InvalidInvocation("Missing invocationId or deadline")
  if (inv.kind !== "sync") {
    const run = inv.run as unknown
    const journal = inv.journal as unknown
    if (!isObject(run) || typeof run.id !== "string" || typeof run.workflow !== "string" || typeof run.attempt !== "number")
      throw new InvalidInvocation("Missing run")
    if (!isObject(journal) || typeof journal.complete !== "boolean" || !Array.isArray(journal.entries))
      throw new InvalidInvocation("Missing journal")
    for (const e of journal.entries as unknown[])
      if (!isObject(e) || typeof e.seq !== "number" || typeof e.stepId !== "string" || typeof e.status !== "string")
        throw new InvalidInvocation("A journal entry needs seq, stepId and status")
    if (typeof inv.token !== "string" || inv.token === "" || !isObject(inv.endpoints) || typeof inv.endpoints.live !== "string")
      throw new InvalidInvocation("Missing token or endpoints.live")
    if (inv.kind === "step" && (typeof inv.targetStepId !== "string" || inv.targetStepId === ""))
      throw new InvalidInvocation("A step invocation needs targetStepId")
  }
  if (inv.cancel !== undefined && inv.cancel !== null && !isObject(inv.cancel)) throw new InvalidInvocation("cancel isn't an object")
  return { ...inv, cancel: inv.cancel ?? null } as Invocation
}
