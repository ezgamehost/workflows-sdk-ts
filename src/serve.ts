import type { InvocationResult, SyncReply } from "./protocol/index.ts"
import { PROTOCOL_VERSIONS, SIGNATURE_HEADER, SignatureError, decodeSecret, parseSignature, verifyParsed } from "./protocol/index.ts"
import { detectDeployment } from "./env.ts"
import { execute } from "./runtime/execution.ts"
import { InvalidInvocation, parseInvocation } from "./runtime/invocation.ts"
import { CheckpointFailed, InvocationSuperseded, httpLive, type FetchLike } from "./runtime/live.ts"
import { definitionOf, type Workflow, type WorkflowDefinition } from "./runtime/workflow.ts"
import { SDK_NAME, SDK_VERSION } from "./version.ts"

const MAX_BODY = 8 * 1024 * 1024
const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } })

export interface ServeOptions {
  workflows: Workflow<any, any>[]
  // The app's active signing secrets (whsec_…); pass two during a rotation. They're decoded on the
  // first request, so a build without the environment variable still works (undefined entries are
  // skipped then; with none left, requests answer 500).
  signingSecrets: (string | undefined)[]
  // How long your platform lets a request run (1–900, default 300); EZGH sets deadlines inside it.
  maxInvocationSeconds?: number
  // A deployment ID recorded with each invocation; detected on Vercel, Lambda and Cloud Run.
  deployment?: string
  // The fetch used to call workflows-live (tests and custom transports).
  fetch?: FetchLike
  // Steps run per invocation before it ends and EZGH invokes the workflow again (default 50).
  maxStepsPerInvocation?: number
  // The public API base for callback URLs (default https://workflows-api.ezghcloud.com); used only
  // when an invocation doesn't carry endpoints.api.
  apiBaseUrl?: string
}

class TooLarge extends Error {}

async function readBody(req: Request): Promise<Uint8Array> {
  const declared = Number(req.headers.get("Content-Length") ?? "0")
  if (declared > MAX_BODY) throw new TooLarge()
  if (!req.body) return new Uint8Array(0)
  const reader = req.body.getReader()
  const chunks: Uint8Array[] = []
  let size = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    size += value.length
    if (size > MAX_BODY) { await reader.cancel().catch(() => {}); throw new TooLarge() }
    chunks.push(value)
  }
  const out = new Uint8Array(size)
  let at = 0
  for (const c of chunks) { out.set(c, at); at += c.length }
  return out
}

// serve returns a Fetch-API handler for push invocations. Bun, Deno, Cloudflare Workers and
// Next.js route handlers take it as is; Node needs a small adapter (examples/node). Answers
// (plan D25): 200 SyncReply or InvocationResult, 401 bad signature, 400 bad invocation, 409
// superseded, 503 checkpoint failed, 500 SDK error, 405 non-POST, 413 over 8 MiB.
export function serve(opts: ServeOptions): (req: Request) => Promise<Response> {
  const byName = new Map<string, { wf: Workflow<any, any>; def: WorkflowDefinition }>()
  for (const wf of opts.workflows) {
    const def = definitionOf(wf)
    if (byName.has(def.name)) throw new TypeError(`Two workflows are named "${def.name}"`)
    byName.set(def.name, { wf, def })
  }
  if (!Array.isArray(opts.signingSecrets) || opts.signingSecrets.length === 0) throw new TypeError("serve() needs at least one signing secret")
  // A malformed secret fails now; an unset or empty one (an env var at build time) is skipped.
  for (const s of opts.signingSecrets) if (s !== undefined && s !== "") decodeSecret(s)
  let secrets: Uint8Array[] | undefined
  const keys = () => (secrets ??= opts.signingSecrets.filter((s): s is string => typeof s === "string" && s !== "").map(decodeSecret))
  if (opts.maxStepsPerInvocation !== undefined && (!Number.isInteger(opts.maxStepsPerInvocation) || opts.maxStepsPerInvocation < 1))
    throw new TypeError("maxStepsPerInvocation is a whole number ≥ 1")
  const maxSeconds = opts.maxInvocationSeconds ?? 300
  if (!Number.isInteger(maxSeconds) || maxSeconds < 1 || maxSeconds > 900) throw new TypeError("maxInvocationSeconds is a whole number from 1 to 900")
  for (const { def } of byName.values())
    if (def.opts.timeoutMs !== undefined && def.opts.timeoutMs > maxSeconds * 1000)
      throw new TypeError(`Workflow "${def.name}" has timeoutMs ${def.opts.timeoutMs}, longer than an invocation (maxInvocationSeconds ${maxSeconds}): a step can't outlive its invocation; long steps need a pull worker`)
  const deployment = opts.deployment ?? detectDeployment()
  const sdk = { name: SDK_NAME, version: SDK_VERSION }
  const fetchImpl: FetchLike = opts.fetch ?? ((u, i) => fetch(u, i))

  return async (req) => {
    const receivedAt = Date.now()
    if (req.method !== "POST") return json(405, { code: "method_not_allowed" })
    const active = keys()
    if (active.length === 0) return json(500, { code: "sdk_error", message: "No signing secret is configured" })
    // The header and its timestamp first: a stale or malformed signature costs no body read.
    let sig
    try { sig = parseSignature(req.headers.get(SIGNATURE_HEADER) ?? "", new Date(receivedAt)) } catch (e) {
      if (e instanceof SignatureError) return json(401, { code: e.reason === "expired" ? "expired_signature" : "invalid_signature" })
      return json(500, { code: "sdk_error" })
    }
    let body: Uint8Array
    try { body = await readBody(req) } catch (e) {
      if (e instanceof TooLarge) return json(413, { code: "payload_too_large" })
      return json(400, { code: "invalid_invocation" })
    }
    try {
      await verifyParsed(sig, active, body)
    } catch (e) {
      if (e instanceof SignatureError) return json(401, { code: "invalid_signature" })
      return json(500, { code: "sdk_error" })
    }
    let inv
    try { inv = parseInvocation(new TextDecoder().decode(body)) } catch (e) {
      if (e instanceof InvalidInvocation) return json(400, { code: "invalid_invocation", message: e.message })
      return json(500, { code: "sdk_error" })
    }
    if (inv.kind === "sync") {
      const reply: SyncReply = {
        protocolVersions: [...PROTOCOL_VERSIONS], sdk, maxInvocationSeconds: maxSeconds,
        workflows: [...byName.values()].map(({ def }) => ({ name: def.name, ...(def.opts.retry ? { retry: def.opts.retry } : {}), ...(def.opts.timeoutMs ? { timeoutMs: def.opts.timeoutMs } : {}) })),
        ...(deployment ? { deployment } : {}),
      }
      return json(200, reply)
    }
    const result = (commands: InvocationResult["commands"]): InvocationResult => ({ commands, ...(deployment ? { deployment } : {}), sdk })
    const wf = byName.get(inv.run!.workflow)?.wf
    if (!wf) return json(200, result([{ type: "workflowNotFound", workflow: inv.run!.workflow }]))
    // The deadline on this machine's clock: the engine's budget (deadline − t) from receipt, and
    // never later than the deadline as this clock reads it.
    const deadline = Math.min(receivedAt + (Date.parse(inv.deadline) - sig.t * 1000), Date.parse(inv.deadline))
    const total = Math.max(0, deadline - receivedAt)
    const reserveMs = Math.max(5000, total * 0.1)
    try {
      const io = httpLive(inv.endpoints.live, inv.token, fetchImpl, { deadline, reserveMs })
      return json(200, result(await execute(inv, wf, io, { deadline, ...(opts.apiBaseUrl ? { apiBaseUrl: opts.apiBaseUrl } : {}), ...(opts.maxStepsPerInvocation ? { maxStepsPerInvocation: opts.maxStepsPerInvocation } : {}) })))
    } catch (e) {
      if (e instanceof InvocationSuperseded) return json(409, { code: "invocation_superseded" })
      // The code goes top level (as before) and in the kit's error shape, which dispatch reads:
      // payload_unavailable costs the run no step attempt.
      if (e instanceof CheckpointFailed) return json(503, { code: e.code, error: { code: e.code, message: e.message } })
      return json(500, { code: "sdk_error" })
    }
  }
}
