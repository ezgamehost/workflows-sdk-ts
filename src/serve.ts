import type { InvocationResult, SyncReply } from "./protocol/index.ts"
import { PROTOCOL_VERSION, SIGNATURE_HEADER, SignatureError, decodeSecret, verify } from "./protocol/index.ts"
import { detectDeployment } from "./env.ts"
import { execute } from "./runtime/execution.ts"
import { InvalidInvocation, parseInvocation } from "./runtime/invocation.ts"
import { CheckpointFailed, InvocationSuperseded, httpLive, type FetchLike } from "./runtime/live.ts"
import type { Workflow } from "./runtime/workflow.ts"
import { SDK_NAME, SDK_VERSION } from "./version.ts"

const MAX_BODY = 8 * 1024 * 1024
const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } })

export interface ServeOptions {
  workflows: Workflow<any, any>[]
  // The app's active signing secrets (whsec_…). Pass two during a rotation.
  signingSecrets: string[]
  // How long your platform lets a request run (1–900, default 300); EZGH sets deadlines inside it.
  maxInvocationSeconds?: number
  // A deployment ID recorded with each invocation; detected on Vercel, Lambda and Cloud Run.
  deployment?: string
  // The fetch used to call workflows-live (tests and custom transports).
  fetch?: FetchLike
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
  const byName = new Map<string, Workflow<any, any>>()
  for (const w of opts.workflows) {
    if (byName.has(w.name)) throw new TypeError(`Two workflows are named "${w.name}"`)
    byName.set(w.name, w)
  }
  if (!Array.isArray(opts.signingSecrets) || opts.signingSecrets.length === 0) throw new TypeError("serve() needs at least one signing secret")
  const secrets = opts.signingSecrets.map(decodeSecret)
  const maxSeconds = opts.maxInvocationSeconds ?? 300
  if (!Number.isInteger(maxSeconds) || maxSeconds < 1 || maxSeconds > 900) throw new TypeError("maxInvocationSeconds is a whole number from 1 to 900")
  const deployment = opts.deployment ?? detectDeployment()
  const sdk = { name: SDK_NAME, version: SDK_VERSION }
  const fetchImpl: FetchLike = opts.fetch ?? ((u, i) => fetch(u, i))

  return async (req) => {
    if (req.method !== "POST") return json(405, { code: "method_not_allowed" })
    let body: Uint8Array
    try { body = await readBody(req) } catch (e) {
      if (e instanceof TooLarge) return json(413, { code: "too_large" })
      return json(400, { code: "invalid_invocation" })
    }
    try {
      await verify(req.headers.get(SIGNATURE_HEADER) ?? "", secrets, body, new Date())
    } catch (e) {
      if (e instanceof SignatureError) return json(401, { code: e.reason === "expired" ? "expired_signature" : "invalid_signature" })
      return json(500, { code: "sdk_error" })
    }
    let inv
    try { inv = parseInvocation(new TextDecoder().decode(body)) } catch (e) {
      if (e instanceof InvalidInvocation) return json(400, { code: "invalid_invocation", message: e.message })
      return json(500, { code: "sdk_error" })
    }
    if (inv.kind === "sync") {
      const reply: SyncReply = {
        protocolVersions: [PROTOCOL_VERSION], sdk, maxInvocationSeconds: maxSeconds,
        workflows: [...byName.values()].map((w) => ({ name: w.name, ...(w.opts.retry ? { retry: w.opts.retry } : {}), ...(w.opts.timeoutMs ? { timeoutMs: w.opts.timeoutMs } : {}) })),
        ...(deployment ? { deployment } : {}),
      }
      return json(200, reply)
    }
    const result = (commands: InvocationResult["commands"]): InvocationResult => ({ commands, ...(deployment ? { deployment } : {}), sdk })
    const wf = byName.get(inv.run!.workflow)
    if (!wf) return json(200, result([{ type: "workflowNotFound", workflow: inv.run!.workflow }]))
    try {
      return json(200, result(await execute(inv, wf, httpLive(inv.endpoints.live, inv.token, fetchImpl))))
    } catch (e) {
      if (e instanceof InvocationSuperseded) return json(409, { code: "invocation_superseded" })
      if (e instanceof CheckpointFailed) return json(503, { code: "checkpoint_failed" })
      return json(500, { code: "sdk_error" })
    }
  }
}
