# @ezgh/workflows

The TypeScript SDK for [EZGH Workflows](https://ezghcloud.com): durable workflows as code. You
write a workflow as an async function whose side effects run in named steps. EZGH records each
step's result, so a workflow survives crashes, deploys and long sleeps, and resumes exactly where
it left off. Your code runs on your own HTTPS endpoint (Node, Bun, Deno, Cloudflare Workers,
Next.js), and EZGH pushes signed invocations to it.

```sh
npm i github:ezgamehost/workflows-sdk-ts#v0.2.0
```

**Status:** pre-release, not published to npm yet (publishing is an open question in the design).
Install it from GitHub by tag or commit, as above: npm builds the package as it installs it (the
`prepare` script), so the install takes a few seconds longer. It imports as `@ezgh/workflows`.

## Quick start

**1. Define workflows.** Side effects go in named steps; their JSON results are recorded.

```ts
import { workflow } from "@ezgh/workflows"

export const onboarding = workflow("onboarding", async ({ input, step }) => {
  const user = await step.run("load-user", () => db.users.get(input.userId))
  await step.run("welcome-email", () => email.send(user.email, "Welcome!"))
  await step.sleep("cooldown", "3d")
  return step.run("follow-up", () => email.send(user.email, "How is it going?"))
})
```

**2. Serve them** from one HTTPS route, with the app's signing secret (`whsec_…`, returned once
when you create the app; pass two during a rotation):

```ts
import { serve } from "@ezgh/workflows"

export const POST = serve({ workflows: [onboarding], signingSecrets: [process.env.EZGH_WORKFLOWS_SIGNING_SECRET] })
```

`serve` returns a Fetch-API handler `(Request) => Promise<Response>`. Examples:
[Bun](examples/bun/server.ts), [Node](examples/node/server.ts),
[Cloudflare Workers](examples/cloudflare/worker.ts) and
[Next.js](examples/nextjs/app/api/workflows/route.ts). Set `maxInvocationSeconds` to your
platform's request limit (default 300, at most 900).

**3. Sync after each deploy,** so EZGH learns the endpoint and its workflows
([example](examples/deploy-sync.ts)):

```ts
import { Client } from "@ezgh/workflows"

const client = new Client({ apiKey, organizationId, projectId })
await client.sync(appId, { endpoint: "https://app.example.com/api/workflows" })
```

**4. Start runs** and follow them:

```ts
const run = await client.start(appId, "onboarding", { userId: "u_1" }, { runKey: "signup-u_1" })
const latest = await client.get(run.id)   // status, attempt, steps
const output = await client.output(run.id) // once completed
await client.cancel(run.id)                // or client.retry(run.id) after a failure
```

## Parallel steps

Steps started together run in parallel: `Promise.all` and `Promise.race` over `step.run` calls
(and sleeps, callbacks and child workflows) hand every step to EZGH at once, and each runs in its
own invocation, retried on its own.

```ts
const sizes = await Promise.all(urls.map((url, i) => step.run(`size-${i}`, () => fetchSize(url))))
const first = await Promise.race([step.run("work", doWork), step.sleep("limit", "10m").then(() => "timed out")])
```

A run has at most 32 step invocations at once; further steps wait their turn. Steps pending
together need distinct names (or `{ id }`s). Replays resolve steps in the order they completed,
so a race takes the same branch every time. A step's `signal` also aborts when the run is
cancelling (pull workers learn it from their heartbeats).

## Callbacks

`step.createCallback(name, { timeoutMs })` creates a callback (default timeout 7 days, from 1 s to
a year). Its `url` is `https://workflows-api.ezghcloud.com/public/v1/callbacks/<token>`: give it
to whoever completes the callback (a webhook, an approval email). `wait()` suspends the run until
someone POSTs a JSON body (at most 1 MiB) to the URL, and resolves with that body:

```ts
const cb = await step.createCallback<{ approved: boolean }>("approval", { timeoutMs: 3 * 86_400_000 })
await step.run("ask", () => email.send(manager, `Approve? POST to ${cb.url}`))
try {
  const { approved } = await cb.wait()
} catch (e) {
  if (e instanceof CallbackTimeout) { /* nobody answered in time */ }
}
```

The first completion wins; repeating it with the same body succeeds, a different body is a `409
callback_completed`, and a completion after the timeout is a `410 callback_expired`. An
authenticated caller can complete it with `POST …/runs/:runId/callbacks/:stepId` instead
(`workflows.callbacks.complete`). The token is the URL's only credential: keep it private. The
URL is recorded before your code sees it, so replays rebuild the same one.

## Child workflows

`step.invoke(name, { workflow, input })` starts a run of another workflow in the same app and
waits for it, resolving with its output. A failed child rejects with `ChildFailed` (code
`workflows.child_failed`), whose `cause` is the child's own error. The child's run ID is derived
from the parent's and the step's, so it starts exactly once. Children nest at most 10 deep, and
each counts as one of its parent's steps.

```ts
const report = await step.invoke("report", { workflow: buildReport, input: { month: "2026-10" } })
```

A run a schedule started sees `run.schedule` (`{ id, scheduledTime }`, the cron instant).

Parallel steps, callbacks and child workflows need a workflows-stack serving protocol 1.1. An
invocation at protocol 1.0 runs steps one after another, and `createCallback` and `invoke` fail
the run with `NonRetriableError`.

## Tracing

While the project's Workflows observability setting is on, EZGH sends each invocation a W3C
`traceparent` (the push request's header, or a pulled invocation's field), and the context exposes
it exactly as received as `traceparent`. It's `""` while the setting is off, so a parent-based
sampler never waits for a parent that won't arrive. The SDK has no OpenTelemetry dependency; with
`@opentelemetry/api` (and the W3C propagator, which the Node SDK registers by default), parent your
own spans on it. Start them inside step functions: code outside steps re-runs on every replay.

```ts
import { context, propagation, trace } from "@opentelemetry/api"

const tracer = trace.getTracer("billing")

const charge = workflow("charge", async ({ input, step, traceparent }) =>
  step.run("charge-card", () => {
    const parent = propagation.extract(context.active(), { traceparent })
    return tracer.startActiveSpan("charge card", {}, parent, async (span) => {
      try { return await chargeCard(input) } finally { span.end() }
    })
  }))
```

## Pull workers

A pull worker polls EZGH for work instead of exposing an HTTPS endpoint: for code behind a
firewall, or long CPU-bound steps. A pull invocation's deadline is the longest `timeoutMs` the
app's workers announce, at least 15 minutes and at most 24 hours; heartbeats keep it honest. So
for a step longer than 15 minutes, give its workflow a `timeoutMs` (the workflow option, which
workers announce) that covers it. Create a
pull app (`kind: "pull"`, with a task queue and its partitions), then run a worker anywhere with
`fetch` (Node, Bun, Deno):

```ts
import { worker } from "@ezgh/workflows"

const w = worker({ apiKey, organizationId, projectId, app: pullAppId, workflows: [resize], queue: "default" })
process.on("SIGTERM", () => void w.stop())
await w.run()
```

- **Polling:** one long poll at a time (held up to 55 s) for up to `maxTasks` tasks (1–10, default
  4), never more than the free slots of `concurrency` (default 10 invocations at once). Each poll
  announces the worker's workflows, so they show on the app. 429, 5xx and network errors are
  retried with jittered backoff after any `Retry-After` (at most `backoffMaxMs`, default 30 s).
  A refused API key or permission (`WorkerAuthError`) or an unknown app or queue
  (`WorkerConfigError`) stops polling, lets running invocations finish (up to 30 s), then rejects
  `run()`.
- **Heartbeats:** a running invocation heartbeats every `heartbeatIntervalMs` (default 20 s, at
  most 30 s: EZGH re-dispatches after 60 s without one). A failed heartbeat (a refused token, the
  network) goes to `onError`. When the run is cancelling, each running step's `signal`
  aborts with a `CancelledError` (pass it on), no new step starts, and the invocation ends: the
  next one carries the cancel, so your cleanup (`finally`) runs there.
- **`stop({ graceMs })`** stops polling at once (a poll still waiting is aborted; one whose answer
  is arriving is run, since its tasks are started), waits up to `graceMs` (default 30 s) for
  running invocations, then cancels their steps (`signal` aborts) and waits at most 5 s more,
  then resolves `run()`; EZGH re-dispatches whatever was abandoned.
- **Limits:** a worker serves at most 1,000 workflows (checked by `worker()`), and an invocation's
  result is at most 8 MiB of JSON: one bigger (say, many large child inputs at once) fails the run
  with `NonRetriableError` instead of failing to send.

The API key needs `workflows.workers.poll` (and `workflows.apps.sync` to announce workflows).
[Example](examples/worker/worker.ts).

## Dev mode

Each developer runs workflows on their own machine, with no tunnel, through a pull app of their
own: the live host serves only an app's own queue, so dev mode can't borrow the team's app.
`client.devApp()` creates (or finds) the pull app `dev-<user>` with the queue `dev-<user>` (from
`$USER`, `$USERNAME` or `$LOGNAME`, or `devApp({ user })`), and `worker({ dev: true })` polls that
queue:

```ts
const app = await client.devApp()
await worker({ apiKey, organizationId, projectId, app: app.id, dev: true, workflows: [resize] }).run()
await client.start(app.id, "resize", { url })   // runs on this machine
```

Developers sharing a project never take each other's runs. `devApp` needs `workflows.apps.create`
(and `workflows.apps.list` to find an existing one).

## The Client

`new Client({ apiKey, organizationId, projectId, baseUrl?, retries?, maxRetryDelayMs?, timeoutMs? })`
calls the Workflows API at `baseUrl`, by default `https://workflows-api.ezghcloud.com`. Each
request times out after `timeoutMs` (default 30 s). It retries 429, 503, a 409
`idempotency_in_progress` and network errors after `Retry-After` (default 3 retries, at most 10 s
apart) with the same `Idempotency-Key` (an `idempotencyKey` you pass is a non-empty string), and
throws `WorkflowsApiError` with the HTTP `status` and the platform `code`. Redirects are never
followed: a 3xx is a `WorkflowsApiError` with code `redirect`.

| Methods | Credential |
| --- | --- |
| `start`, `get`, `cancel`, `retry`, `output`, `input`, `stepOutput`, `events` | an EZGH API key with the `workflows.runs.*` actions it uses, or an EZGH SSO token with the `workflows:runs` scope |
| `completeCallback` | `workflows.callbacks.complete`, which the `workflows:runs` scope includes |
| `createApp`, `getApp`, `sync`, `rotateSecret` | an EZGH API key with the `workflows.apps.*` actions it uses |
| `createSchedule`, `getSchedule`, `listSchedules`, `updateSchedule`, `pauseSchedule`, `resumeSchedule`, `deleteSchedule`, `triggerSchedule` | an EZGH API key with the `workflows.schedules.*` actions it uses (create, update of the workflow, resume and trigger also need `workflows.runs.start` on the workflow) |

`createApp` and `rotateSecret` return the signing secret once, and a resend's replay would answer
`signingSecret: null`. So any answer that leaves the outcome unknown (a network error or timeout,
a 503, a 409 `idempotency_in_progress`, or a push app's success without a secret) throws
`WorkflowsApiError` with status 0 and code `outcome_unknown`; only a 429 (not processed) is
retried. Check the app with `getApp`, and rotate its secret if you didn't receive one.

**Pull apps:** `createApp(name, { kind: "pull", queue, partitions })` creates an app for
[pull workers](#pull-workers); its partitions (1–32, default 4) may grow later but never shrink.

**Schedules** start runs of one workflow on a cron expression (5 fields or `@hourly`, `@daily`,
`@weekly`, `@monthly`, `@yearly`; at most once a minute) in an IANA time zone (default `UTC`):

```ts
const s = await client.createSchedule({ appId, workflow: "nightly-report", name: "nightly", cron: "0 2 * * *", timeZone: "Europe/Berlin" })
await client.updateSchedule(s.id, { cron: "30 2 * * *" }, { revision: s.revision }) // 412 if it changed since
await client.triggerSchedule(s.id, { idempotencyKey: "rerun-2026-10-10" })          // a run now; the key names it
await client.pauseSchedule(s.id); await client.resumeSchedule(s.id); await client.deleteSchedule(s.id)
```

Options: `overlap` (`skip`, the default, `allow` or `bufferOne`), `jitterSeconds` (0–3,600),
`catchupWindowSeconds` (default 3,600, at most 86,400) and `input` (JSON, at most 1 MiB). A run a
schedule started sees `run.schedule` (`{ id, scheduledTime }`).

**Callbacks** can also be completed by an authenticated caller: `completeCallback(runId, stepId,
value)` answers the run. Without a client, `completeCallbackURL(url, value)` POSTs the value to a
callback's URL (no credential: the URL's token is one).

## Semantics

- **Steps run at least once.** A step can run again if the process dies after it ran but before
  its result was recorded. Inside `step.run`, `step.idempotencyKey()` is
  `<runId>:<stepId>:<attempt>`; pass it to APIs that deduplicate.
- **Values are JSON.** A step's result is what JSON makes of it, live and on replay alike: a
  `Date` becomes an ISO string, `undefined` becomes `null`, and functions, symbols and bigints
  fail the step without retries. A result over 1 MiB fails the step too.
- **Code between steps re-runs on every replay**, so keep it deterministic: read the time and
  random values through `step.now(name)` and `step.uuid(name)`.
- **Step names identify steps.** A name is 1–256 bytes of UTF-8 without NUL, and the *n*th call
  with a name is that name's *n*th step. Two steps pending at the same time (in `Promise.all` or
  `Promise.race`) need distinct names or explicit `{ id }`s. An explicit id names one step for the
  whole run: using it twice fails the run with `workflows.duplicate_step_name`. Replays resolve recorded steps in the order they completed, so races take
  the same branch every time.
- **Retries:** a step that throws is retried with backoff (default 5 attempts, from 1 s doubling to
  at most 10 minutes; override per step with `{ retry }`, or per workflow; partial policies are
  merged with the defaults). Bounds: `maxAttempts` 1–100, `maxMs` up to 24 hours, `multiplier`
  1–10. Throw
  `NonRetriableError` to fail at once, or `RetryAfterError(message, delayMs)` to pick the delay (0 to 24 hours),
  directly or as any error's `cause` (an error whose `code` is `workflows.non_retriable` isn't
  retried either).
  On replay a recorded failure is rethrown as a `WorkflowError` (or the SDK class it was), with
  its `code`, `name`, `data` and `cause`. Error codes on the wire are `user` or `workflows.*`: a
  `WorkflowError` with another code is sent as `user`, its code kept as the name and in
  `data.code`. Messages and stacks are cut to 8 KiB, and `data` over 64 KiB is dropped.
- **Timeouts:** with `{ timeoutMs }` (at most 24 hours; a step can't outlive its invocation, so
  `serve()` refuses a workflow default longer than `maxInvocationSeconds`, and long steps need a
  pull worker) a step
  that runs too long fails with
  `workflows.step_timeout` (retryable). The step function gets `{ signal, attempt,
  idempotencyKey }`; `signal` aborts at the timeout, so pass it to `fetch`.
- **Invocation time:** an invocation stops starting steps when less than max(5 s, 10 %) of its
  time is left, and EZGH invokes the workflow again. The deadline is measured on your machine's
  clock from when the request arrived, so clock skew doesn't matter. Calls to EZGH that are
  rate-limited or briefly unavailable are retried within that time. When EZGH can't store a step's
  output (its payload store is down), the invocation is retried later without using a step
  attempt.
- **Cancellation:** cancelling a run throws `CancelledError` from the call the workflow is
  waiting on. `finally` and `catch` blocks can still run `step.run` cleanup steps; sleeps and
  new values are refused. The run then ends `cancelled`.

The package has no runtime dependencies: it uses Web Crypto and `fetch` only. It's ESM only; in
TypeScript use `moduleResolution` `bundler`, `node16` or `nodenext` (TypeScript 5.4 or later).
`@ezgh/workflows/protocol` exports `sign`, `verify`, `decodeSecret`, `SignatureError` and the wire
types, for checking `EZGH-Signature` headers in a custom transport.

## Development

```sh
bun install
bun run check        # typecheck, tests, build, a Node import smoke test, npm pack --dry-run
```

`conformance/workflows.ts` holds the conformance workflows every EZGH Workflows SDK implements
(workflows-stack `protocol/conformance/workflows.md`), served by `conformance/app.ts` (push) and
`conformance/worker.ts` (a pull worker). `scripts/conformance.sh <workflows-stack checkout>`
builds workflows-stack's harness and runs its push and pull scenarios against them; CI does the same
at `.workflows-stack-rev` (it reads the internal workflows-stack repo with the
`WORKFLOWS_STACK_DEPLOY_KEY` secret, a read-only deploy key on workflows-stack, and skips with a notice without it).

`scripts/e2e.sh <workflows-stack checkout>` runs the SDK end to end against the real
workflows-stack (`workflows-stack all` on Postgres and an S3 fixture, with a fake org-api):
completion, idempotent start, sleep and payload replay, step retries, a process killed mid-step,
cancel with cleanup, retry from failure, exact JSON replay (key order and NUL characters), and
phase 2: a callback completed over its public URL (repeat and conflicting completions), a parent
and its child under the derived child run ID, a five-step parallel fan-out, a pull worker
(`e2e/worker.ts`) announcing and running a pull app's workflow, and a schedule firing a run
under its derived scheduled run ID, then trigger (idempotent by key), pause and delete. The CI
job `e2e` runs it with the same secret rule. Against a slow shared database, set
`E2E_TIME_SCALE` (for example `3`) to stretch every wait; no check changes.

Verified end to end against workflows-stack ec53771: the CI job e2e ran the full suite, and
conformance passed all 57 scenarios, push and pull.

`test/vectors` is a verbatim copy of workflows-stack's `protocol/vectors` at the commit in
`.workflows-stack-rev`; refresh it with `scripts/sync-vectors.sh <workflows-stack checkout> [<commit>]`.

## License

Apache-2.0
