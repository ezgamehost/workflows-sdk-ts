# @ezgh/workflows

The TypeScript SDK for [EZGH Workflows](https://ezghcloud.com): durable workflows as code. You
write a workflow as an async function whose side effects run in named steps. EZGH records each
step's result, so a workflow survives crashes, deploys and long sleeps, and resumes exactly where
it left off. Your code runs on your own HTTPS endpoint (Node, Bun, Deno, Cloudflare Workers,
Next.js), and EZGH pushes signed invocations to it.

```sh
npm i @ezgh/workflows
```

**Status:** pre-release, not published to npm yet (publishing is an open question in the design).

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

## The Client

`new Client({ apiKey, organizationId, projectId, baseUrl?, retries?, maxRetryDelayMs? })` calls
the Workflows API at `baseUrl`, by default `https://workflows-api.ezghcloud.com`. It retries 429
and 503 answers after `Retry-After` (default 3 retries, at most 10 s apart) with the same
`Idempotency-Key`, and throws `WorkflowsApiError` with the HTTP `status` and the platform `code`.

| Methods | Credential |
| --- | --- |
| `start`, `get`, `cancel`, `retry`, `output`, `input`, `stepOutput`, `events` | an EZGH API key with the `workflows.runs.*` actions it uses, or an EZGH SSO token with the `workflows:runs` scope |
| `createApp`, `getApp`, `sync`, `rotateSecret` | an EZGH API key with the `workflows.apps.*` actions it uses |

`createApp` and `rotateSecret` return the signing secret once, so they aren't resent after a
network error (a replay would answer `signingSecret: null`): they throw `WorkflowsApiError` with
status 0 and code `outcome_unknown`. Check the app with `getApp`, and rotate its secret if you
didn't receive one.

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
  `NonRetriableError` to fail at once, or `RetryAfterError(message, delayMs)` to pick the delay (0 to 24 hours).
  On replay a recorded failure is rethrown as a `WorkflowError` (or the SDK class it was), with
  its `code`, `name`, `data` and `cause`. Error codes on the wire are `user` or `workflows.*`: a
  `WorkflowError` with another code is sent as `user`, its code kept as the name and in
  `data.code`. Messages and stacks are cut to 8 KiB, and `data` over 64 KiB is dropped.
- **Timeouts:** with `{ timeoutMs }` (at most 900,000: a step can't outlive an invocation) a step
  that runs too long fails with
  `workflows.step_timeout` (retryable). The step function gets `{ signal, attempt,
  idempotencyKey }`; `signal` aborts at the timeout, so pass it to `fetch`.
- **Invocation time:** an invocation stops starting steps when less than max(5 s, 10 %) of its
  time is left, and EZGH invokes the workflow again. The deadline is measured on your machine's
  clock from when the request arrived, so clock skew doesn't matter. Calls to EZGH that are
  rate-limited or briefly unavailable are retried within that time.
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

`conformance/app.ts` is the conformance app every EZGH Workflows SDK implements
(workflows-stack `protocol/conformance/workflows.md`). `scripts/conformance.sh <workflows-stack
checkout>` builds workflows-stack's harness and runs its scenarios against it; CI does the same
at `.workflows-stack-rev` (it reads the internal workflows-stack repo with the
`WORKFLOWS_STACK_DEPLOY_KEY` secret, a read-only deploy key on workflows-stack, and skips with a notice without it).

`scripts/e2e.sh <workflows-stack checkout>` runs the SDK end to end against the real
workflows-stack (`workflows-stack all` on Postgres and an S3 fixture, with a fake org-api):
completion, idempotent start, sleep and payload replay, step retries, a process killed mid-step,
cancel with cleanup, retry from failure, and exact JSON replay (key order and NUL characters). The CI job `e2e` runs it with the same secret rule.

Verified end to end against workflows-stack bd9eae3 (`scripts/e2e.sh`, run against the shared test
fixtures; CI job e2e, which runs with the `WORKFLOWS_STACK_DEPLOY_KEY` secret).

`test/vectors` is a verbatim copy of workflows-stack's `protocol/vectors` at the commit in
`.workflows-stack-rev`; refresh it with `scripts/sync-vectors.sh <workflows-stack checkout> [<commit>]`.

## License

Apache-2.0
