// A pull worker: run with EZGH_API_KEY, EZGH_ORG and EZGH_PROJECT set, plus EZGH_PULL_APP in
// production. In development (NODE_ENV isn't production) it runs on your own dev app instead.
import { Client, worker, workflow } from "@ezghcloud/workflows"

const resize = workflow("resize", async ({ input, step }) => {
  const { url } = input as { url: string }
  const bytes = await step.run("download", async ({ signal }) => Number((await fetch(url, { signal })).headers.get("content-length")))
  return step.run("report", () => ({ url, bytes }))
})

const creds = { apiKey: process.env.EZGH_API_KEY!, organizationId: process.env.EZGH_ORG!, projectId: process.env.EZGH_PROJECT! }
const dev = process.env.NODE_ENV !== "production"
// Dev mode: each developer's own pull app (dev-<user>), polled on its dev-<user> queue.
const app = dev ? (await new Client(creds).devApp()).id : process.env.EZGH_PULL_APP!
const w = worker({ ...creds, app, workflows: [resize], dev })
process.on("SIGTERM", () => void w.stop())
await w.run()
