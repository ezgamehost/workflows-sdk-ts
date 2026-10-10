// A pull worker: run with EZGH_API_KEY, EZGH_ORG, EZGH_PROJECT and EZGH_PULL_APP set.
import { worker, workflow } from "@ezgh/workflows"

const resize = workflow("resize", async ({ input, step }) => {
  const { url } = input as { url: string }
  const bytes = await step.run("download", async ({ signal }) => Number((await fetch(url, { signal })).headers.get("content-length")))
  return step.run("report", () => ({ url, bytes }))
})

const w = worker({
  apiKey: process.env.EZGH_API_KEY!, organizationId: process.env.EZGH_ORG!, projectId: process.env.EZGH_PROJECT!,
  app: process.env.EZGH_PULL_APP!, workflows: [resize], dev: process.env.NODE_ENV !== "production",
})
process.on("SIGTERM", () => void w.stop())
await w.run()
