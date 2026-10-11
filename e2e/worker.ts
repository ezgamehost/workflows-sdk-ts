// The e2e pull worker: runs e2e-pulled for the pull app e2e/setup.ts created.
import { worker, workflow } from "@ezgh/workflows"

const pulled = workflow("e2e-pulled", async ({ input, step }) => step.run("square", () => (input as { n: number }).n ** 2))
const w = worker({
  apiKey: process.env.E2E_API_KEY!, organizationId: process.env.E2E_ORG!, projectId: process.env.E2E_PROJECT!,
  app: process.env.E2E_PULL_APP_ID!, queue: "e2e", workflows: [pulled], liveUrl: process.env.E2E_LIVE ?? "http://127.0.0.1:6150",
  maxTasks: 4, heartbeatIntervalMs: 2000, backoffMaxMs: 1000,
  onError: (e, context) => console.error(`worker: ${context}:`, e),
})
process.on("SIGTERM", () => void w.stop({ graceMs: 2000 }))
await w.run()
