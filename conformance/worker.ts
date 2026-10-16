// The conformance pull worker: polls the harness's workflows-live stand-in
// (workflows-conformance --pull-addr), as workflows.md's notes say.
import { worker } from "@ezghcloud/workflows"
import { all } from "./workflows.ts"

const w = worker({
  apiKey: "conformance-key", organizationId: "0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70", projectId: "0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81",
  app: "0199a3c2-7d30-7f62-9b5c-4eaf8d3f6b92", queue: "conformance", workflows: all, liveUrl: process.env.CONFORMANCE_LIVE!,
  maxTasks: 4, concurrency: 4, heartbeatIntervalMs: 300, backoffMaxMs: 500, deployment: "conformance",
})
process.on("SIGTERM", () => void w.stop({ graceMs: 1000 }))
await w.run()
