// Run after each deploy: EZGH calls the app's endpoint and records its workflows.
import { Client } from "@ezgh/workflows"

const client = new Client({
  apiKey: process.env.EZGH_API_KEY!,
  organizationId: process.env.EZGH_ORGANIZATION_ID!,
  projectId: process.env.EZGH_PROJECT_ID!,
})
const app = await client.sync(process.env.EZGH_WORKFLOWS_APP_ID!, { endpoint: process.env.APP_URL + "/api/workflows" })
console.log(`synced ${app.id}: ${app.workflows.map((w) => w.name).join(", ")}`)
