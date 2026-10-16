// Creates the e2e push app and pull app through the Client (which retries the cold engine's 503s with the same
// Idempotency-Key) and prints "<appId> <signingSecret> <pullAppId>".
import { Client } from "@ezghcloud/workflows"

const client = new Client({
  apiKey: process.env.E2E_API_KEY!, organizationId: process.env.E2E_ORG!, projectId: process.env.E2E_PROJECT!,
  baseUrl: process.env.E2E_API ?? "http://127.0.0.1:6100", retries: 90, maxRetryDelayMs: 2000,
})
const { app, signingSecret } = await client.createApp(`e2e-${Date.now()}`, { endpoint: process.env.E2E_APP ?? "http://127.0.0.1:3200/" })
if (!signingSecret) throw new Error("createApp returned no signing secret")
const pull = await client.createApp(`e2e-pull-${Date.now()}`, { kind: "pull", queue: "e2e", partitions: 2 })
console.log(`${app.id} ${signingSecret} ${pull.app.id}`)

