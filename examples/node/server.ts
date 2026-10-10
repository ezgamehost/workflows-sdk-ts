import { createServer } from "node:http"
import { serve } from "@ezgh/workflows"
import { onboarding } from "../workflows.ts"

const handler = serve({ workflows: [onboarding], signingSecrets: [process.env.EZGH_WORKFLOWS_SIGNING_SECRET!] })

// node:http to Fetch API and back.
createServer(async (req, res) => {
  const chunks: Buffer[] = []
  for await (const c of req) chunks.push(c as Buffer)
  const headers = new Headers()
  for (const [k, v] of Object.entries(req.headers)) if (typeof v === "string") headers.set(k, v)
  const response = await handler(new Request(`http://localhost${req.url}`, { method: req.method, headers, ...(req.method === "POST" ? { body: Buffer.concat(chunks) } : {}) }))
  res.writeHead(response.status, Object.fromEntries(response.headers))
  res.end(Buffer.from(await response.arrayBuffer()))
}).listen(Number(process.env.PORT ?? 3000))
