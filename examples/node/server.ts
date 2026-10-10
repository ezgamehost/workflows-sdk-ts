import { createServer } from "node:http"
import { Readable } from "node:stream"
import { serve } from "@ezgh/workflows"
import { onboarding } from "../workflows.ts"

const handler = serve({ workflows: [onboarding], signingSecrets: [process.env.EZGH_WORKFLOWS_SIGNING_SECRET] })

// node:http to Fetch API and back. The body is streamed, not buffered: serve() reads at most 8 MiB
// of it, and only after the signature header checks out.
createServer(async (req, res) => {
  const headers = new Headers()
  for (const [k, v] of Object.entries(req.headers)) if (typeof v === "string") headers.set(k, v)
  const hasBody = req.method !== "GET" && req.method !== "HEAD"
  const response = await handler(new Request(`http://localhost${req.url}`, {
    method: req.method, headers,
    ...(hasBody ? { body: Readable.toWeb(req) as unknown as BodyInit, duplex: "half" } : {}),
  } as RequestInit))
  // An unread (refused) body is dropped once the answer is out.
  res.on("finish", () => { if (!req.readableEnded) req.destroy() })
  res.writeHead(response.status, Object.fromEntries(response.headers))
  res.end(Buffer.from(await response.arrayBuffer()))
}).listen(Number(process.env.PORT ?? 3000))
