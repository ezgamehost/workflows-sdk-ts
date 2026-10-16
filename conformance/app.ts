// The conformance app: workflows-stack protocol/conformance/workflows.md, served for
// cmd/workflows-conformance (scripts/conformance.sh).
import { serve } from "@ezghcloud/workflows"
import { all } from "./workflows.ts"

const handler = serve({ workflows: all, signingSecrets: [process.env.CONFORMANCE_SECRET!], maxInvocationSeconds: 300, deployment: "conformance" })
Bun.serve({ hostname: "127.0.0.1", port: Number(process.env.PORT ?? 3100), fetch: handler })
