import { serve } from "@ezghcloud/workflows"
import { onboarding } from "../workflows.ts"

const handler = serve({ workflows: [onboarding], signingSecrets: [process.env.EZGH_WORKFLOWS_SIGNING_SECRET] })
Bun.serve({ port: Number(process.env.PORT ?? 3000), fetch: handler })
