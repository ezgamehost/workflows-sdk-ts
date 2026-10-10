import { serve } from "@ezgh/workflows"
import { onboarding } from "../workflows.ts"

interface Env { EZGH_WORKFLOWS_SIGNING_SECRET: string }
let handler: ((req: Request) => Promise<Response>) | undefined

// Checkpoints are awaited inside the request, so no ctx.waitUntil is needed (design, "Transports").
export default {
  fetch(req: Request, env: Env): Promise<Response> {
    handler ??= serve({ workflows: [onboarding], signingSecrets: [env.EZGH_WORKFLOWS_SIGNING_SECRET], maxInvocationSeconds: 30 })
    return handler(req)
  },
}
