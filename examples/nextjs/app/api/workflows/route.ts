import { serve } from "@ezghcloud/workflows"
import { onboarding } from "../../../../workflows.ts"

// Set maxInvocationSeconds to your plan's function limit (Vercel Pro's default is 300 s).
export const maxDuration = 300
export const POST = serve({ workflows: [onboarding], signingSecrets: [process.env.EZGH_WORKFLOWS_SIGNING_SECRET], maxInvocationSeconds: 300 })
