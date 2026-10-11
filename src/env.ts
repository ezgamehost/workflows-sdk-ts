import { truncateUtf8 } from "./protocol/bytes.ts"

// The longest deployment label the engine stores (internal/protocol MaxDeploymentLen), in bytes.
const MAX_DEPLOYMENT_BYTES = 256

// cleanDeployment: a deployment label as the engine takes it: NUL characters removed, cut to
// 256 bytes of UTF-8. Empty is none.
export function cleanDeployment(s: string | undefined): string | undefined {
  if (typeof s !== "string") return undefined
  return truncateUtf8(s.replaceAll("\u0000", ""), MAX_DEPLOYMENT_BYTES) || undefined
}

// Deployment IDs from the platform's environment (design, "Runs continue on whatever code is
// deployed"): Vercel, AWS Lambda (function:version, but not $LATEST), Cloud Run, Fly.io and Render.
export function detectDeployment(): string | undefined {
  try {
    // Deno throws on process.env without --allow-env.
    const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env
    if (!env) return undefined
    if (env.VERCEL_DEPLOYMENT_ID) return cleanDeployment(env.VERCEL_DEPLOYMENT_ID)
    const fn = env.AWS_LAMBDA_FUNCTION_NAME, version = env.AWS_LAMBDA_FUNCTION_VERSION
    if (fn && version && version !== "$LATEST") return cleanDeployment(`${fn}:${version}`)
    for (const k of ["K_REVISION", "FLY_MACHINE_VERSION", "RENDER_GIT_COMMIT"]) if (env[k]) return cleanDeployment(env[k])
    return undefined
  } catch {
    return undefined
  }
}
