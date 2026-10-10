// Deployment IDs from the platform's environment (design, "Runs continue on whatever code is deployed").
export function detectDeployment(): string | undefined {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env
  if (!env) return undefined
  return env.VERCEL_DEPLOYMENT_ID
    || (env.AWS_LAMBDA_FUNCTION_VERSION && env.AWS_LAMBDA_FUNCTION_NAME ? `${env.AWS_LAMBDA_FUNCTION_NAME}:${env.AWS_LAMBDA_FUNCTION_VERSION}` : undefined)
    || env.K_REVISION
    || undefined
}
