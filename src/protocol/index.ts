import type { RetryPolicy } from "./types.ts"

export type * from "./types.ts"
export { MAX_OUTPUT_BYTES, backoff, encodeValue, jsonByteLength, parseDuration } from "./codec.ts"
export { MAX_SIGNATURE_AGE_SECONDS, MAX_SIGNATURES, SECRET_PREFIX, SIGNATURE_HEADER, SignatureError, decodeSecret, parseSignature, sign, verify, verifyParsed } from "./signature.ts"
export type { ParsedSignature } from "./signature.ts"
export { stepId, validRunKey, validStepIdOverride, validWorkflowName } from "./stepid.ts"
export { isInvocationToken, shardOf } from "./tokens.ts"

// The newest protocol version this SDK speaks, and every one it speaks (sync replies and pull polls
// declare them; the engine picks the newest it shares).
export const PROTOCOL_VERSION = "1.2"
export const PROTOCOL_VERSIONS: readonly string[] = Object.freeze(["1.0", "1.1", "1.2"])
// supportsParallel: protocol 1.1 and later plan parallel steps and allow callbacks and invoke.
export function supportsParallel(version: string): boolean {
  const [major, minor] = String(version).split(".").map(Number)
  return major === 1 && (minor ?? 0) >= 1
}
// supportsMessages: protocol 1.2 and later wait for queue messages (step.waitForMessage).
export function supportsMessages(version: string): boolean {
  const [major, minor] = String(version).split(".").map(Number)
  return major === 1 && (minor ?? 0) >= 2
}
export { callbackTokenHash, mintCallbackToken, parseCallbackToken } from "./callback.ts"
export { childRunId, derivedRunId, rfc3339Nano, scheduledRunId, triggeredRunId } from "./derived.ts"
export const DEFAULT_RETRY: Readonly<RetryPolicy> = Object.freeze({ maxAttempts: 5, initialMs: 1000, maxMs: 600_000, multiplier: 2 })
