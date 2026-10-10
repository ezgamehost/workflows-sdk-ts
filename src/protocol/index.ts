import type { RetryPolicy } from "./types.ts"

export type * from "./types.ts"
export { MAX_OUTPUT_BYTES, backoff, encodeValue, jsonByteLength, parseDuration } from "./codec.ts"
export { MAX_SIGNATURE_AGE_SECONDS, SECRET_PREFIX, SIGNATURE_HEADER, SignatureError, decodeSecret, sign, verify } from "./signature.ts"
export { stepId, validRunKey, validStepIdOverride, validWorkflowName } from "./stepid.ts"
export { isInvocationToken, shardOf } from "./tokens.ts"

export const PROTOCOL_VERSION = "1.0"
export const DEFAULT_RETRY: Readonly<RetryPolicy> = Object.freeze({ maxAttempts: 5, initialMs: 1000, maxMs: 600_000, multiplier: 2 })
