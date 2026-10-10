import { toHex, utf8 } from "./bytes.ts"

// stepId: lowercase hex of the first 16 bytes of SHA-256(name + "\0" + decimal occurrence).
export async function stepId(name: string, occurrence: number): Promise<string> {
  const d = new Uint8Array(await crypto.subtle.digest("SHA-256", utf8(`${name}\u0000${occurrence}`)))
  return toHex(d.subarray(0, 16))
}

export const validStepIdOverride = (s: string) => typeof s === "string" && /^[A-Za-z0-9._:-]{1,128}$/.test(s)
export const validWorkflowName = (s: string) => typeof s === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(s)
export const validRunKey = validStepIdOverride
