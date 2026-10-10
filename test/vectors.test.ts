import { describe, expect, test } from "bun:test"
import backoffVectors from "./vectors/backoff.json"
import codec from "./vectors/codec.json"
import errors from "./vectors/errors.json"
import tokens from "./vectors/invocation-tokens.json"
import messages from "./vectors/messages.json"
import runIds from "./vectors/run-ids.json"
import signatures from "./vectors/signatures.json"
import stepIds from "./vectors/step-ids.json"
import unknownFields from "./vectors/unknown-fields.json"
import { backoff, decodeSecret, encodeValue, isInvocationToken, shardOf, sign, SignatureError, stepId, verify } from "../src/protocol/index.ts"
import { decodeError, encodeError } from "../src/runtime/errors.ts"
import { InvalidInvocation, parseInvocation } from "../src/runtime/invocation.ts"
import { codecValues } from "./helpers.ts"
import callbackTokens from "./vectors/callback-tokens.json"
import derivedIds from "./vectors/derived-ids.json"
import { callbackTokenHash, mintCallbackToken, parseCallbackToken } from "../src/protocol/callback.ts"
import { childRunId, rfc3339Nano, scheduledRunId, triggeredRunId } from "../src/protocol/derived.ts"

const utf8 = (s: string) => new TextEncoder().encode(s)

describe("workflows-stack protocol vectors", () => {
  test("step IDs", async () => {
    for (const c of stepIds.cases) expect([c.name, c.occurrence, await stepId(c.name, c.occurrence)]).toEqual([c.name, c.occurrence, c.stepId])
  })
  test("signing", async () => {
    for (const c of signatures.sign) expect(await sign(c.secrets.map(decodeSecret), new Date(c.t * 1000), utf8(c.body))).toBe(c.header)
  })
  test("verifying", async () => {
    for (const c of signatures.verify) {
      const err = await verify(c.header, c.secrets.map(decodeSecret), utf8(c.body), new Date(c.now * 1000)).then(() => null, (e) => e)
      if (c.result === "ok") expect(err).toBeNull()
      else {
        expect(err).toBeInstanceOf(SignatureError)
        expect([c.header, c.now, (err as SignatureError).reason]).toEqual([c.header, c.now, c.result as "expired" | "invalid"])
      }
    }
  })
  test("tokens", () => {
    for (const t of tokens.valid) expect([t.token, isInvocationToken(t.token)]).toEqual([t.token, true])
    for (const t of tokens.invalid) expect([t.token, t.why, isInvocationToken(t.token)]).toEqual([t.token, t.why, false])
  })
  test("run IDs carry their shard", () => {
    for (const c of runIds.valid) expect([c.id, shardOf(c.id)]).toEqual([c.id, c.shard])
    for (const c of runIds.invalid) expect([c.id, c.why, shardOf(c.id)]).toEqual([c.id, c.why, undefined])
  })
  test("backoff", () => {
    backoffVectors.delaysMs.forEach((ms, i) => expect([i + 1, backoff(backoffVectors.policy, i + 1)]).toEqual([i + 1, ms]))
  })
  test("codec", () => {
    for (const c of codec.cases) {
      expect(c.tag in codecValues).toBe(true)
      if ("error" in c) expect(() => encodeValue(codecValues[c.tag])).toThrow(TypeError)
      else expect([c.tag, encodeValue(codecValues[c.tag])]).toEqual([c.tag, c.encoded])
    }
  })
  test("errors survive decode and encode unchanged", () => {
    for (const c of errors.cases) expect(encodeError(decodeError(c.wire as never))).toEqual(c.wire as never)
  })
  test("invocations parse; unknown fields are ignored", () => {
    for (const inv of [...messages.invocation.valid, ...unknownFields.cases.map((c) => c.invocation)]) {
      const parsed = parseInvocation(JSON.stringify(inv))
      expect(parsed.invocationId).toBe(inv.invocationId)
      expect(parsed.kind as string).toBe(inv.kind)
    }
  })
  test("invalid invocations are refused", () => {
    for (const inv of messages.invocation.invalid) expect(() => parseInvocation(JSON.stringify(inv))).toThrow(InvalidInvocation)
  })
  test("callback-tokens.json: parse, shard, hash and mint", async () => {
    for (const c of callbackTokens.valid) {
      expect([c.token, parseCallbackToken(c.token).shard]).toEqual([c.token, c.shard])
      expect([c.token, await callbackTokenHash(c.token)]).toEqual([c.token, c.hashHex])
      const secret = new Uint8Array(c.secretHex.match(/../g)!.map((h) => parseInt(h, 16)))
      expect(mintCallbackToken(c.shard, () => secret)).toBe(c.token)
    }
    for (const c of callbackTokens.invalid) expect([c.token, c.why, (() => { try { parseCallbackToken(c.token); return "parsed" } catch { return "refused" } })()]).toEqual([c.token, c.why, "refused"])
  })
  test("derived-ids.json: child, scheduled and triggered run IDs carry their shard", async () => {
    for (const c of derivedIds.child) expect([c.parent, c.stepId, await childRunId(c.parent, c.stepId), shardOf(c.id)]).toEqual([c.parent, c.stepId, c.id, c.shard])
    for (const c of derivedIds.scheduled) expect([c.scheduleId, c.scheduledTime, await scheduledRunId(c.scheduleId, c.scheduledTime), shardOf(c.id)]).toEqual([c.scheduleId, c.scheduledTime, c.id, c.shard])
    for (const c of derivedIds.scheduled) expect(await scheduledRunId(c.scheduleId, new Date(c.scheduledTime))).toBe(c.id)
    for (const c of derivedIds.triggered) expect([c.scheduleId, c.idempotencyKey, await triggeredRunId(c.scheduleId, c.idempotencyKey), shardOf(c.id)]).toEqual([c.scheduleId, c.idempotencyKey, c.id, c.shard])
  })
  test("rfc3339Nano trims like Go and converts to UTC", () => {
    expect(rfc3339Nano("2026-10-10T00:00:00.000Z")).toBe("2026-10-10T00:00:00Z")
    expect(rfc3339Nano("2026-10-10T00:00:00.120Z")).toBe("2026-10-10T00:00:00.12Z")
    expect(rfc3339Nano("2026-10-10T02:00:00.123456789+02:00")).toBe("2026-10-10T00:00:00.123456789Z")
    expect(rfc3339Nano(new Date("2026-10-10T00:00:01.500Z"))).toBe("2026-10-10T00:00:01.5Z")
  })
})
