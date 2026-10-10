// Type-level checks (tsc --noEmit runs them): Jsonify and the open unions.
import { expect, test } from "bun:test"
import type { App, Jsonify, Run } from "../src/index.ts"

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false
const assert = <T extends true>(_: T) => {}

test("Jsonify", () => {
  assert<Equal<Jsonify<{ a?: Date }>, { a?: string }>>(true)
  assert<Equal<Jsonify<{ a: number | undefined; b: string }>, { a?: number; b: string }>>(true)
  assert<Equal<Jsonify<(Date | undefined)[]>, (string | null)[]>>(true)
  assert<Equal<Jsonify<undefined>, null>>(true)
  assert<Equal<Jsonify<{ n: { d: Date } }>, { n: { d: string } }>>(true)
  expect(true).toBe(true)
})

test("RunStatus and App.kind stay open to values a later API adds", () => {
  const s: Run["status"] = "paused"
  const k: App["kind"] = "scheduled"
  expect([s, k]).toEqual(["paused", "scheduled"])
})
