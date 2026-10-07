import { expect, test } from "bun:test";
import { COLLECTOR_WIRE_LIMITS, collectorUtf8Bytes, encodeCollectorJson } from "./shared";

test("collector wire encoding sorts object keys, omits undefined properties, and measures UTF-8 bytes", () => {
  expect(COLLECTOR_WIRE_LIMITS).toEqual({
    activityTextBytes: 20_000,
    cursorTokenBytes: 20_000,
    requestBytes: 524_288,
    responseBytes: 524_288,
    topologyBytes: 65_536,
    maxArrayItems: 1_000,
  });
  expect(encodeCollectorJson({ z: 1, a: { keep: "\u0000", omit: undefined }, items: [{ b: 2, a: 1 }] })).toBe('{"a":{"keep":"\\u0000"},"items":[{"a":1,"b":2}],"z":1}');
  expect(collectorUtf8Bytes("界".repeat(8_000))).toBe(24_000);
});
