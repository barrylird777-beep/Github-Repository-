import test from "node:test";
import assert from "node:assert/strict";
import { createZeroTrustHttpPolicy } from "../src/security/zero-trust-http.mjs";

function run(headers = {}, production = true) {
  const set = {};
  const req = { headers, method: "GET", url: "/api/test" };
  const res = { setHeader(k, v) { set[k] = v; } };
  const result = createZeroTrustHttpPolicy({ production, allowedOrigins: ["https://trusted.example"] })(req, res);
  return { set, result };
}

test("adds security headers and no-store", () => {
  const { set, result } = run();
  assert.equal(result.allowed, true);
  assert.equal(set["x-content-type-options"], "nosniff");
  assert.equal(set["cache-control"], "no-store");
  assert.ok(set["x-request-id"]);
});

test("allows a configured origin and reflects only that origin", () => {
  const { set, result } = run({ origin: "https://trusted.example", host: "engine.example" });
  assert.equal(result.allowed, true);
  assert.equal(set["access-control-allow-origin"], "https://trusted.example");
});

test("rejects an untrusted production origin", () => {
  const { result } = run({ origin: "https://evil.example", host: "engine.example" });
  assert.equal(result.allowed, false);
});

test("does not mistake a missing origin for authentication", () => {
  const { result } = run({});
  assert.equal(result.allowed, true);
  // Sensitive handlers still need authorizeScanRequest.
});

test("allows same-origin browser requests", () => {
  const { result } = run({ origin: "https://engine.example", host: "engine.example" });
  assert.equal(result.allowed, true);
});
