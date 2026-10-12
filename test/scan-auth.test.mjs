import test from "node:test";
import assert from "node:assert/strict";
import { authorizeScanRequest } from "../src/api/scan-auth.mjs";

const req = authorization => ({ headers: authorization ? { authorization } : {} });

test("scan endpoint rejects missing configuration and missing credentials", () => {
  assert.equal(authorizeScanRequest(req("Bearer secret"), ""), false);
  assert.equal(authorizeScanRequest(req(), "secret"), false);
});

test("scan endpoint accepts only the configured bearer token", () => {
  assert.equal(authorizeScanRequest(req("Bearer wrong"), "secret"), false);
  assert.equal(authorizeScanRequest(req("secret"), "secret"), false);
  assert.equal(authorizeScanRequest(req("Bearer secret"), "secret"), true);
});
