import test from "node:test";
import assert from "node:assert/strict";
import { authorizeScanRequest } from "../src/api/scan-auth.mjs";
import { readFile } from "node:fs/promises";

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

test("opportunity ingestion is authenticated before request-body parsing or persistence", async () => {
  const source = await readFile(new URL("../src/index.mjs", import.meta.url), "utf8");
  const start = source.indexOf('u.pathname==="/api/ingest"');
  const end = source.indexOf('u.pathname==="/api/scan"', start);
  assert.ok(start >= 0 && end > start, "ingest and scan routes must exist");
  const route = source.slice(start, end);
  assert.match(route, /if\(!authorizeScanRequest\(req\)\)return json\(res,401/);
  assert.ok(route.indexOf("authorizeScanRequest(req)") < route.indexOf("await read(req)"), "authorization must happen before body parsing");
});
