import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { SovereignApiStore } from "../src/queue/sovereign-api-store.mjs";

test("SovereignApiStore sends authenticated opportunity writes to KashKorner", async () => {
  let request;
  const store = new SovereignApiStore({
    baseUrl: "https://apex.example/",
    token: "test-token",
    fetchImpl: async (url, options) => {
      request = { url, options };
      return { ok: true, status: 202, json: async () => ({ success: true, id: "op-1" }) };
    }
  });

  const opportunity = { fingerprint: "stable-fingerprint", estimated_net_profit_usd: 3.2 };
  const result = await store.saveOpportunity(opportunity);
  assert.equal(result.success, true);
  assert.equal(request.url, "https://apex.example/api/kash-korner/opportunities");
  assert.equal(request.options.method, "POST");
  assert.equal(request.options.headers["x-apex-opportunity-token"], "test-token");
  assert.deepEqual(JSON.parse(request.options.body), { opportunity });
});

test("SovereignApiStore reads the public KashKorner opportunity feed", async () => {
  let request;
  const store = new SovereignApiStore({
    baseUrl: "https://apex.example",
    token: "test-token",
    fetchImpl: async (url, options) => {
      request = { url, options };
      return { ok: true, status: 200, json: async () => ({ success: true, items: [{ id: "op-1" }] }) };
    }
  });

  const items = await store.listOpportunities(17);
  assert.deepEqual(items, [{ id: "op-1" }]);
  assert.equal(request.url, "https://apex.example/api/kash-korner/opportunities?limit=17");
  assert.equal(request.options.method, "GET");
});

test("SovereignApiStore fails closed when write credentials are missing or rejected", async () => {
  const missing = new SovereignApiStore({ baseUrl: "https://apex.example", token: "" });
  await assert.rejects(missing.saveOpportunity({ fingerprint: "op-1" }), /credentials are not configured/);

  const unauthorized = new SovereignApiStore({
    baseUrl: "https://apex.example",
    token: "bad-token",
    fetchImpl: async () => ({
      ok: false,
      status: 401,
      statusText: "Unauthorized",
      json: async () => ({ success: false, error: "Unauthorized" })
    })
  });
  await assert.rejects(unauthorized.saveOpportunity({ fingerprint: "op-1" }), /failed \(401\)/);
});

test("active Opportunity Engine runtime has no PostgreSQL queue path", async () => {
  const source = await readFile(new URL("../src/index.mjs", import.meta.url), "utf8");
  assert.doesNotMatch(source, /import\s+pg\s+from/);
  assert.doesNotMatch(source, /PostgresQueue|new\s+pg\.Pool|postgres:Boolean\(pool\)/i);
  assert.match(source, /SovereignApiStore/);
  assert.match(source, /store\.saveOpportunity/);
});
