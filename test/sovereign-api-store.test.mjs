import test from "node:test";
import assert from "node:assert/strict";
import { SovereignApiStore } from "../src/queue/sovereign-api-store.mjs";

test("SovereignApiStore sends authenticated Pure-WAL writes", async () => {
  let request;
  const store = new SovereignApiStore({
    baseUrl: "https://apex.example/",
    token: "test-token",
    fetchImpl: async (url, options) => {
      request = { url, options };
      return { ok: true, status: 200, json: async () => ({ success: true, record: { id: "op-1" } }) };
    }
  });

  const record = await store.put("koin_kob_opportunities", "op-1", { kind: "test" });
  assert.deepEqual(record, { id: "op-1" });
  assert.equal(request.url, "https://apex.example/api/internal/opportunity-engine/store/put");
  assert.equal(request.options.method, "POST");
  assert.equal(request.options.headers["x-apex-opportunity-token"], "test-token");
  assert.deepEqual(JSON.parse(request.options.body), {
    collection: "koin_kob_opportunities",
    id: "op-1",
    record: { kind: "test" }
  });
});

test("SovereignApiStore preserves allowlisted query parameters", async () => {
  let body;
  const store = new SovereignApiStore({
    baseUrl: "https://apex.example",
    token: "test-token",
    fetchImpl: async (_url, options) => {
      body = JSON.parse(options.body);
      return { ok: true, status: 200, json: async () => ({ success: true, records: [{ id: "op-1" }] }) };
    }
  });

  const records = await store.query("koin_kob_opportunities", {
    filters: { source: "live" },
    sortField: "estimated_net_profit_usd",
    descending: true,
    limit: 100
  });
  assert.deepEqual(records, [{ id: "op-1" }]);
  assert.deepEqual(body, {
    collection: "koin_kob_opportunities",
    filters: { source: "live" },
    sortField: "estimated_net_profit_usd",
    descending: true,
    limit: 100
  });
});

test("SovereignApiStore fails closed when credentials are missing or rejected", async () => {
  const missing = new SovereignApiStore({ baseUrl: "", token: "" });
  await assert.rejects(missing.get("koin_kob_opportunities", "op-1"), /not configured/);

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
  await assert.rejects(unauthorized.get("koin_kob_opportunities", "op-1"), /failed \(401\)/);
});

test("SovereignApiStore limits bulk batches to the API contract", async () => {
  const store = new SovereignApiStore({ baseUrl: "https://apex.example", token: "test-token" });
  await assert.rejects(store.bulk([]), /1–100 records/);
  await assert.rejects(store.bulk(Array.from({ length: 101 }, (_, index) => ({ collection: "koin_kob_opportunities", id: String(index), record: {} }))), /1–100 records/);
});
