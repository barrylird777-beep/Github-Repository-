import test from "node:test";
import assert from "node:assert/strict";
import { migratePostgresToSovereign } from "../scripts/migrate-postgres-to-sovereign.mjs";

test("legacy opportunity migration copies records to Pure-WAL without mutating PostgreSQL", async () => {
  const tables = {
    opportunities: [{
      id: 1, fingerprint: "a".repeat(64), kind: "uniswap-v3-cross-pool", chain_id: 1,
      gross_edge_bps: "45.5", estimated_gas_usd: "8", estimated_slippage_usd: "2.1",
      estimated_net_profit_usd: "3.2", observed_at: new Date("2026-10-01T00:00:00Z"), payload: {}
    }],
    ingestion_events: [{
      id: 2, fingerprint: "b".repeat(64), source: "test", sequence: 1, payload: { ok: true },
      observed_at: new Date("2026-10-01T00:00:00Z"), status: "processed", attempts: 1,
      created_at: new Date("2026-10-01T00:00:00Z"), updated_at: new Date("2026-10-01T00:00:01Z")
    }],
    ingestion_dead_letter: [{
      id: 3, fingerprint: "c".repeat(64), source: "test", payload: { failed: true },
      attempts: 8, last_error: "test failure", failed_at: new Date("2026-10-01T00:00:02Z")
    }],
    raw_liquidity_events: [{
      id: 4, fingerprint: "d".repeat(64), block_number: "100", transaction_hash: "0x123",
      token0: "0xabc", token1: "0xdef", fee: 3000, tick_spacing: 60, pool: "0x456",
      observed_at: new Date("2026-10-01T00:00:03Z"), payload: {}
    }],
    engine_state: [{ key: "scanner", value: { status: "ok" }, updated_at: new Date("2026-10-01T00:00:04Z") }]
  };
  const queries = [];
  const pool = {
    async query(sql, params = []) {
      queries.push(sql);
      if (/to_regclass/i.test(sql)) {
        const table = params[0];
        return { rows: [{ table_name: Object.hasOwn(tables, table) ? table : null }] };
      }
      const match = sql.match(/FROM\s+(\w+)/i);
      assert.ok(match, "migration query must target a known table");
      const rows = tables[match[1]] || [];
      const limit = Number(params[0]);
      const offset = Number(params[1]);
      return { rows: rows.slice(offset, offset + limit) };
    }
  };
  const written = [];
  const store = {
    configured: true,
    async bulk(items) {
      written.push(...items);
      return { success: true, written: items.length, existing: 0, records: items.map(item => item.record) };
    }
  };

  const result = await migratePostgresToSovereign({ store, pool, pageSize: 10, batchSize: 2 });
  assert.equal(result.databaseModified, false);
  assert.equal(result.rowsRead, 5);
  assert.equal(result.written, 5);
  assert.equal(written.length, 5);
  assert.ok(written.some(item => item.collection === "koin_kob_opportunities" && typeof item.record.estimated_net_profit_usd === "number"));
  assert.ok(written.some(item => item.collection === "koin_kob_ingestion_events" && item.record.status === "dead-letter"));
  assert.ok(written.some(item => item.collection === "koin_kob_raw_liquidity_events" && item.record.blockNumber === 100));
  assert.ok(written.some(item => item.collection === "koin_kob_engine_state" && item.id === "scanner"));
  assert.ok(queries.every(sql => /^SELECT/i.test(sql.trim())), "migration must never mutate the source database");
});
