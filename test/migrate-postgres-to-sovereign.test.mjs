import test from "node:test";
import assert from "node:assert/strict";
import { migratePostgresOpportunitiesToKashKorner } from "../scripts/migrate-postgres-to-sovereign.mjs";

test("legacy opportunity migration copies records to KashKorner without mutating PostgreSQL", async () => {
  const tables = {
    opportunities: [{
      id: 1, fingerprint: "a".repeat(64), kind: "uniswap-v3-cross-pool", chain_id: 1,
      token_in: "0xabc", token_out: "0xdef", buy_pool: "0x111", sell_pool: "0x222",
      gross_edge_bps: "45.5", estimated_gas_usd: "8", estimated_slippage_usd: "2.1",
      estimated_net_profit_usd: "3.2", observed_at: new Date("2026-10-01T00:00:00Z"), payload: {}
    }],
    ingestion_events: [{ id: 2 }],
    ingestion_dead_letter: [{ id: 3 }],
    raw_liquidity_events: [{ id: 4 }],
    engine_state: [{ key: "scanner" }]
  };
  const queries = [];
  const pool = {
    async query(sql, params = []) {
      queries.push(sql);
      if (/to_regclass/i.test(sql)) {
        const table = params[0];
        return { rows: [{ table_name: Object.hasOwn(tables, table) ? table : null }] };
      }
      if (/COUNT\(\*\)/i.test(sql)) {
        const table = sql.match(/FROM\s+(\w+)/i)?.[1];
        return { rows: [{ rows: (tables[table] || []).length }] };
      }
      const rows = tables.opportunities;
      const limit = Number(params[0]);
      const offset = Number(params[1]);
      return { rows: rows.slice(offset, offset + limit) };
    }
  };
  const saved = [];
  const api = {
    configured: true,
    async saveOpportunity(opportunity) {
      saved.push(opportunity);
      return { success: true };
    }
  };

  const result = await migratePostgresOpportunitiesToKashKorner({ api, pool, pageSize: 10, writeConcurrency: 2 });
  assert.equal(result.databaseModified, false);
  assert.equal(result.rowsRead, 1);
  assert.equal(result.saved, 1);
  assert.equal(saved.length, 1);
  assert.equal(saved[0].fingerprint, "a".repeat(64));
  assert.equal(saved[0].estimated_net_profit_usd, 3.2);
  assert.deepEqual(result.retainedInLegacyDatabase, {
    ingestion_events: 1,
    ingestion_dead_letter: 1,
    raw_liquidity_events: 1,
    engine_state: 1
  });
  assert.ok(queries.every(sql => /^SELECT/i.test(sql.trim())), "migration must never mutate the source database");
});
