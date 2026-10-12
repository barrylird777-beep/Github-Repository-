import test from 'node:test';
import assert from 'node:assert/strict';
import { migrateLegacyPostgresToPureWal } from '../src/migrate-legacy-postgres-to-pure-wal.mjs';

function makeStore() {
  const records = new Map();
  return {
    records,
    async get(collection, id) { return records.get(`${collection}:${id}`) ?? null; },
    async put(collection, id, record) {
      const value = { ...record, id: String(id), updatedAt: record.updatedAt || new Date().toISOString() };
      records.set(`${collection}:${id}`, value);
      return value;
    }
  };
}

function makePool() {
  let ended = false;
  const stamp = new Date('2026-10-12T00:00:00.000Z');
  const rowsByTable = {
    ingestion_events: [{ id: 1, fingerprint: 'evt-1', source: 'test', sequence: 1, observed_at: stamp, payload: { event: true }, status: 'queued', attempts: 0, lease_token: null, lease_until: null, last_error: null, next_attempt_at: stamp, created_at: stamp, updated_at: stamp }],
    opportunities: [{ id: 1, fingerprint: 'opp-1', kind: 'arbitrage', chain_id: 1, token_in: 'A', token_out: 'B', buy_pool: 'buy', sell_pool: 'sell', gross_edge_bps: '12.5', estimated_gas_usd: '1.25', estimated_slippage_usd: '0.25', estimated_net_profit_usd: '11', observed_at: stamp, payload: { extra: true }, created_at: stamp }],
    raw_liquidity_events: [{ id: 1, fingerprint: 'raw-1', block_number: '123', transaction_hash: '0xhash', token0: 'A', token1: 'B', fee: 3000, tick_spacing: 60, pool: '0xpool', observed_at: stamp, payload: { raw: true } }]
  };
  return {
    get ended() { return ended; },
    async query(sql, values = []) {
      if (sql.includes('FROM engine_state')) return { rows: [{ key: 'scanner', value: { status: 'ok' }, updated_at: stamp }] };
      const table = Object.keys(rowsByTable).find(name => sql.includes(`FROM ${name}`));
      if (!table) throw new Error('unexpected SQL in migration test');
      return { rows: Number(values[0] || 0) === 0 ? rowsByTable[table] : [] };
    },
    async end() { ended = true; }
  };
}

test('legacy migration copies all four datasets into KoinKob Pure-WAL and marks completion', async () => {
  const store = makeStore();
  const pool = makePool();
  const logs = [];
  const result = await migrateLegacyPostgresToPureWal({
    remoteStore: store,
    databaseUrl: 'postgres://legacy.invalid/test',
    poolFactory: async url => { assert.equal(url, 'postgres://legacy.invalid/test'); return pool; },
    logger: { log: (...args) => logs.push(args.join(' ')) }
  });
  assert.equal(result.skipped, false);
  assert.deepEqual(result.counts, { ingestionEvents: 1, opportunities: 1, engineState: 1, rawLiquidityEvents: 1 });
  assert.equal((await store.get('koin_kob_ingestion_events', 'evt-1')).status, 'queued');
  assert.equal((await store.get('koin_kob_opportunities', 'opp-1')).estimated_net_profit_usd, 11);
  assert.equal((await store.get('koin_kob_engine_state', 'scanner')).value.status, 'ok');
  assert.equal((await store.get('koin_kob_raw_liquidity_events', 'raw-1')).blockNumber, 123);
  assert.equal((await store.get('koin_kob_engine_state', 'legacy-postgres-migration-v1')).value.counts.opportunities, 1);
  assert.equal(pool.ended, true);
  assert.match(logs.join('\n'), /snapshot copied to Pure-WAL/);

  const secondRun = await migrateLegacyPostgresToPureWal({
    remoteStore: store,
    databaseUrl: 'postgres://legacy.invalid/test',
    poolFactory: async () => { throw new Error('migration marker should prevent a second DB connection'); },
    logger: { log: () => {} }
  });
  assert.equal(secondRun.skipped, true);
});

test('legacy migration skips PostgreSQL when no source URL is configured', async () => {
  const store = makeStore();
  const result = await migrateLegacyPostgresToPureWal({
    remoteStore: store,
    databaseUrl: '',
    poolFactory: async () => { throw new Error('pool must not be created without a source URL'); },
    logger: { log: () => {} }
  });
  assert.equal(result.skipped, true);
  assert.equal(store.records.size, 0);
});
