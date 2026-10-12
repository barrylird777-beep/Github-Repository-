import test from 'node:test';
import assert from 'node:assert/strict';
import { PureWalQueue } from '../src/queue/pure-wal-queue.mjs';

function makeStore() {
  const records = new Map();
  return {
    records,
    async get(collection, id) { return records.get(`${collection}:${id}`) ?? null; },
    async put(collection, id, record) {
      const stored = { ...record, id: String(id), updatedAt: record.updatedAt || new Date().toISOString() };
      records.set(`${collection}:${id}`, stored);
      return stored;
    },
    async query(collection, filters = {}, { sortField = '', descending = false, limit = 100 } = {}) {
      const rows = [...records.entries()]
        .filter(([key]) => key.startsWith(`${collection}:`))
        .map(([, value]) => value)
        .filter(row => Object.entries(filters).every(([key, value]) => row?.[key] === value));
      if (sortField) rows.sort((a, b) => {
        const left = a?.[sortField], right = b?.[sortField];
        const cmp = typeof left === 'number' && typeof right === 'number'
          ? left - right
          : String(left ?? '').localeCompare(String(right ?? ''));
        return descending ? -cmp : cmp;
      });
      return rows.slice(0, limit);
    }
  };
}

test('Pure-WAL queue deduplicates, leases, and processes an ingestion event', async () => {
  const store = makeStore();
  const queue = new PureWalQueue(store);
  const event = { fingerprint: 'event-1', source: 'test', sequence: 1, observed_at: new Date().toISOString(), payload: { ok: true } };
  assert.equal((await queue.enqueue(event)).duplicate, false);
  assert.equal((await queue.enqueue(event)).duplicate, true);
  const [job] = await queue.claim(1);
  assert.equal(job.status, 'processing');
  assert.ok(job.leaseToken);
  await queue.process(job, async claimed => assert.equal(claimed.fingerprint, 'event-1'));
  assert.equal((await store.get('koin_kob_ingestion_events', 'event-1')).status, 'processed');
  assert.deepEqual(await queue.stats(), { storage: 'pure-wal', total: 1, queued: 0, processing: 0, processed: 1, failed: 0 });
});

test('Pure-WAL queue recovers expired leases and dead-letters exhausted retries', async () => {
  const store = makeStore();
  const queue = new PureWalQueue(store, { leaseSeconds: 10, maxAttempts: 2 });
  await queue.enqueue({ fingerprint: 'event-2', source: 'test', payload: {} });
  const [job] = await queue.claim(1);
  await store.put('koin_kob_ingestion_events', job.id, { ...job, leaseUntil: new Date(Date.now() - 1000).toISOString() });
  assert.equal(await queue.recoverExpired(), 1);
  const [retried] = await queue.claim(1);
  assert.equal(retried.attempts, 2);
  await queue.fail(retried, new Error('synthetic failure'));
  const final = await store.get('koin_kob_ingestion_events', 'event-2');
  assert.equal(final.status, 'failed');
  assert.match(final.lastError, /synthetic failure/);
});

test('Pure-WAL queue persists opportunities, raw events, and engine state', async () => {
  const store = makeStore();
  const queue = new PureWalQueue(store);
  await queue.saveOpportunity({ fingerprint: 'opp-1', kind: 'arbitrage', estimated_net_profit_usd: 12.5 }, '2026-10-12T00:00:00.000Z');
  await queue.saveRawLiquidityEvent({ fingerprint: 'raw-1', blockNumber: 100 });
  await queue.saveEngineState('scanner', { status: 'ok' });
  assert.equal((await store.get('koin_kob_opportunities', 'opp-1')).estimated_net_profit_usd, 12.5);
  assert.equal((await store.get('koin_kob_raw_liquidity_events', 'raw-1')).blockNumber, 100);
  assert.deepEqual((await store.get('koin_kob_engine_state', 'scanner')).value, { status: 'ok' });
});
