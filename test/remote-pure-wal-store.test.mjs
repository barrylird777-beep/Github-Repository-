import test from 'node:test';
import assert from 'node:assert/strict';
import { RemotePureWalStore } from '../src/store/remote-pure-wal-store.mjs';

test('remote Pure-WAL client requires configured API URL and token', () => {
  assert.throws(() => new RemotePureWalStore({ baseUrl: '', token: 'x' }), /APEX_WORKER_API_URL/);
  assert.throws(() => new RemotePureWalStore({ baseUrl: 'https://apex.example', token: '' }), /APEX_OPPORTUNITY_ENGINE_TOKEN/);
});

test('remote Pure-WAL client sends the scoped token and uses get/query/put endpoints', async (t) => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url: String(url), options, body: JSON.parse(options.body) });
    return { ok: true, status: 200, json: async () => ({ success: true, record: { id: 'opp-1' }, records: [{ id: 'opp-1' }] }) };
  };
  t.after(() => { globalThis.fetch = originalFetch; });

  const store = new RemotePureWalStore({ baseUrl: 'https://apex.example/', token: 'test-token' });
  assert.equal((await store.get('koin_kob_opportunities', 'opp-1')).id, 'opp-1');
  assert.equal((await store.query('koin_kob_opportunities', { kind: 'arbitrage' }, { sortField: 'estimated_net_profit_usd', descending: true, limit: 10 }))[0].id, 'opp-1');
  assert.equal((await store.put('koin_kob_opportunities', 'opp-1', { kind: 'arbitrage' })).id, 'opp-1');
  const bulk = await store.putMany([{ collection: 'koin_kob_opportunities', id: 'opp-2', record: { kind: 'arbitrage' } }]);
  assert.equal(bulk.records[0].id, 'opp-1');
  assert.deepEqual(calls.map(call => call.url), [
    'https://apex.example/api/internal/opportunity-engine/store/get',
    'https://apex.example/api/internal/opportunity-engine/store/query',
    'https://apex.example/api/internal/opportunity-engine/store/put',
    'https://apex.example/api/internal/opportunity-engine/store/bulk'
  ]);
  assert.ok(calls.every(call => call.options.headers['x-apex-opportunity-token'] === 'test-token'));
  assert.equal(calls[1].body.sortField, 'estimated_net_profit_usd');
});

test('remote Pure-WAL client rejects failed API responses', async (t) => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => ({ ok: false, status: 401, statusText: 'Unauthorized', json: async () => ({ success: false, error: 'Unauthorized' }) });
  t.after(() => { globalThis.fetch = originalFetch; });
  const store = new RemotePureWalStore({ baseUrl: 'https://apex.example', token: 'wrong' });
  await assert.rejects(() => store.get('koin_kob_opportunities', 'opp-1'), /HTTP 401: Unauthorized/);
});
