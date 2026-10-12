import test from 'node:test';
import assert from 'node:assert/strict';
import { RemotePureWalStore } from '../src/store/remote-pure-wal-store.mjs';

test('remote Pure-WAL store requires an explicit API URL and token', () => {
  assert.throws(() => new RemotePureWalStore({ baseUrl: '', token: 'token' }), /APEX_WORKER_API_URL/);
  assert.throws(() => new RemotePureWalStore({ baseUrl: 'https://apex.invalid', token: '' }), /APEX_OPPORTUNITY_ENGINE_TOKEN/);
});

test('remote Pure-WAL store sends the scoped auth header and returns stored records', async (t) => {
  const originalFetch = globalThis.fetch;
  let seen;
  globalThis.fetch = async (url, options) => {
    seen = { url: String(url), options, body: JSON.parse(options.body) };
    return { ok: true, status: 200, json: async () => ({ success: true, record: { id: 'opp-1', fingerprint: 'opp-1' }, records: [{ id: 'opp-1' }] }) };
  };
  t.after(() => { globalThis.fetch = originalFetch; });

  const store = new RemotePureWalStore({ baseUrl: 'https://apex.example/', token: 'test-secret' });
  const written = await store.put('koin_kob_opportunities', 'opp-1', { fingerprint: 'opp-1' });
  assert.equal(written.id, 'opp-1');
  assert.equal(seen.url, 'https://apex.example/api/internal/opportunity-engine/store/put');
  assert.equal(seen.options.headers['x-apex-opportunity-token'], 'test-secret');
  assert.equal(seen.body.collection, 'koin_kob_opportunities');

  const queried = await store.query('koin_kob_opportunities', { kind: 'arbitrage' }, { sortField: 'estimated_net_profit_usd', descending: true, limit: 10 });
  assert.equal(queried[0].id, 'opp-1');
  assert.equal(seen.body.sortField, 'estimated_net_profit_usd');
  assert.equal(seen.body.descending, true);
});

test('remote Pure-WAL store surfaces API authorization and validation failures', async (t) => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => ({
    ok: false,
    status: 401,
    statusText: 'Unauthorized',
    json: async () => ({ success: false, error: 'Unauthorized' })
  });
  t.after(() => { globalThis.fetch = originalFetch; });

  const store = new RemotePureWalStore({ baseUrl: 'https://apex.example', token: 'wrong-secret' });
  await assert.rejects(() => store.get('koin_kob_opportunities', 'opp-1'), /HTTP 401: Unauthorized/);
});
