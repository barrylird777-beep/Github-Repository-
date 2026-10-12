import { randomUUID } from 'node:crypto';

const EVENTS = 'koin_kob_ingestion_events';
const OPPORTUNITIES = 'koin_kob_opportunities';
const ENGINE_STATE = 'koin_kob_engine_state';
const RAW_EVENTS = 'koin_kob_raw_liquidity_events';

function backoff(attempts) {
  const base = Math.min(300, 2 ** Math.min(Math.max(1, Number(attempts) || 1), 8));
  const jitter = Math.floor(Math.random() * Math.max(1, Math.floor(base * 0.25)));
  return base + jitter;
}

export class PureWalQueue {
  constructor(store, { leaseSeconds = 60, maxAttempts = 8 } = {}) {
    if (!store) throw new TypeError('remote Pure-WAL store is required');
    this.store = store;
    this.leaseSeconds = Math.max(10, Number(leaseSeconds) || 60);
    this.maxAttempts = Math.max(1, Number(maxAttempts) || 8);
  }

  async enqueue(event) {
    const fingerprint = String(event?.fingerprint || '');
    if (!fingerprint) throw new Error('ingestion event fingerprint is required');
    const existing = await this.store.get(EVENTS, fingerprint);
    if (existing) return { id: fingerprint, duplicate: true, status: existing.status };
    const now = new Date().toISOString();
    await this.store.put(EVENTS, fingerprint, {
      id: fingerprint,
      fingerprint,
      source: String(event.source || 'unknown'),
      sequence: event.sequence ?? null,
      payload: event.payload ?? null,
      observed_at: event.observed_at || now,
      status: 'queued',
      attempts: 0,
      nextAttemptAt: now,
      leaseToken: null,
      leaseUntil: null,
      lastError: null,
      createdAt: now,
      updatedAt: now
    });
    return { id: fingerprint, duplicate: false, status: 'queued' };
  }

  async recoverExpired() {
    const now = Date.now();
    const rows = await this.store.query(EVENTS, { status: 'processing' }, { limit: 500 });
    let recovered = 0;
    for (const row of rows) {
      const leaseUntil = Date.parse(row.leaseUntil || '');
      if (!Number.isFinite(leaseUntil) || leaseUntil > now) continue;
      await this.store.put(EVENTS, row.id, {
        ...row, status: 'queued', leaseToken: null, leaseUntil: null,
        nextAttemptAt: new Date(now).toISOString(), updatedAt: new Date().toISOString()
      });
      recovered += 1;
    }
    return recovered;
  }

  async claim(limit = 8) {
    const now = Date.now();
    const rows = await this.store.query(EVENTS, { status: 'queued' }, { sortField: 'createdAt', descending: false, limit: 500 });
    const claimed = [];
    for (const row of rows) {
      if (claimed.length >= Math.max(1, Number(limit) || 1)) break;
      if (Number(row.attempts || 0) >= this.maxAttempts) continue;
      const nextAttempt = Date.parse(row.nextAttemptAt || '');
      if (Number.isFinite(nextAttempt) && nextAttempt > now) continue;
      const leaseToken = randomUUID();
      const job = {
        ...row,
        status: 'processing',
        attempts: Number(row.attempts || 0) + 1,
        leaseToken,
        leaseUntil: new Date(now + this.leaseSeconds * 1000).toISOString(),
        updatedAt: new Date(now).toISOString()
      };
      await this.store.put(EVENTS, row.id, job);
      claimed.push(job);
    }
    return claimed;
  }

  async process(job, handler = async () => {}) {
    await handler(job);
    const current = await this.store.get(EVENTS, job.id);
    if (!current || current.status !== 'processing' || current.leaseToken !== job.leaseToken) {
      throw new Error('lease lost');
    }
    await this.store.put(EVENTS, job.id, {
      ...current, status: 'processed', leaseToken: null, leaseUntil: null,
      lastError: null, updatedAt: new Date().toISOString()
    });
  }

  async fail(job, error) {
    const current = await this.store.get(EVENTS, job.id);
    if (!current || current.status !== 'processing' || current.leaseToken !== job.leaseToken) return false;
    const message = String(error?.message || error).slice(0, 1000);
    const attempts = Number(current.attempts || 0);
    const terminal = attempts >= this.maxAttempts;
    const now = Date.now();
    await this.store.put(EVENTS, job.id, {
      ...current,
      status: terminal ? 'failed' : 'queued',
      leaseToken: null,
      leaseUntil: null,
      lastError: message,
      nextAttemptAt: terminal ? current.nextAttemptAt : new Date(now + backoff(attempts) * 1000).toISOString(),
      updatedAt: new Date(now).toISOString()
    });
    return true;
  }

  async saveOpportunity(opportunity, observedAt = null) {
    const fingerprint = String(opportunity?.fingerprint || '');
    if (!fingerprint) throw new Error('opportunity fingerprint is required');
    const existing = await this.store.get(OPPORTUNITIES, fingerprint);
    const now = new Date().toISOString();
    return this.store.put(OPPORTUNITIES, fingerprint, {
      ...(existing || {}),
      ...opportunity,
      id: fingerprint,
      fingerprint,
      observed_at: observedAt || opportunity.observed_at || now,
      createdAt: existing?.createdAt || now,
      updatedAt: now
    });
  }

  async saveRawLiquidityEvent(event) {
    const fingerprint = String(event?.fingerprint || '');
    if (!fingerprint) throw new Error('raw liquidity event fingerprint is required');
    return this.store.put(RAW_EVENTS, fingerprint, { ...event, id: fingerprint, fingerprint });
  }

  async saveEngineState(key, value) {
    const id = String(key || '');
    if (!id) throw new Error('engine state key is required');
    return this.store.put(ENGINE_STATE, id, { id, key: id, value, updatedAt: new Date().toISOString() });
  }

  async stats() {
    const rows = await this.store.query(EVENTS, {}, { limit: 500 });
    return {
      storage: 'pure-wal',
      total: rows.length,
      queued: rows.filter(row => row.status === 'queued').length,
      processing: rows.filter(row => row.status === 'processing').length,
      processed: rows.filter(row => row.status === 'processed').length,
      failed: rows.filter(row => row.status === 'failed').length
    };
  }

  async shutdown() {}
}
