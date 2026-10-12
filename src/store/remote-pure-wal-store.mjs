export class RemotePureWalStore {
  constructor({ baseUrl = process.env.APEX_WORKER_API_URL, token = process.env.APEX_OPPORTUNITY_ENGINE_TOKEN, timeoutMs = 12000 } = {}) {
    this.baseUrl = String(baseUrl || '').replace(/\/+$/, '');
    this.token = String(token || '');
    this.timeoutMs = Math.max(1000, Number(timeoutMs) || 12000);
    let parsed;
    try { parsed = new URL(this.baseUrl); } catch { throw new Error('APEX_WORKER_API_URL must be an absolute HTTP(S) URL'); }
    if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error('APEX_WORKER_API_URL must use HTTP(S)');
    if (!this.token) throw new Error('APEX_OPPORTUNITY_ENGINE_TOKEN is required for sovereign storage');
  }

  async request(action, payload) {
    const response = await fetch(`${this.baseUrl}/api/internal/opportunity-engine/store/${action}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-apex-opportunity-token': this.token
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(this.timeoutMs)
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.success !== true) {
      const reason = String(data.error || response.statusText || 'remote Pure-WAL request failed').slice(0, 300);
      throw new Error(`Sovereign Pure-WAL ${action} failed (HTTP ${response.status}): ${reason}`);
    }
    return data;
  }

  async get(collection, id) {
    const result = await this.request('get', { collection, id });
    return result.record ?? null;
  }

  async query(collection, filters = {}, { sortField = '', descending = false, limit = 100 } = {}) {
    const result = await this.request('query', { collection, filters, sortField, descending, limit });
    return Array.isArray(result.records) ? result.records : [];
  }

  async put(collection, id, record) {
    const result = await this.request('put', { collection, id, record });
    return result.record;
  }

  async putMany(items) {
    if (!Array.isArray(items) || !items.length || items.length > 100) throw new Error('Pure-WAL bulk writes require 1 to 100 items');
    const result = await this.request('bulk', { items });
    return {
      written: Number(result.written || 0),
      existing: Number(result.existing || 0),
      records: Array.isArray(result.records) ? result.records : []
    };
  }
}
