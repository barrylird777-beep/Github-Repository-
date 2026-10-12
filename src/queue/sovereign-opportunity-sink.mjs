export class SovereignOpportunitySink {
  constructor({ wal, apiUrl = "", token = "" } = {}) {
    if (!wal) throw new TypeError("wal is required");
    this.wal = wal;
    this.apiUrl = String(apiUrl || "").replace(/\/+$/, "");
    this.token = String(token || "");
    this.pending = new Map();
    this.lastSent = new Map();
    this.ready = false;
    this.readyPromise = null;
    this.lastError = null;
    this.lastSuccessAt = null;
  }

  async init() {
    if (this.ready) return this;
    if (!this.readyPromise) {
      this.readyPromise = (async () => {
        await this.wal.init();
        await this.wal.replay(async event => {
          const fingerprint = String(event?.fingerprint || "");
          if (!fingerprint) return;
          if (event.type === "opportunity-pending" && event.opportunity) {
            this.pending.set(fingerprint, event.opportunity);
          } else if (event.type === "opportunity-sent" && event.opportunity) {
            this.pending.delete(fingerprint);
            this.lastSent.set(fingerprint, event.opportunity);
          }
        });
        this.ready = true;
      })().catch(error => {
        this.readyPromise = null;
        throw error;
      });
    }
    await this.readyPromise;
    return this;
  }

  async saveOpportunity(opportunity, observedAt) {
    await this.init();
    const fingerprint = String(opportunity?.fingerprint || "").trim();
    if (!fingerprint) throw new TypeError("Opportunity fingerprint is required");
    const value = {
      ...opportunity,
      observed_at: opportunity.observed_at || observedAt || new Date().toISOString()
    };
    const prior = this.pending.get(fingerprint);
    const sent = this.lastSent.get(fingerprint);
    if (prior && JSON.stringify(prior) === JSON.stringify(value)) {
      await this.flushPending();
      return { durable: true, queued: this.pending.has(fingerprint), duplicate: true };
    }
    if (!prior && sent && JSON.stringify(sent) === JSON.stringify(value)) {
      return { durable: true, queued: false, duplicate: true };
    }
    await this.wal.append({ type: "opportunity-pending", fingerprint, opportunity: value });
    this.pending.set(fingerprint, value);
    await this.flushPending();
    return { durable: true, queued: this.pending.has(fingerprint), duplicate: false };
  }

  async flushPending() {
    await this.init();
    if (!this.apiUrl || !this.token) {
      this.lastError = "Sovereign API URL or opportunity token is not configured";
      return { delivered: 0, pending: this.pending.size };
    }
    let delivered = 0;
    for (const [fingerprint, opportunity] of this.pending) {
      let response;
      try {
        response = await fetch(this.apiUrl + "/api/kash-korner/opportunities", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-apex-opportunity-token": this.token
          },
          body: JSON.stringify(opportunity)
        });
      } catch (error) {
        this.lastError = String(error?.message || error);
        break;
      }
      if (!response.ok) {
        this.lastError = "Sovereign API rejected opportunity (" + response.status + ")";
        break;
      }
      await this.wal.append({ type: "opportunity-sent", fingerprint, opportunity });
      this.pending.delete(fingerprint);
      this.lastSent.set(fingerprint, opportunity);
      this.lastSuccessAt = new Date().toISOString();
      this.lastError = null;
      delivered++;
    }
    return { delivered, pending: this.pending.size };
  }

  async listOpportunities(limit = 100) {
    await this.init();
    if (!this.apiUrl) throw new Error("Sovereign API URL is not configured");
    const response = await fetch(this.apiUrl + "/api/kash-korner/opportunities?limit=" + encodeURIComponent(String(limit)));
    if (!response.ok) throw new Error("Sovereign API opportunity read failed (" + response.status + ")");
    const body = await response.json();
    if (!Array.isArray(body.items)) throw new Error("Sovereign API returned an invalid opportunity list");
    return body.items;
  }

  status() {
    return {
      storage: this.apiUrl && this.token ? "sovereign-pure-wal" : "local-wal-unconfigured",
      configured: Boolean(this.apiUrl && this.token),
      pending: this.pending.size,
      lastError: this.lastError,
      lastSuccessAt: this.lastSuccessAt
    };
  }

  stats() {
    return {
      queued: this.pending.size,
      processing: 0,
      processed: this.lastSent.size,
      failed: 0,
      dead_letter: 0
    };
  }

  async shutdown() {
    return this.flushPending();
  }
}
