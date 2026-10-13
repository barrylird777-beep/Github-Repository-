import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

/**
 * Bounded, local JSONL ring log for PAPER-ONLY evaluation results.
 * It stores newest records first-in-order and atomically trims old records.
 * This module deliberately has no transaction signer or submission client.
 */
export class JsonlRingWal {
  constructor({ dir = "./data/paper", fileName = "paper-events.jsonl", maxRecords = 5000, maxBytes = 8 * 1024 * 1024 } = {}) {
    this.dir = path.resolve(dir);
    this.file = path.join(this.dir, fileName);
    this.maxRecords = Math.max(100, Number(maxRecords) || 5000);
    this.maxBytes = Math.max(64 * 1024, Number(maxBytes) || 8 * 1024 * 1024);
    this.chain = Promise.resolve();
  }

  async init() {
    await fs.mkdir(this.dir, { recursive: true, mode: 0o700 });
    try {
      await fs.access(this.file);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      const handle = await fs.open(this.file, "wx", 0o600);
      try { await handle.sync(); } finally { await handle.close(); }
      await this.syncDir();
    }
    return this;
  }

  async syncDir() {
    const handle = await fs.open(this.dir, "r");
    try { await handle.sync(); } finally { await handle.close(); }
  }

  async readRecords() {
    let raw;
    try { raw = await fs.readFile(this.file, "utf8"); }
    catch (error) { if (error.code === "ENOENT") return []; throw error; }
    const records = [];
    for (const line of raw.split("\n")) {
      if (!line.trim()) continue;
      try { records.push(JSON.parse(line)); } catch { /* ignore only a torn final line */ }
    }
    return records;
  }

  append(event) {
    const run = async () => {
      const body = {
        ...event,
        eventId: crypto.createHash("sha256").update(JSON.stringify(event)).digest("hex"),
        loggedAt: new Date().toISOString()
      };
      const records = await this.readRecords();
      records.push(body);
      let retained = records.slice(-this.maxRecords);
      let serialized = retained.map(row => JSON.stringify(row)).join("\n") + (retained.length ? "\n" : "");
      while (Buffer.byteLength(serialized) > this.maxBytes && retained.length > 1) {
        retained = retained.slice(1);
        serialized = retained.map(row => JSON.stringify(row)).join("\n") + "\n";
      }
      const temp = this.file + ".tmp";
      const handle = await fs.open(temp, "w", 0o600);
      try { await handle.writeFile(serialized, "utf8"); await handle.sync(); }
      finally { await handle.close(); }
      await fs.rename(temp, this.file);
      await this.syncDir();
      return body;
    };
    const pending = this.chain.then(run, run);
    this.chain = pending.then(() => undefined, () => undefined);
    return pending;
  }

  async stats() {
    await this.chain;
    const records = await this.readRecords();
    let bytes = 0;
    try { bytes = (await fs.stat(this.file)).size; } catch {}
    return { file: this.file, records: records.length, bytes, maxRecords: this.maxRecords, maxBytes: this.maxBytes, mode: "paper-only" };
  }
}
