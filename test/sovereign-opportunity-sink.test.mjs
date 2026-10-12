import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import http from "node:http";
import { Wal } from "../src/wal/wal.mjs";
import { SovereignOpportunitySink } from "../src/queue/sovereign-opportunity-sink.mjs";

async function fixture(t, { failPosts = false } = {}) {
  const dir = await mkdtemp(path.join(tmpdir(), "apex-opportunity-wal-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const records = new Map();
  const state = { failPosts, posts: 0 };
  const server = http.createServer(async (req, res) => {
    if (req.method === "GET" && req.url.startsWith("/api/kash-korner/opportunities")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ items: [...records.values()] }));
      return;
    }
    if (req.method === "POST" && req.url === "/api/kash-korner/opportunities") {
      state.posts++;
      if (req.headers["x-apex-opportunity-token"] !== "test-token") {
        res.writeHead(401); res.end(); return;
      }
      if (state.failPosts) { res.writeHead(503); res.end(); return; }
      let body = "";
      for await (const chunk of req) body += chunk;
      const opportunity = JSON.parse(body);
      records.set(opportunity.fingerprint, opportunity);
      res.writeHead(202, { "content-type": "application/json" });
      res.end(JSON.stringify({ success: true }));
      return;
    }
    res.writeHead(404); res.end();
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const apiUrl = `http://127.0.0.1:${server.address().port}`;
  const makeSink = () => new SovereignOpportunitySink({
    wal: new Wal(dir),
    apiUrl,
    token: "test-token"
  });
  return { dir, records, state, makeSink };
}

test("opportunities sync to the sovereign API and survive sink restart", async t => {
  const f = await fixture(t);
  const sink = f.makeSink();
  await sink.init();
  const opportunity = { fingerprint: "pool-a-pool-b", estimated_net_profit_usd: 4.25, observed_at: "2026-10-12T00:00:00.000Z" };
  const saved = await sink.saveOpportunity(opportunity);
  assert.equal(saved.queued, false);
  assert.equal(f.records.size, 1);
  assert.equal(sink.status().storage, "sovereign-pure-wal");
  assert.equal((await sink.listOpportunities()).length, 1);
  const restarted = f.makeSink();
  await restarted.init();
  const duplicate = await restarted.saveOpportunity(opportunity);
  assert.equal(duplicate.duplicate, true);
  assert.equal(f.state.posts, 1);
});

test("failed remote writes stay queued and retry from WAL", async t => {
  const f = await fixture(t, { failPosts: true });
  const sink = f.makeSink();
  await sink.init();
  const result = await sink.saveOpportunity({ fingerprint: "pool-c-pool-d", estimated_net_profit_usd: 2 });
  assert.equal(result.queued, true);
  assert.equal(sink.stats().queued, 1);
  assert.match(sink.status().lastError, /503/);
  f.state.failPosts = false;
  const flushed = await sink.flushPending();
  assert.equal(flushed.pending, 0);
  assert.equal(f.records.size, 1);
  assert.equal(sink.stats().processed, 1);
});

test("unconfigured sink never claims remote durability", async t => {
  const dir = await mkdtemp(path.join(tmpdir(), "apex-opportunity-unconfigured-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const sink = new SovereignOpportunitySink({ wal: new Wal(dir), apiUrl: "", token: "" });
  await sink.init();
  const result = await sink.saveOpportunity({ fingerprint: "pool-e-pool-f" });
  assert.equal(result.queued, true);
  assert.equal(sink.status().configured, false);
  assert.equal(sink.status().storage, "local-wal-unconfigured");
});
