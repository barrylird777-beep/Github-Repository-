import http from "node:http";
import pg from "pg";
import { config } from "./config.mjs";
import { Wal } from "./wal/wal.mjs";
import { PostgresQueue } from "./queue/postgres-queue.mjs";
import { normalizeEvent } from "./normalize/normalize.mjs";

const pool = config.databaseUrl ? new pg.Pool({ connectionString: config.databaseUrl, max: 10, idleTimeoutMillis: 30000 }) : null;
const wal = new Wal(config.walDir);
await wal.init();

if (pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingestion_events(
      id BIGSERIAL PRIMARY KEY,fingerprint TEXT NOT NULL UNIQUE,source TEXT NOT NULL,sequence BIGINT,
      observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),payload JSONB NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','processing','processed','failed')),
      attempts INTEGER NOT NULL DEFAULT 0,lease_token UUID,lease_until TIMESTAMPTZ,last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS ingestion_events_queue_idx ON ingestion_events(status,id);
    CREATE INDEX IF NOT EXISTS ingestion_events_lease_idx ON ingestion_events(status,lease_until);
    CREATE TABLE IF NOT EXISTS opportunities(
      id BIGSERIAL PRIMARY KEY,fingerprint TEXT NOT NULL UNIQUE,kind TEXT NOT NULL,chain_id INTEGER,
      token_in TEXT,token_out TEXT,buy_pool TEXT,sell_pool TEXT,gross_edge_bps NUMERIC NOT NULL,
      estimated_gas_usd NUMERIC,estimated_slippage_usd NUMERIC,estimated_net_profit_usd NUMERIC,
      observed_at TIMESTAMPTZ NOT NULL,payload JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS opportunities_rank_idx ON opportunities(estimated_net_profit_usd DESC NULLS LAST,observed_at DESC)
  `);
}

const queue = pool ? new PostgresQueue(pool) : null;

async function ingest(source, payload, sequence = null) {
  const e = normalizeEvent({ source, payload, sequence });
  await wal.append(e);
  if (queue) await queue.enqueue(e);
  return e;
}

async function work() {
  if (!queue) return;
  await queue.recoverExpired();
  for (const j of await queue.claim(config.workerBatchSize)) {
    try {
      if (j.payload?.opportunity) await queue.saveOpportunity(j.payload.opportunity, j.observed_at);
      await queue.process(j, async () => {});
    } catch (e) {
      await queue.fail(j, e);
      console.error("job", e);
    }
  }
}

const timer = setInterval(() => work().catch(console.error), config.pollMs);

const css = `
:root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#080b12;color:#eef2ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#16213b 0,#080b12 42%);min-height:100vh}
header{padding:28px 5vw 18px;border-bottom:1px solid #20283a;background:#0b0f18cc;backdrop-filter:blur(12px);position:sticky;top:0;z-index:2}
.brand{font-size:26px;font-weight:800;letter-spacing:-.6px}.sub{color:#8d98ad;margin-top:5px}
main{width:min(1180px,90vw);margin:30px auto 70px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
.card{background:#101522;border:1px solid #222c40;border-radius:16px;padding:20px;box-shadow:0 12px 35px #0005}.label{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#7e8aa1}.value{font-size:25px;font-weight:750;margin-top:9px}
.ok{color:#6ee7a1}.warn{color:#ffd166}.muted{color:#8d98ad}.section{margin-top:22px}.section h2{font-size:18px;margin:0 0 12px}
.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}button{border:1px solid #35415b;background:#171f30;color:#eef2ff;border-radius:10px;padding:10px 14px;font-weight:650;cursor:pointer}button:hover{background:#202b41}
.table{overflow:auto;border:1px solid #222c40;border-radius:14px;background:#0e131e}table{width:100%;border-collapse:collapse;min-width:760px}th,td{text-align:left;padding:14px;border-bottom:1px solid #20283a}th{font-size:11px;color:#7e8aa1;text-transform:uppercase;letter-spacing:.8px}td{font-size:14px}.empty{padding:45px;text-align:center;color:#7e8aa1}.pill{display:inline-block;padding:5px 9px;border-radius:999px;background:#182a25;color:#79e2a7;font-size:12px;font-weight:700}
@media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}main{width:92vw}}@media(max-width:480px){.grid{grid-template-columns:1fr 1fr}.card{padding:15px}.value{font-size:20px}}
`;

function dashboardPage() {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Opportunity Engine</title><style>${css}</style></head>
<body><header><div class="brand">Opportunity Engine</div><div class="sub">Live opportunity intelligence &amp; feed monitor</div></header>
<main>
<div class="grid">
<div class="card"><div class="label">Engine</div><div id="engine" class="value">Checking…</div></div>
<div class="card"><div class="label">PostgreSQL</div><div id="db" class="value">Checking…</div></div>
<div class="card"><div class="label">Queue</div><div id="queue" class="value">—</div></div>
<div class="card"><div class="label">Opportunities</div><div id="count" class="value">—</div></div>
</div>
<section class="section"><h2>Opportunity Feed</h2><div class="toolbar"><button onclick="load()">Refresh</button><span id="updated" class="muted"></span></div>
<div class="table"><table><thead><tr><th>Type</th><th>Chain</th><th>Route</th><th>Gross Edge</th><th>Gas</th><th>Slippage</th><th>Net Profit</th><th>Observed</th></tr></thead><tbody id="rows"><tr><td colspan="8" class="empty">Loading opportunity feed…</td></tr></tbody></table></div></section>
<section class="section"><div class="card"><div class="label">System</div><p id="system" class="muted">Connecting to engine…</p></div></section>
</main>
<script>
const money=v=>v==null?"—":"$"+Number(v).toFixed(2);
async function load(){
 try{
  const h=await fetch("/health",{cache:"no-store"}).then(r=>r.json());
  engine.textContent=h.status==="ok"?"ONLINE":"OFFLINE";engine.className="value "+(h.status==="ok"?"ok":"warn");
  db.textContent=h.postgres?"CONNECTED":"NOT CONFIGURED";db.className="value "+(h.postgres?"ok":"warn");
  queue.textContent=h.queue?(h.queue.queued??h.queue.pending??"ACTIVE"):"—";
  const o=await fetch("/api/opportunities",{cache:"no-store"}).then(r=>r.json());
  const items=o.items||[];count.textContent=items.length;updated.textContent="Updated "+new Date().toLocaleTimeString();
  rows.innerHTML=items.length?items.map(x=>"<tr><td><span class=\"pill\">"+(x.kind||"opportunity")+"</span></td><td>"+(x.chain_id??"—")+"</td><td>"+(x.token_in||"—")+" → "+(x.token_out||"—")+"</td><td>"+(x.gross_edge_bps??"—")+" bps</td><td>"+money(x.estimated_gas_usd)+"</td><td>"+money(x.estimated_slippage_usd)+"</td><td>"+money(x.estimated_net_profit_usd)+"</td><td>"+(x.observed_at?new Date(x.observed_at).toLocaleString():"—")+"</td></tr>").join(""):"<tr><td colspan=\"8\" class=\"empty\">"+(h.postgres?"No opportunities detected yet.":"Connect PostgreSQL to enable the live opportunity feed.")+"</td></tr>";
  system.textContent=h.postgres?"Engine, queue, WAL and PostgreSQL are available.":"Engine is online, but PostgreSQL is not configured yet.";
 }catch(e){engine.textContent="ERROR";engine.className="value warn";system.textContent=e.message;rows.innerHTML='<tr><td colspan="8" class="empty">Unable to load engine data.</td></tr>'}
} load();setInterval(load,15000);
</script></body></html>`;
}

function json(res,status,data){res.writeHead(status,{"content-type":"application/json; charset=utf-8","cache-control":"no-store","access-control-allow-origin":"*"});res.end(JSON.stringify(data))}
async function read(req){let s="";for await(const c of req){s+=c;if(s.length>1048576)throw Error("request too large")}return s?JSON.parse(s):{}}

const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,"http://localhost");
  if(req.method==="GET"&&(u.pathname==="/"||u.pathname==="/dashboard")){res.writeHead(200,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});return res.end(dashboardPage())}
  if(req.method==="GET"&&u.pathname==="/health")return json(res,200,{status:"ok",service:"opportunity-feed-intelligence",standalone:true,postgres:Boolean(pool),rpc:Boolean(config.rpcUrl),queue:queue?await queue.stats():null})
  if(req.method==="POST"&&u.pathname==="/api/ingest"){const b=await read(req);if(!b.source)return json(res,400,{error:"source required"});return json(res,202,{ok:true,event:await ingest(b.source,b.payload,b.sequence??null)})}
  if(req.method==="GET"&&u.pathname==="/api/opportunities"){if(!pool)return json(res,503,{error:"PostgreSQL is not configured"});const r=await pool.query("SELECT id,kind,chain_id,token_in,token_out,buy_pool,sell_pool,gross_edge_bps,estimated_gas_usd,estimated_slippage_usd,estimated_net_profit_usd,observed_at FROM opportunities ORDER BY estimated_net_profit_usd DESC NULLS LAST,observed_at DESC LIMIT 100");return json(res,200,{items:r.rows})}
  return json(res,404,{error:"not found"})
 }catch(e){return json(res,500,{error:String(e?.message||e)})}
});
server.listen(config.port,"0.0.0.0",()=>console.log("opportunity engine listening on "+config.port));
async function shutdown(){clearInterval(timer);server.close();if(queue)await queue.shutdown();if(pool)await pool.end();process.exit(0)}
process.on("SIGTERM",shutdown);process.on("SIGINT",shutdown);
