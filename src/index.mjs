import http from "node:http";
import pg from "pg";
import {config} from "./config.mjs";
import {Wal} from "./wal/wal.mjs";
import {PostgresQueue} from "./queue/postgres-queue.mjs";
import {normalizeEvent} from "./normalize/normalize.mjs";
import {UniswapV3Scanner} from "./scanner/uniswap-v3.mjs";
import {RawSniper} from "./scanner/raw-sniper.mjs";
import {extract} from "./extract/instant-extract.mjs";

const pool=/^postgres(?:ql)?:\/\//i.test(config.databaseUrl)
 ? new pg.Pool({connectionString:config.databaseUrl,max:10,idleTimeoutMillis:30000,connectionTimeoutMillis:10000})
 : null;
const wal=new Wal(config.walDir);await wal.init();
if(!pool)console.warn("DATABASE_URL is not configured");
if(pool)await pool.query(`
 CREATE TABLE IF NOT EXISTS ingestion_events(
  id BIGSERIAL PRIMARY KEY,fingerprint TEXT NOT NULL UNIQUE,source TEXT NOT NULL,sequence BIGINT,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','processing','processed','failed')),
  attempts INTEGER NOT NULL DEFAULT 0,lease_token UUID,lease_until TIMESTAMPTZ,last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
 CREATE INDEX IF NOT EXISTS ingestion_events_queue_idx ON ingestion_events(status,id);
 CREATE INDEX IF NOT EXISTS ingestion_events_lease_idx ON ingestion_events(status,lease_until);
 CREATE TABLE IF NOT EXISTS opportunities(
  id BIGSERIAL PRIMARY KEY,fingerprint TEXT NOT NULL UNIQUE,kind TEXT NOT NULL,chain_id INTEGER,
  token_in TEXT,token_out TEXT,buy_pool TEXT,sell_pool TEXT,gross_edge_bps NUMERIC NOT NULL,
  estimated_gas_usd NUMERIC,estimated_slippage_usd NUMERIC,estimated_net_profit_usd NUMERIC,
  observed_at TIMESTAMPTZ NOT NULL,payload JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now());
 CREATE INDEX IF NOT EXISTS opportunities_rank_idx ON opportunities(estimated_net_profit_usd DESC NULLS LAST,observed_at DESC);
 CREATE TABLE IF NOT EXISTS engine_state(key TEXT PRIMARY KEY,value JSONB NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
 CREATE TABLE IF NOT EXISTS raw_liquidity_events(id BIGSERIAL PRIMARY KEY,fingerprint TEXT NOT NULL UNIQUE,block_number BIGINT NOT NULL,transaction_hash TEXT NOT NULL,token0 TEXT NOT NULL,token1 TEXT NOT NULL,fee INTEGER NOT NULL,tick_spacing INTEGER NOT NULL,pool TEXT NOT NULL,observed_at TIMESTAMPTZ NOT NULL,payload JSONB NOT NULL);
`);
const queue=pool?new PostgresQueue(pool,{leaseSeconds:config.leaseSeconds,maxAttempts:config.maxAttempts}):null;
const scanner=new UniswapV3Scanner({rpcUrl:config.rpcUrl,chainId:config.chainId,notionalUsd:config.scanNotionalUsd});
const sniper=new RawSniper({rpcUrl:config.rpcUrl,chainId:config.chainId});
let sniperState={status:"starting",events:0,lastBlock:0,error:null};
let lastScan={status:"not_run",pools:0,opportunities:0,at:null,error:null};
let stopping=false;

async function ingest(source,payload,sequence=null){const e=normalizeEvent({source,payload,sequence});await wal.append(e);if(queue)await queue.enqueue(e);return e}
async function work(){
 if(!queue)return;
 await queue.recoverExpired();
 for(const job of await queue.claim(config.workerBatchSize)){
  try{if(job.payload?.opportunity)await queue.saveOpportunity(job.payload.opportunity,job.observed_at);await queue.process(job,async()=>{})}
  catch(e){await queue.fail(job,e);console.error("job",e)}
 }
}
async function sniff(){if(stopping)return;try{await sniper.start();const events=await sniper.poll();for(const e of events){if(pool)await pool.query("INSERT INTO raw_liquidity_events(fingerprint,block_number,transaction_hash,token0,token1,fee,tick_spacing,pool,observed_at,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(fingerprint) DO NOTHING",[e.fingerprint,e.blockNumber,e.transactionHash,e.token0,e.token1,e.fee,e.tickSpacing,e.pool,e.observedAt,e]);}sniperState={status:"ok",events:sniperState.events+events.length,lastBlock:sniper.lastBlock,error:null};}catch(e){sniperState={...sniperState,status:"error",error:String(e?.message||e)}}}
async function scan(){
 if(stopping)return;
 try{
  const result=await scanner.scan();
  for(const opportunity of result.opportunities)if(queue)await queue.saveOpportunity(opportunity,opportunity.observed_at);
  lastScan={status:"ok",pools:result.pools,opportunities:result.opportunities.length,at:new Date().toISOString(),error:null};console.log("scan complete",JSON.stringify(lastScan));
  if(pool)await pool.query("INSERT INTO engine_state(key,value,updated_at) VALUES('scanner',$1,now()) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=now()",[lastScan]);
 }catch(e){lastScan={...lastScan,status:"error",at:new Date().toISOString(),error:String(e?.message||e)};console.error("scanner",e)}
}

const css=`:root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#080b12;color:#eef2ff}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#16213b 0,#080b12 42%);min-height:100vh}header{padding:28px 5vw 18px;border-bottom:1px solid #20283a;background:#0b0f18dd;backdrop-filter:blur(12px);position:sticky;top:0;z-index:2}.brand{font-size:26px;font-weight:800}.sub{color:#8d98ad;margin-top:5px}main{width:min(1180px,92vw);margin:30px auto 70px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.card{background:#101522;border:1px solid #222c40;border-radius:16px;padding:20px}.label{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#7e8aa1}.value{font-size:25px;font-weight:750;margin-top:9px}.ok{color:#6ee7a1}.warn{color:#ffd166}.muted{color:#8d98ad}.section{margin-top:22px}.section h2{font-size:18px}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}button{border:1px solid #35415b;background:#171f30;color:#eef2ff;border-radius:10px;padding:10px 14px;font-weight:650}.table{overflow:auto;border:1px solid #222c40;border-radius:14px;background:#0e131e}table{width:100%;border-collapse:collapse;min-width:820px}th,td{text-align:left;padding:13px;border-bottom:1px solid #20283a}th{font-size:11px;color:#7e8aa1;text-transform:uppercase}td{font-size:14px}.empty{padding:45px;text-align:center;color:#7e8aa1}.pill{display:inline-block;padding:5px 9px;border-radius:999px;background:#182a25;color:#79e2a7;font-size:12px;font-weight:700}@media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}}`;
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
function dashboard(){return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Opportunity Engine</title><style>${css}</style></head><body><header><div class="brand">Opportunity Engine</div><div class="sub">Live Ethereum opportunity intelligence</div></header><main><div class="grid"><div class="card"><div class="label">Engine</div><div id="engine" class="value">Checking…</div></div><div class="card"><div class="label">PostgreSQL</div><div id="db" class="value">Checking…</div></div><div class="card"><div class="label">RPC</div><div id="rpc" class="value">Checking…</div></div><div class="card"><div class="label">Opportunities</div><div id="count" class="value">—</div></div></div><section class="section"><h2>Opportunity Feed</h2><div class="toolbar"><button onclick="load()">Refresh</button><span id="updated" class="muted"></span></div><div class="table"><table><thead><tr><th>Type</th><th>Route</th><th>Gross Edge</th><th>Gas</th><th>Slippage</th><th>Net Profit</th><th>Observed</th></tr></thead><tbody id="rows"><tr><td colspan="7" class="empty">Loading…</td></tr></tbody></table></div></section><section class="section"><div class="card"><div class="label">Scanner</div><p id="scanner" class="muted">Connecting…</p></div></section></main><script>
const money=v=>v==null?"—":"$"+Number(v).toFixed(2);
async function load(){try{const h=await fetch("/health",{cache:"no-store"}).then(r=>r.json());engine.textContent=h.status==="ok"?"ONLINE":"ERROR";engine.className="value "+(h.status==="ok"?"ok":"warn");db.textContent=h.postgres?"CONNECTED":"NOT CONFIGURED";db.className="value "+(h.postgres?"ok":"warn");rpc.textContent=h.rpc?"CONNECTED":"NOT CONFIGURED";rpc.className="value "+(h.rpc?"ok":"warn");const o=await fetch("/api/opportunities",{cache:"no-store"}).then(r=>r.json());const items=o.items||[];count.textContent=items.length;updated.textContent="Updated "+new Date().toLocaleTimeString();rows.innerHTML=items.length?items.map(x=>"<tr><td><span class=pill>"+String(x.kind||"opportunity").replace(/[<>]/g,"")+"</span></td><td>"+String(x.token_in||"—").slice(0,10)+"… → "+String(x.token_out||"—").slice(0,10)+"…</td><td>"+Number(x.gross_edge_bps||0).toFixed(1)+" bps</td><td>"+money(x.estimated_gas_usd)+"</td><td>"+money(x.estimated_slippage_usd)+"</td><td>"+money(x.estimated_net_profit_usd)+"</td><td>"+(x.observed_at?new Date(x.observed_at).toLocaleString():"—")+"</td></tr>").join(""):"<tr><td colspan=7 class=empty>No opportunities currently detected.</td></tr>";scanner.textContent="Last scan: "+(h.scanner?.status||"unknown")+" · pools "+(h.scanner?.pools??0)+" · opportunities "+(h.scanner?.opportunities??0)+(h.scanner?.error?" · "+h.scanner.error:"")}catch(e){engine.textContent="ERROR";engine.className="value warn";scanner.textContent=e.message}}load();setInterval(load,15000);</script></body></html>`}
function json(res,status,data){res.writeHead(status,{"content-type":"application/json; charset=utf-8","cache-control":"no-store","access-control-allow-origin":"*"});res.end(JSON.stringify(data))}
async function read(req){let s="";for await(const c of req){s+=c;if(s.length>1048576)throw new Error("request too large")}return s?JSON.parse(s):{}}
async function extractRoute(req,res){
  try{
    if(req.method!=="POST")return json(res,405,{ok:false,error:"method not allowed"});
    const b=await read(req);
    if(typeof b.url!=="string"||!b.url.trim())return json(res,400,{ok:false,error:"url required"});
    return json(res,200,{ok:true,result:await extract(b.url)});
  }catch(e){
    return json(res,400,{ok:false,error:String(e?.message||e)});
  }
}
const server=http.createServer(async(req,res)=>{try{const u=new URL(req.url,"http://localhost");if(req.method==="GET"&&(u.pathname==="/"||u.pathname==="/dashboard")){res.writeHead(200,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});return res.end(dashboard())}if(req.method==="GET"&&u.pathname==="/health"){return json(res,200,{status:"ok",service:"opportunity-feed-intelligence",postgres:Boolean(pool),rpc:Boolean(config.rpcUrl),chainId:config.chainId,queue:queue?await queue.stats():null,scanner:lastScan,rawSniper:sniperState})}if(u.pathname==="/api/extract")return extractRoute(req,res);if(req.method==="GET"&&u.pathname==="/api/opportunities"){if(!pool)return json(res,503,{error:"PostgreSQL is not configured"});const r=await pool.query("SELECT id,kind,chain_id,token_in,token_out,buy_pool,sell_pool,gross_edge_bps,estimated_gas_usd,estimated_slippage_usd,estimated_net_profit_usd,observed_at FROM opportunities ORDER BY estimated_net_profit_usd DESC NULLS LAST,observed_at DESC LIMIT 100");return json(res,200,{items:r.rows})}if(req.method==="POST"&&u.pathname==="/api/ingest"){const b=await read(req);if(!b.source)return json(res,400,{error:"source required"});return json(res,202,{ok:true,event:await ingest(b.source,b.payload,b.sequence??null)})}if(req.method==="POST"&&u.pathname==="/api/scan"){await sniff();await scan();return json(res,200,lastScan)}return json(res,404,{error:"not found"})}catch(e){return json(res,500,{error:String(e?.message||e)})}});
server.listen(config.port,"0.0.0.0",()=>console.log("opportunity engine listening on "+config.port));
let workBusy=false,scanBusy=false;
const workTimer=setInterval(()=>{if(!workBusy){workBusy=true;work().catch(console.error).finally(()=>workBusy=false)}},config.pollMs);
const sniperTimer=setInterval(()=>{sniff().catch(console.error)},config.sniperMs);
const scanTimer=setInterval(()=>{if(!scanBusy){scanBusy=true;scan().catch(console.error).finally(()=>scanBusy=false)}},config.scannerMs);
await sniff();await scan();
async function shutdown(){if(stopping)return;stopping=true;clearInterval(workTimer);clearInterval(sniperTimer);clearInterval(scanTimer);server.close();if(queue)await queue.shutdown();if(pool)await pool.end();process.exit(0)}
process.on("SIGTERM",shutdown);process.on("SIGINT",shutdown);
