import http from "node:http";
import {config} from "./config.mjs";
import {Wal} from "./wal/wal.mjs";
import {SovereignApiStore} from "./queue/sovereign-api-store.mjs";
import crypto from "node:crypto";
import {normalizeEvent} from "./normalize/normalize.mjs";
import {UniswapV3Scanner} from "./scanner/uniswap-v3.mjs";
import {RawSniper} from "./scanner/raw-sniper.mjs";
import {PrivateMesh} from "./network/private-mesh.mjs";
import {BountyScanner} from "./bounty/bounty-scanner.mjs";
import {handleInstantExtract} from "./api/instant-extract-route.mjs";
import {AiEvalHelper} from "./eval/ai-eval-helper.mjs";

const wal=new Wal(config.walDir);await wal.init();
const store=new SovereignApiStore({baseUrl:config.apexApiUrl,token:config.opportunityToken});
const pendingWrites=new Map();
await wal.replay(async event=>{
 if(event?.type==="pending-opportunity"&&typeof event.id==="string"&&event.opportunity&&typeof event.opportunity==="object"){
  pendingWrites.set(event.id,{id:event.id,opportunity:event.opportunity});
 }else if(event?.type==="ack-opportunity"&&typeof event.id==="string"){
  pendingWrites.delete(event.id);
 }
});
if(String(process.env.MIGRATE_POSTGRES_TO_SOVEREIGN||"").toLowerCase()==="true"){
 const {migratePostgresOpportunitiesToKashKorner}=await import("../scripts/migrate-postgres-to-sovereign.mjs");
 const migrated=await migratePostgresOpportunitiesToKashKorner({databaseUrl:config.legacyMigrationDatabaseUrl,api:store});
 console.log("legacy opportunities copied into KashKorner Pure-WAL",JSON.stringify(migrated));
}
const scanner=new UniswapV3Scanner({rpcUrl:config.rpcUrl,chainId:config.chainId,notionalUsd:config.scanNotionalUsd});
const sniper=new RawSniper({rpcUrl:config.rpcUrl,chainId:config.chainId});
const bountyScanner=new BountyScanner({token:process.env.GITHUB_TOKEN||""});
const evalHelper=new AiEvalHelper();
const mesh=process.env.MESH_SECRET?new PrivateMesh({secret:process.env.MESH_SECRET,maxLanes:8,peers:(()=>{try{return process.env.MESH_PEERS?JSON.parse(process.env.MESH_PEERS):[]}catch{return[]}})()}):null;
let sniperState={status:"starting",events:0,lastBlock:0,error:null};
let lastScan={status:"not_run",pools:0,opportunities:0,at:null,error:null};
let stopping=false;

async function persistOpportunity(opportunity){
 const id=String(opportunity.fingerprint||"");
 if(!id)throw new Error("Opportunity fingerprint is required for sovereign persistence");
 try{
  await store.saveOpportunity(opportunity);
  if(pendingWrites.has(id)){
   await wal.append({type:"ack-opportunity",id,acknowledgedAt:new Date().toISOString()});
   pendingWrites.delete(id);
  }
  return {durable:true};
 }catch(error){
  const item={id,opportunity};
  await wal.append({type:"pending-opportunity",...item,failedAt:new Date().toISOString()});
  pendingWrites.set(id,item);
  return {durable:false,error:String(error?.message||error).slice(0,300)};
 }
}
async function flushPendingWrites(){
 if(!pendingWrites.size||!store.configured)return;
 const batch=[...pendingWrites.values()].slice(0,20);
 for(const item of batch){
  try{
   await store.saveOpportunity(item.opportunity);
   await wal.append({type:"ack-opportunity",id:item.id,acknowledgedAt:new Date().toISOString()});
   pendingWrites.delete(item.id);
  }catch(error){
   console.warn("KashKorner opportunity outbox sync deferred",JSON.stringify({pending:pendingWrites.size,error:String(error?.message||error).slice(0,240)}));
   break;
  }
 }
}
function liveOpportunityId(opportunity){
 const key=[opportunity.kind,opportunity.chain_id,opportunity.token_in,opportunity.token_out,opportunity.buy_pool,opportunity.sell_pool,opportunity.buy_fee,opportunity.sell_fee].map(value=>String(value??"")).join("|");
 return "live-"+crypto.createHash("sha256").update(key).digest("hex");
}
async function ingest(source,payload,sequence=null){
 const e=normalizeEvent({source,payload,sequence});
 await wal.append({type:"ingestion-event",event:e});
 return {...e,durable:false,storage:"local-wal-telemetry"};
}
async function sniff(){
 if(stopping)return;
 try{
  await sniper.start();
  const events=await sniper.poll();
  for(const event of events)await wal.append({type:"raw-liquidity-event",event});
  sniperState={status:"ok",events:sniperState.events+events.length,lastBlock:sniper.lastBlock,storage:"local-wal-telemetry",error:null};
 }catch(e){sniperState={...sniperState,status:"error",error:String(e?.message||e)}}
}
async function scan(){
 if(stopping)return;
 try{
  const result=await scanner.scan();
  const now=new Date().toISOString();
  let durableWrites=0,writeFailures=0;
  for(const opportunity of result.opportunities){
   const fingerprint=liveOpportunityId(opportunity);
   const record={...opportunity,fingerprint,sourceFingerprint:opportunity.fingerprint,source:"live",observed_at:now,observedAt:now};
   const saved=await persistOpportunity(record);
   if(saved.durable)durableWrites++;else writeFailures++;
  }
  lastScan={status:writeFailures?"degraded":"ok",pools:result.pools,opportunities:result.opportunities.length,durableWrites,pendingLocalWrites:pendingWrites.size,rpcErrors:result.rpcErrors||[],diagnostics:result.diagnostics||null,at:now,error:writeFailures?writeFailures+" opportunity writes buffered locally":null};
  await wal.append({type:"scanner-state",value:lastScan});
  console.log("scan complete",JSON.stringify(lastScan));
 }catch(e){lastScan={...lastScan,status:"error",at:new Date().toISOString(),error:String(e?.message||e)};console.error("scanner",e)}
}

const css=`:root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#080b12;color:#eef2ff}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#16213b 0,#080b12 42%);min-height:100vh}header{padding:28px 5vw 18px;border-bottom:1px solid #20283a;background:#0b0f18dd;backdrop-filter:blur(12px);position:sticky;top:0;z-index:2}.brand{font-size:26px;font-weight:800}.sub{color:#8d98ad;margin-top:5px}main{width:min(1180px,92vw);margin:30px auto 70px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.card{background:#101522;border:1px solid #222c40;border-radius:16px;padding:20px}.label{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#7e8aa1}.value{font-size:25px;font-weight:750;margin-top:9px}.ok{color:#6ee7a1}.warn{color:#ffd166}.muted{color:#8d98ad}.section{margin-top:22px}.section h2{font-size:18px}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}button{border:1px solid #35415b;background:#171f30;color:#eef2ff;border-radius:10px;padding:10px 14px;font-weight:650}.table{overflow:auto;border:1px solid #222c40;border-radius:14px;background:#0e131e}table{width:100%;border-collapse:collapse;min-width:820px}th,td{text-align:left;padding:13px;border-bottom:1px solid #20283a}th{font-size:11px;color:#7e8aa1;text-transform:uppercase}td{font-size:14px}.empty{padding:45px;text-align:center;color:#7e8aa1}.pill{display:inline-block;padding:5px 9px;border-radius:999px;background:#182a25;color:#79e2a7;font-size:12px;font-weight:700}@media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}}`;
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
function dashboard(){return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>KoinKob Opportunity Intelligence</title><style>${css}</style></head><body><header><div class="brand">KoinKob Opportunity Intelligence</div><div class="sub">Live Ethereum opportunity intelligence</div></header><main><div class="grid"><div class="card"><div class="label">Engine</div><div id="engine" class="value">Checking…</div></div><div class="card"><div class="label">Sovereign Store</div><div id="db" class="value">Checking…</div></div><div class="card"><div class="label">RPC</div><div id="rpc" class="value">Checking…</div></div><div class="card"><div class="label">Opportunities</div><div id="count" class="value">—</div></div></div><section class="section"><h2>Evaluation Workbench</h2><div class="card"><div class="toolbar"><select id="evalType"><option value="edgeCases">Edge Cases</option><option value="codeReview">Code Review</option><option value="testPlan">Test Plan</option><option value="reasoning">Technical Reasoning</option><option value="formatter">Format Response</option></select><button onclick="runEval()">Generate</button><button onclick="copyEval()">Copy</button></div><textarea id="evalInput" style="width:100%;min-height:180px;background:#090d16;color:#eef2ff;border:1px solid #2a3448;border-radius:10px;padding:14px" placeholder="Paste a task, code sample, or draft response"></textarea><textarea id="evalOutput" readonly style="width:100%;min-height:220px;margin-top:12px;background:#090d16;color:#eef2ff;border:1px solid #2a3448;border-radius:10px;padding:14px" placeholder="Generated result"></textarea><div id="evalMode" class="muted"></div></div></section><section class="section"><h2>Opportunity Feed</h2><div class="toolbar"><button onclick="load()">Refresh</button><span id="updated" class="muted"></span></div><div class="table"><table><thead><tr><th>Type</th><th>Route</th><th>Gross Edge</th><th>Gas</th><th>Slippage</th><th>Net Profit</th><th>Observed</th></tr></thead><tbody id="rows"><tr><td colspan="7" class="empty">Loading…</td></tr></tbody></table></div></section><section class="section"><div class="card"><div class="label">Scanner</div><p id="scanner" class="muted">Connecting…</p></div></section></main><script>
const money=v=>v==null?"—":"$"+Number(v).toFixed(2);
async function runEval(){const input=document.getElementById("evalInput").value;if(!input.trim()){document.getElementById("evalOutput").value="Input is required";return}const out=document.getElementById("evalOutput");out.value="Generating…";try{const r=await fetch("/api/eval/generate",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({type:document.getElementById("evalType").value,input})});const d=await r.json();if(!r.ok)throw new Error(d.error||"generation failed");out.value=d.result||"";document.getElementById("evalMode").textContent="Mode: "+d.mode+(d.mode==="template"?" (configure AI_API_KEY for model generation)":"")}catch(e){out.value=e.message;document.getElementById("evalMode").textContent=""}}
async function copyEval(){const v=document.getElementById("evalOutput").value;if(!v)return;await navigator.clipboard.writeText(v);document.getElementById("evalMode").textContent="Copied to clipboard."}
async function load(){try{const h=await fetch("/health",{cache:"no-store"}).then(r=>r.json());engine.textContent=h.status==="ok"?"ONLINE":"ERROR";engine.className="value "+(h.status==="ok"?"ok":"warn");db.textContent=h.durableStore?"CONNECTED":"NOT CONNECTED";db.className="value "+(h.durableStore?"ok":"warn");rpc.textContent=h.rpc?"CONNECTED":"NOT CONFIGURED";rpc.className="value "+(h.rpc?"ok":"warn");const o=await fetch("/api/opportunities",{cache:"no-store"}).then(r=>r.json());const items=o.items||[];count.textContent=items.length;updated.textContent="Updated "+new Date().toLocaleTimeString();rows.innerHTML=items.length?items.map(x=>"<tr><td><span class=pill>"+String(x.kind||"opportunity").replace(/[<>]/g,"")+"</span></td><td>"+String(x.token_in||"—").slice(0,10)+"… → "+String(x.token_out||"—").slice(0,10)+"…</td><td>"+Number(x.gross_edge_bps||0).toFixed(1)+" bps</td><td>"+money(x.estimated_gas_usd)+"</td><td>"+money(x.estimated_slippage_usd)+"</td><td>"+money(x.estimated_net_profit_usd)+"</td><td>"+(x.observed_at?new Date(x.observed_at).toLocaleString():"—")+"</td></tr>").join(""):"<tr><td colspan=7 class=empty>No opportunities currently detected.</td></tr>";scanner.textContent="Last scan: "+(h.scanner?.status||"unknown")+" · pools "+(h.scanner?.pools??0)+" · opportunities "+(h.scanner?.opportunities??0)+(h.scanner?.error?" · "+h.scanner.error:"")}catch(e){engine.textContent="ERROR";engine.className="value warn";scanner.textContent=e.message}}load();setInterval(load,15000);</script></body></html>`}
function json(res,status,data){res.writeHead(status,{"content-type":"application/json; charset=utf-8","cache-control":"no-store","access-control-allow-origin":"*"});res.end(JSON.stringify(data))}
async function read(req){let s="";for await(const c of req){s+=c;if(s.length>1048576)throw new Error("request too large")}return s?JSON.parse(s):{}}
const server=http.createServer(async(req,res)=>{try{const u=new URL(req.url,"http://localhost");if(req.method==="GET"&&(u.pathname==="/"||u.pathname==="/dashboard")){res.writeHead(200,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});return res.end(dashboard())}if(req.method==="GET"&&u.pathname==="/health"){const storeHealth=await store.health();return json(res,200,{status:"ok",service:"kash-korner-opportunity-intelligence",durableStore:storeHealth.ok,storeConfigured:storeHealth.configured,storeError:storeHealth.error||null,rpc:Boolean(config.rpcUrl),chainId:config.chainId,outbox:{backend:"local-wal-retry-buffer",pendingLocalWrites:pendingWrites.size},scanner:lastScan,rawSniper:sniperState})}if(u.pathname==="/api/extract")return handleInstantExtract(req,res);if(req.method==="GET"&&u.pathname==="/api/network"){return json(res,200,{configured:Boolean(mesh),status:mesh?mesh.status():{status:"unconfigured",reason:"MESH_SECRET not set"}})}if(req.method==="GET"&&u.pathname==="/api/bounties"){const items=await bountyScanner.scan();return json(res,200,{items})}if(req.method==="GET"&&u.pathname==="/api/eval/templates"){return json(res,200,{templates:evalHelper.templates(),aiConfigured:Boolean(process.env.AI_API_KEY||process.env.OPENAI_API_KEY)})}if(req.method==="POST"&&u.pathname==="/api/eval/generate"){const b=await read(req);const result=await evalHelper.generate({type:b.type,input:b.input,useModel:b.useModel!==false});return json(res,200,result)}if(req.method==="GET"&&u.pathname==="/api/opportunities"){const items=await store.listOpportunities(100);return json(res,200,{items})}if(req.method==="POST"&&u.pathname==="/api/ingest"){const b=await read(req);if(!b.source)return json(res,400,{error:"source required"});return json(res,202,{ok:true,event:await ingest(b.source,b.payload,b.sequence??null)})}if(req.method==="POST"&&u.pathname==="/api/scan"){await sniff();await scan();await flushPendingWrites();if(mesh)await mesh.refresh();return json(res,200,lastScan)}return json(res,404,{error:"not found"})}catch(e){return json(res,500,{error:String(e?.message||e)})}});
server.listen(config.port,"0.0.0.0",()=>console.log("opportunity engine listening on "+config.port));
let scanBusy=false;
const syncTimer=setInterval(()=>{flushPendingWrites().catch(console.error)},config.pollMs);
const sniperTimer=setInterval(()=>{sniff().catch(console.error)},config.sniperMs);
const scanTimer=setInterval(()=>{if(!scanBusy){scanBusy=true;scan().catch(console.error).finally(()=>scanBusy=false)}},config.scannerMs);
const meshTimer=mesh?setInterval(()=>{mesh.refresh().catch(()=>{})},15000):null;
await sniff();await scan();
async function shutdown(){if(stopping)return;stopping=true;clearInterval(syncTimer);clearInterval(sniperTimer);clearInterval(scanTimer);if(meshTimer)clearInterval(meshTimer);await flushPendingWrites();server.close();process.exit(0)}
process.on("SIGTERM",shutdown);process.on("SIGINT",shutdown);
