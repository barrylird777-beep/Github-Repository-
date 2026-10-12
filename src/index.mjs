import http from "node:http";
import {config} from "./config.mjs";
import {Wal} from "./wal/wal.mjs";
import {normalizeEvent} from "./normalize/normalize.mjs";
import {UniswapV3Scanner} from "./scanner/uniswap-v3.mjs";
import {RawSniper} from "./scanner/raw-sniper.mjs";
import {PrivateMesh} from "./network/private-mesh.mjs";
import {BountyScanner} from "./bounty/bounty-scanner.mjs";
import {handleInstantExtract} from "./api/instant-extract-route.mjs";
import {AiEvalHelper} from "./eval/ai-eval-helper.mjs";
import { authorizeScanRequest } from "./api/scan-auth.mjs";

const wal=new Wal(config.walDir);await wal.init();
const opportunities=new Map(),ingestionEvents=new Map(),rawLiquidityEvents=new Map(),persistedAt=new Map(),remoteSyncedAt=new Map();
const apiBase=String(process.env.APEX_SOVEREIGN_API_URL||"").trim().replace(/\/+$/,"");
const apiToken=String(process.env.APEX_OPPORTUNITY_ENGINE_TOKEN||"").trim();
let remoteStoreState={configured:Boolean(apiBase&&apiToken),status:apiBase&&apiToken?"pending":"not-configured",lastSyncAt:null,error:null};
let lastSniperBlock=0;
let sniperState={status:"starting",events:0,lastBlock:0,error:null};
let lastScan={status:"not_run",pools:0,opportunities:0,at:null,error:null};
let stopping=false;
function restoreRecord(r){
 if(r?.type==="opportunity"&&r.opportunity?.fingerprint){opportunities.set(String(r.opportunity.fingerprint),r.opportunity);persistedAt.set(String(r.opportunity.fingerprint),Date.parse(r.savedAt||r.opportunity.observed_at||0)||0);}
 else if(r?.type==="ingestion"&&r.event?.fingerprint)ingestionEvents.set(String(r.event.fingerprint),r.event);
 else if(r?.type==="raw-liquidity"&&r.event?.fingerprint)rawLiquidityEvents.set(String(r.event.fingerprint),r.event);
 else if(r?.type==="raw-sniper-cursor")lastSniperBlock=Math.max(lastSniperBlock,Number(r.lastBlock)||0);
 else if(r?.type==="scanner-state"&&r.state)lastScan=r.state;
 else if(r?.fingerprint&&r?.source)ingestionEvents.set(String(r.fingerprint),r);
}
await wal.replay(restoreRecord);
async function postOpportunityToSovereignApi(o){
 if(!apiBase||!apiToken)return false;
 const response=await fetch(apiBase+"/api/kash-korner/opportunities",{method:"POST",headers:{"content-type":"application/json","x-apex-opportunity-token":apiToken},body:JSON.stringify(o),signal:AbortSignal.timeout(8000)});
 if(!response.ok)throw new Error("Sovereign opportunity API HTTP "+response.status);
 remoteStoreState={configured:true,status:"connected",lastSyncAt:new Date().toISOString(),error:null};
 remoteSyncedAt.set(String(o.fingerprint),Date.now());
 return true;
}
async function saveOpportunity(o){
 const fp=String(o?.fingerprint||"").trim();
 if(!fp)throw new Error("Opportunity fingerprint is required");
 const previous=opportunities.get(fp),now=Date.now();
 const changed=!previous||Math.abs(Number(previous.gross_edge_bps||0)-Number(o.gross_edge_bps||0))>=0.1||Math.abs(Number(previous.estimated_net_profit_usd||0)-Number(o.estimated_net_profit_usd||0))>=0.01||previous.buy_pool!==o.buy_pool||previous.sell_pool!==o.sell_pool;
 if(changed||now-(persistedAt.get(fp)||0)>=300000){await wal.append({type:"opportunity",opportunity:o,savedAt:new Date(now).toISOString()});persistedAt.set(fp,now);}
 opportunities.set(fp,o);
 if(apiBase&&apiToken&&(changed||now-(remoteSyncedAt.get(fp)||0)>=300000)){
  try{await postOpportunityToSovereignApi(o);}
  catch(e){remoteStoreState={configured:true,status:"degraded",lastSyncAt:remoteStoreState.lastSyncAt,error:String(e?.message||e).slice(0,300)};}
 }
}
async function opportunityFeed(){
 if(apiBase&&apiToken){
  try{
   const response=await fetch(apiBase+"/api/kash-korner/opportunities?limit=100",{cache:"no-store",signal:AbortSignal.timeout(5000)});
   if(!response.ok)throw new Error("Sovereign opportunity feed HTTP "+response.status);
   const data=await response.json();
   remoteStoreState={configured:true,status:"connected",lastSyncAt:remoteStoreState.lastSyncAt,error:null};
   return {items:Array.isArray(data.items)?data.items:[],storage:"railway-pure-wal",source:"sovereign-api"};
  }catch(e){remoteStoreState={configured:true,status:"degraded",lastSyncAt:remoteStoreState.lastSyncAt,error:String(e?.message||e).slice(0,300)};}
 }
 return {items:[...opportunities.values()].sort((a,b)=>Number(b.estimated_net_profit_usd||0)-Number(a.estimated_net_profit_usd||0)||String(b.observed_at||"").localeCompare(String(a.observed_at||""))).slice(0,100),storage:"local-pure-wal",source:"local-fallback",warning:remoteStoreState.error||(!remoteStoreState.configured?"Sovereign API sync is not configured":null)};
}
const scanner=new UniswapV3Scanner({rpcUrl:config.rpcUrl,chainId:config.chainId,notionalUsd:config.scanNotionalUsd});
const sniper=new RawSniper({rpcUrl:config.rpcUrl,chainId:config.chainId});
sniper.lastBlock=lastSniperBlock;
const bountyScanner=new BountyScanner({token:process.env.GITHUB_TOKEN||""});
const evalHelper=new AiEvalHelper();
const mesh=process.env.MESH_SECRET?new PrivateMesh({secret:process.env.MESH_SECRET,maxLanes:8,peers:(()=>{try{return process.env.MESH_PEERS?JSON.parse(process.env.MESH_PEERS):[]}catch{return[]}})()}):null;
async function ingest(source,payload,sequence=null){const event=normalizeEvent({source,payload,sequence});await wal.append({type:"ingestion",event});ingestionEvents.set(String(event.fingerprint),event);return event;}
async function sniff(){if(stopping)return;try{await sniper.start();const events=await sniper.poll();for(const event of events){if(!rawLiquidityEvents.has(String(event.fingerprint))){await wal.append({type:"raw-liquidity",event});rawLiquidityEvents.set(String(event.fingerprint),event);}}if(sniper.lastBlock>lastSniperBlock){lastSniperBlock=sniper.lastBlock;await wal.append({type:"raw-sniper-cursor",lastBlock:lastSniperBlock,savedAt:new Date().toISOString()});}sniperState={status:"ok",events:rawLiquidityEvents.size,lastBlock:sniper.lastBlock,error:null};}catch(e){sniperState={...sniperState,status:"error",error:String(e?.message||e)};}}
async function scan(){if(stopping)return;try{const result=await scanner.scan();for(const opportunity of result.opportunities)await saveOpportunity(opportunity);lastScan={status:"ok",pools:result.pools,opportunities:result.opportunities.length,rpcErrors:result.rpcErrors||[],diagnostics:result.diagnostics||null,at:new Date().toISOString(),error:null};await wal.append({type:"scanner-state",state:lastScan});console.log("scan complete",JSON.stringify({...lastScan,storage:"pure-wal",remoteStore:remoteStoreState.status}));}catch(e){lastScan={...lastScan,status:"error",at:new Date().toISOString(),error:String(e?.message||e)};await wal.append({type:"scanner-state",state:lastScan}).catch(()=>{});console.error("scanner",e);}}

const css=`:root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#080b12;color:#eef2ff}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#16213b 0,#080b12 42%);min-height:100vh}header{padding:28px 5vw 18px;border-bottom:1px solid #20283a;background:#0b0f18dd;backdrop-filter:blur(12px);position:sticky;top:0;z-index:2}.brand{font-size:26px;font-weight:800}.sub{color:#8d98ad;margin-top:5px}main{width:min(1180px,92vw);margin:30px auto 70px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.card{background:#101522;border:1px solid #222c40;border-radius:16px;padding:20px}.label{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#7e8aa1}.value{font-size:25px;font-weight:750;margin-top:9px}.ok{color:#6ee7a1}.warn{color:#ffd166}.muted{color:#8d98ad}.section{margin-top:22px}.section h2{font-size:18px}.toolbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}button{border:1px solid #35415b;background:#171f30;color:#eef2ff;border-radius:10px;padding:10px 14px;font-weight:650}.table{overflow:auto;border:1px solid #222c40;border-radius:14px;background:#0e131e}table{width:100%;border-collapse:collapse;min-width:820px}th,td{text-align:left;padding:13px;border-bottom:1px solid #20283a}th{font-size:11px;color:#7e8aa1;text-transform:uppercase}td{font-size:14px}.empty{padding:45px;text-align:center;color:#7e8aa1}.pill{display:inline-block;padding:5px 9px;border-radius:999px;background:#182a25;color:#79e2a7;font-size:12px;font-weight:700}@media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}}`;
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
function dashboard(){return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Opportunity Engine</title><style>${css}</style></head><body><header><div class="brand">Opportunity Engine</div><div class="sub">Live Ethereum opportunity intelligence</div></header><main><div class="grid"><div class="card"><div class="label">Engine</div><div id="engine" class="value">Checking…</div></div><div class="card"><div class="label">Sovereign Storage</div><div id="db" class="value">Checking…</div></div><div class="card"><div class="label">RPC</div><div id="rpc" class="value">Checking…</div></div><div class="card"><div class="label">Opportunities</div><div id="count" class="value">—</div></div></div><section class="section"><h2>Evaluation Workbench</h2><div class="card"><div class="toolbar"><select id="evalType"><option value="edgeCases">Edge Cases</option><option value="codeReview">Code Review</option><option value="testPlan">Test Plan</option><option value="reasoning">Technical Reasoning</option><option value="formatter">Format Response</option></select><button onclick="runEval()">Generate</button><button onclick="copyEval()">Copy</button></div><textarea id="evalInput" style="width:100%;min-height:180px;background:#090d16;color:#eef2ff;border:1px solid #2a3448;border-radius:10px;padding:14px" placeholder="Paste a task, code sample, or draft response"></textarea><textarea id="evalOutput" readonly style="width:100%;min-height:220px;margin-top:12px;background:#090d16;color:#eef2ff;border:1px solid #2a3448;border-radius:10px;padding:14px" placeholder="Generated result"></textarea><div id="evalMode" class="muted"></div></div></section><section class="section"><h2>Opportunity Feed</h2><div class="toolbar"><button onclick="load()">Refresh</button><span id="updated" class="muted"></span></div><div class="table"><table><thead><tr><th>Type</th><th>Route</th><th>Gross Edge</th><th>Gas</th><th>Slippage</th><th>Net Profit</th><th>Observed</th></tr></thead><tbody id="rows"><tr><td colspan="7" class="empty">Loading…</td></tr></tbody></table></div></section><section class="section"><div class="card"><div class="label">Scanner</div><p id="scanner" class="muted">Connecting…</p></div></section></main><script>
const money=v=>v==null?"—":"$"+Number(v).toFixed(2);
async function runEval(){const input=document.getElementById("evalInput").value;if(!input.trim()){document.getElementById("evalOutput").value="Input is required";return}const out=document.getElementById("evalOutput");out.value="Generating…";try{const r=await fetch("/api/eval/generate",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({type:document.getElementById("evalType").value,input})});const d=await r.json();if(!r.ok)throw new Error(d.error||"generation failed");out.value=d.result||"";document.getElementById("evalMode").textContent="Mode: "+d.mode+(d.mode==="template"?" (configure AI_API_KEY for model generation)":"")}catch(e){out.value=e.message;document.getElementById("evalMode").textContent=""}}
async function copyEval(){const v=document.getElementById("evalOutput").value;if(!v)return;await navigator.clipboard.writeText(v);document.getElementById("evalMode").textContent="Copied to clipboard."}
async function load(){const engineEl=document.getElementById("engine"),dbEl=document.getElementById("db"),rpcEl=document.getElementById("rpc"),countEl=document.getElementById("count"),updatedEl=document.getElementById("updated"),rowsEl=document.getElementById("rows"),scannerEl=document.getElementById("scanner");try{const h=await fetch("/health",{cache:"no-store"}).then(r=>r.json());engineEl.textContent=h.status==="ok"?"ONLINE":"ERROR";engineEl.className="value "+(h.status==="ok"?"ok":"warn");dbEl.textContent=h.remoteStore?.status==="connected"?"PURE-WAL CONNECTED":(h.remoteStore?.configured?"SYNC DEGRADED":"LOCAL WAL ONLY");dbEl.className="value "+(h.remoteStore?.status==="connected"?"ok":"warn");rpcEl.textContent=h.rpc?"CONNECTED":"NOT CONFIGURED";rpcEl.className="value "+(h.rpc?"ok":"warn");const o=await fetch("/api/opportunities",{cache:"no-store"}).then(r=>r.json());const items=o.items||[];countEl.textContent=items.length;updatedEl.textContent="Updated "+new Date().toLocaleTimeString();rowsEl.innerHTML=items.length?items.map(x=>"<tr><td><span class=pill>"+String(x.kind||"opportunity").replace(/[<>]/g,"")+"</span></td><td>"+String(x.token_in||"—").slice(0,10)+"… → "+String(x.token_out||"—").slice(0,10)+"…</td><td>"+Number(x.gross_edge_bps||0).toFixed(1)+" bps</td><td>"+money(x.estimated_gas_usd)+"</td><td>"+money(x.estimated_slippage_usd)+"</td><td>"+money(x.estimated_net_profit_usd)+"</td><td>"+(x.observed_at?new Date(x.observed_at).toLocaleString():"—")+"</td></tr>").join(""):"<tr><td colspan=7 class=empty>No opportunities currently detected.</td></tr>";scannerEl.textContent="Last scan: "+(h.scanner?.status||"unknown")+" · pools "+(h.scanner?.pools??0)+" · opportunities "+(h.scanner?.opportunities??0)+(h.scanner?.error?" · "+h.scanner.error:"")}catch(e){engineEl.textContent="ERROR";engineEl.className="value warn";scannerEl.textContent=e.message}}load();setInterval(load,15000);</script></body></html>`}
function json(res,status,data){res.writeHead(status,{"content-type":"application/json; charset=utf-8","cache-control":"no-store","access-control-allow-origin":"*"});res.end(JSON.stringify(data))}
async function read(req){let s="";for await(const c of req){s+=c;if(s.length>1048576)throw new Error("request too large")}return s?JSON.parse(s):{}}
const server=http.createServer(async(req,res)=>{try{const u=new URL(req.url,"http://localhost");if(req.method==="GET"&&(u.pathname==="/"||u.pathname==="/dashboard")){res.writeHead(200,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});return res.end(dashboard())}if(req.method==="GET"&&u.pathname==="/health"){return json(res,200,{status:lastScan.status==="error"?"degraded":"ok",service:"opportunity-feed-intelligence",storage:"pure-wal",remoteStore:remoteStoreState,rpc:Boolean(config.rpcUrl),chainId:config.chainId,opportunityCount:opportunities.size,ingestionEventCount:ingestionEvents.size,rawLiquidityEventCount:rawLiquidityEvents.size,scanner:lastScan,rawSniper:sniperState})}if(u.pathname==="/api/extract")return handleInstantExtract(req,res);if(req.method==="GET"&&u.pathname==="/api/network"){return json(res,200,{configured:Boolean(mesh),status:mesh?mesh.status():{status:"unconfigured",reason:"MESH_SECRET not set"}})}if(req.method==="GET"&&u.pathname==="/api/bounties"){const items=await bountyScanner.scan();return json(res,200,{items})}if(req.method==="GET"&&u.pathname==="/api/eval/templates"){return json(res,200,{templates:evalHelper.templates(),aiConfigured:Boolean(process.env.AI_API_KEY||process.env.OPENAI_API_KEY)})}if(req.method==="POST"&&u.pathname==="/api/eval/generate"){const b=await read(req);const result=await evalHelper.generate({type:b.type,input:b.input,useModel:b.useModel!==false});return json(res,200,result)}if(req.method==="GET"&&u.pathname==="/api/opportunities"){return json(res,200,await opportunityFeed())}if(req.method==="POST"&&u.pathname==="/api/ingest"){const b=await read(req);if(!b.source)return json(res,400,{error:"source required"});return json(res,202,{ok:true,event:await ingest(b.source,b.payload,b.sequence??null)})}if(req.method==="POST"&&u.pathname==="/api/scan"){if(!authorizeScanRequest(req))return json(res,401,{error:"unauthorized"});await sniff();await scan();if(mesh)await mesh.refresh();return json(res,200,lastScan)}return json(res,404,{error:"not found"})}catch(e){return json(res,500,{error:String(e?.message||e)})}});
server.listen(config.port,"0.0.0.0",()=>console.log("opportunity engine listening on "+config.port));
let scanBusy=false;
const sniperTimer=setInterval(()=>{sniff().catch(console.error)},config.sniperMs);
const scanTimer=setInterval(()=>{if(!scanBusy){scanBusy=true;scan().catch(console.error).finally(()=>scanBusy=false)}},config.scannerMs);
const meshTimer=mesh?setInterval(()=>{mesh.refresh().catch(()=>{})},15000):null;
await sniff();await scan();
async function shutdown(){if(stopping)return;stopping=true;clearInterval(sniperTimer);clearInterval(scanTimer);if(meshTimer)clearInterval(meshTimer);server.close();process.exit(0)}
process.on("SIGTERM",shutdown);process.on("SIGINT",shutdown);
