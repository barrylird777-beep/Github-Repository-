import {RawSniper} from "../scanner/raw-sniper.mjs";
import {UniswapV3Scanner} from "../scanner/uniswap-v3.mjs";
import {RawLiquidityWorker} from "../worker/raw-liquidity-worker.mjs";
import {Wal} from "../wal/wal.mjs";

export class ProductionOpportunityEngine{
 constructor({rpcUrl,chainId=1,wal=null,walDir=process.env.WAL_DIR||"./data",scanNotionalUsd=1000,scannerIntervalMs=30000,sniperIntervalMs=5000,scanner=null,sniper=null}={}){
  if(!scanner&&!rpcUrl)throw new Error("RPC_URL is required");
  if(!sniper&&!rpcUrl)throw new Error("RPC_URL is required");
  this.wal=wal||new Wal(walDir);
  this.scanner=scanner||new UniswapV3Scanner({rpcUrl,chainId,notionalUsd:scanNotionalUsd});
  this.sniper=sniper||new RawSniper({rpcUrl,chainId});
  this.opportunities=new Map();
  this.initialized=false;
  this.worker=new RawLiquidityWorker({sniper:this.sniper,wal:this.wal,intervalMs:sniperIntervalMs});
  this.scannerIntervalMs=Math.max(10000,Number(scannerIntervalMs)||30000);
  this.scanTimer=null;this.scanning=false;
  this.state={scanner:{status:"stopped",pools:0,opportunities:0,at:null,error:null},rawLiquidity:this.worker.state};
 }
 async initialize(){
  if(this.initialized)return;
  await this.wal.init();
  await this.wal.replay(async event=>{
   if(event?.type==="raw-sniper-cursor")this.sniper.lastBlock=Math.max(Number(this.sniper.lastBlock)||0,Number(event.lastBlock)||0);
   if(event?.type==="opportunity"&&event.opportunity?.fingerprint)this.opportunities.set(String(event.opportunity.fingerprint),event.opportunity);
  });
  this.initialized=true;
 }
 async scan(){
  if(this.scanning)return this.state.scanner;
  this.scanning=true;
  try{
   await this.initialize();
   const result=await this.scanner.scan();
   for(const opportunity of result.opportunities||[]){
    const fp=String(opportunity?.fingerprint||"").trim();
    if(!fp)continue;
    const previous=this.opportunities.get(fp),now=Date.now();
    const changed=!previous||Math.abs(Number(previous.gross_edge_bps||0)-Number(opportunity.gross_edge_bps||0))>=0.1||Math.abs(Number(previous.estimated_net_profit_usd||0)-Number(opportunity.estimated_net_profit_usd||0))>=0.01||previous.buy_pool!==opportunity.buy_pool||previous.sell_pool!==opportunity.sell_pool;
    if(changed)await this.wal.append({type:"opportunity",opportunity,savedAt:new Date(now).toISOString()});
    this.opportunities.set(fp,opportunity);
   }
   this.state.scanner={status:"ok",pools:result.pools,opportunities:(result.opportunities||[]).length,at:new Date().toISOString(),error:null};
   return this.state.scanner;
  }catch(error){
   this.state.scanner={...this.state.scanner,status:"error",at:new Date().toISOString(),error:error instanceof Error?error.message:String(error)};
   throw error;
  }finally{this.scanning=false;}
 }
 async start(){
  await this.initialize();
  await this.worker.start();
  await this.scan();
  this.scanTimer=setInterval(()=>{void this.scan().catch(()=>{})},this.scannerIntervalMs);
 }
 async stop(){
  if(this.scanTimer){clearInterval(this.scanTimer);this.scanTimer=null;}
  await this.worker.stop();
 }
 status(){return{scanner:this.state.scanner,rawLiquidity:this.worker.state,storage:"pure-wal",opportunityCount:this.opportunities.size};}
}
