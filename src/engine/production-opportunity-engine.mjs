import {RawSniper} from "../scanner/raw-sniper.mjs";
import {UniswapV3Scanner} from "../scanner/uniswap-v3.mjs";
import {RawLiquidityWorker} from "../worker/raw-liquidity-worker.mjs";

export class ProductionOpportunityEngine{
 constructor({rpcUrl,chainId=1,pool=null,scanNotionalUsd=1000,scannerIntervalMs=30000,sniperIntervalMs=5000}){
  if(!rpcUrl)throw new Error("RPC_URL is required");
  this.pool=pool;
  this.scanner=new UniswapV3Scanner({rpcUrl,chainId,notionalUsd:scanNotionalUsd});
  this.sniper=new RawSniper({rpcUrl,chainId});
  this.worker=new RawLiquidityWorker({sniper:this.sniper,pool,intervalMs:sniperIntervalMs});
  this.scannerIntervalMs=Math.max(10000,Number(scannerIntervalMs)||30000);
  this.scanTimer=null;this.scanning=false;
  this.state={scanner:{status:"stopped",pools:0,opportunities:0,at:null,error:null},rawLiquidity:this.worker.state};
 }
 async scan(){
  if(this.scanning)return this.state.scanner;
  this.scanning=true;
  try{
   const result=await this.scanner.scan();
   if(this.pool)for(const opportunity of result.opportunities){
    await this.pool.query(
     "INSERT INTO opportunities(fingerprint,kind,chain_id,token_in,token_out,buy_pool,sell_pool,gross_edge_bps,estimated_gas_usd,estimated_slippage_usd,estimated_net_profit_usd,observed_at,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(fingerprint) DO NOTHING",
     [opportunity.fingerprint,opportunity.kind,opportunity.chain_id,opportunity.token_in,opportunity.token_out,opportunity.buy_pool,opportunity.sell_pool,opportunity.gross_edge_bps,opportunity.estimated_gas_usd,opportunity.estimated_slippage_usd,opportunity.estimated_net_profit_usd,opportunity.observed_at,opportunity]
    );
   }
   this.state.scanner={status:"ok",pools:result.pools,opportunities:result.opportunities.length,at:new Date().toISOString(),error:null};
   return this.state.scanner;
  }catch(error){
   this.state.scanner={...this.state.scanner,status:"error",at:new Date().toISOString(),error:error instanceof Error?error.message:String(error)};
   throw error;
  }finally{this.scanning=false;}
 }
 async start(){await this.worker.start();await this.scan();this.scanTimer=setInterval(()=>{void this.scan().catch(()=>{})},this.scannerIntervalMs);}
 async stop(){if(this.scanTimer){clearInterval(this.scanTimer);this.scanTimer=null;}await this.worker.stop();}
 status(){return{scanner:this.state.scanner,rawLiquidity:this.worker.state};}
}
