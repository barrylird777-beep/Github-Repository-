export class RawLiquidityWorker{
 constructor({sniper,pool=null,intervalMs=5000,onEvent=null}){
  if(!sniper)throw new TypeError("sniper is required");
  this.sniper=sniper;this.pool=pool;this.intervalMs=Math.max(1000,Number(intervalMs)||5000);this.onEvent=onEvent;
  this.timer=null;this.running=false;this.stopping=false;
  this.state={status:"stopped",events:0,lastBlock:0,lastRunAt:null,error:null};
 }
 async persist(event){
  if(!this.pool)return;
  await this.pool.query(
   "INSERT INTO raw_liquidity_events(fingerprint,block_number,transaction_hash,token0,token1,fee,tick_spacing,pool,observed_at,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(fingerprint) DO NOTHING",
   [event.fingerprint,event.blockNumber,event.transactionHash,event.token0,event.token1,event.fee,event.tickSpacing,event.pool,event.observedAt,event]
  );
 }
 async runOnce(){
  if(this.running||this.stopping)return;
  this.running=true;
  try{
   const events=await this.sniper.poll();
   for(const event of events){await this.persist(event);if(this.onEvent)await this.onEvent(event);}
   this.state={status:"ok",events:this.state.events+events.length,lastBlock:this.sniper.lastBlock,lastRunAt:new Date().toISOString(),error:null};
  }catch(error){
   this.state={...this.state,status:"error",lastRunAt:new Date().toISOString(),error:error instanceof Error?error.message:String(error)};
  }finally{this.running=false;}
 }
 async start(){
  if(this.timer)return;
  this.stopping=false;
  await this.sniper.start();
  await this.runOnce();
  this.timer=setInterval(()=>{void this.runOnce()},this.intervalMs);
  this.state={...this.state,status:"running",lastBlock:this.sniper.lastBlock};
 }
 async stop(){
  this.stopping=true;
  if(this.timer){clearInterval(this.timer);this.timer=null;}
  while(this.running)await new Promise(resolve=>setTimeout(resolve,25));
  this.state={...this.state,status:"stopped"};
 }
}
