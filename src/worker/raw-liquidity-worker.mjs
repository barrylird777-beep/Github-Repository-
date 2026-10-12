export class RawLiquidityWorker{
 constructor({sniper,wal,intervalMs=5000,onEvent=null}={}){
  if(!sniper)throw new TypeError("sniper is required");
  if(!wal||typeof wal.append!=="function")throw new TypeError("Pure-WAL store is required");
  this.sniper=sniper;this.wal=wal;this.intervalMs=Math.max(1000,Number(intervalMs)||5000);this.onEvent=onEvent;
  this.timer=null;this.running=false;this.stopping=false;
  this.state={status:"stopped",events:0,lastBlock:0,lastRunAt:null,error:null};
 }
 async persist(event){
  await this.wal.append({type:"raw-liquidity",event});
 }
 async runOnce(){
  if(this.running||this.stopping)return;
  this.running=true;
  try{
   const events=await this.sniper.poll();
   for(const event of events){
    await this.persist(event);
    if(this.onEvent)await this.onEvent(event);
   }
   await this.wal.append({type:"raw-sniper-cursor",lastBlock:Number(this.sniper.lastBlock)||0});
   this.state={status:"ok",events:this.state.events+events.length,lastBlock:this.sniper.lastBlock,lastRunAt:new Date().toISOString(),error:null};
  }catch(error){
   this.state={...this.state,status:"error",lastRunAt:new Date().toISOString(),error:error instanceof Error?error.message:String(error)};
  }finally{this.running=false;}
 }
 async start(){
  if(this.timer)return;
  this.stopping=false;
  await this.wal.init?.();
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
