import {ethers} from "ethers";
import crypto from "node:crypto";
const FACTORY="0x1F98431c8aD98523631AE4a59f267346ea31F984";
const iface=new ethers.Interface(["event PoolCreated(address indexed token0,address indexed token1,uint24 indexed fee,int24 tickSpacing,address pool)","function getPool(address,address,uint24) view returns(address)"]);
const eventTopic=iface.getEvent("PoolCreated").topicHash;
export class RawSniper{
 constructor({rpcUrl,chainId=1,fromBlock=0}){this.provider=new ethers.JsonRpcProvider(rpcUrl,chainId,{staticNetwork:true});this.lastBlock=fromBlock;this.factory=new ethers.Contract(FACTORY,["function getPool(address,address,uint24) view returns(address)"],this.provider)}
 async start(){if(!this.lastBlock)this.lastBlock=await this.provider.getBlockNumber();return this.lastBlock}
 async poll(){
  const latest=await this.provider.getBlockNumber();if(latest<=this.lastBlock)return[];
  const from=this.lastBlock+1,to=latest;this.lastBlock=latest;
  const logs=await this.provider.getLogs({address:FACTORY,topics:[eventTopic],fromBlock:from,toBlock:to});
  return logs.map(log=>{const e=iface.parseLog(log);return{type:"pool-created",blockNumber:log.blockNumber,transactionHash:log.transactionHash,token0:e.args.token0,token1:e.args.token1,fee:Number(e.args.fee),tickSpacing:Number(e.args.tickSpacing),pool:e.args.pool,fingerprint:crypto.createHash("sha256").update(log.transactionHash+String(log.index)).digest("hex"),observedAt:new Date().toISOString()}})
 }
}