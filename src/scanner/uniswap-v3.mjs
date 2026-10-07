import {ethers} from "ethers";
import crypto from "node:crypto";
import {QuoteEngine} from "./quote-engine.mjs";
const FACTORY="0x1F98431c8aD98523631AE4a59f267346ea31F984";
const TOKENS={
 WETH:{address:"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",decimals:18},
 USDC:{address:"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",decimals:6},
 USDT:{address:"0xdac17f958d2ee523a2206206994597c13d831ec7",decimals:6},
 DAI:{address:"0x6b175474e89094c44da98b954eedeac495271d0f",decimals:18},
 WBTC:{address:"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",decimals:8}
};
const FEES=[100,500,3000,10000];
const factoryAbi=["function getPool(address,address,uint24) view returns(address)"];
const poolAbi=[
 "function token0() view returns(address)","function token1() view returns(address)",
 "function fee() view returns(uint24)","function liquidity() view returns(uint128)",
 "function slot0() view returns(uint160 sqrtPriceX96,int24 tick,uint16 observationIndex,uint16 observationCardinality,uint16 observationCardinalityNext,uint8 feeProtocol,bool unlocked)"
];
const zero="0x0000000000000000000000000000000000000000";
const lower=x=>x.toLowerCase();
const fp=x=>crypto.createHash("sha256").update(JSON.stringify(x)).digest("hex");
function spotFromSqrt(sqrt,t0,t1){const q=Number(sqrt);if(!Number.isFinite(q)||q<=0)return null;return (q*q/2**192)*10**(t0.decimals-t1.decimals)}
export class UniswapV3Scanner{
 constructor({rpcUrl,chainId=1,notionalUsd=1000}){this.provider=new ethers.JsonRpcProvider(rpcUrl,chainId,{staticNetwork:true});this.factory=new ethers.Contract(FACTORY,factoryAbi,this.provider);this.notionalUsd=notionalUsd;this.quoter=new QuoteEngine(this.provider)}
 async discover(){const n=Object.keys(TOKENS),jobs=[];for(let i=0;i<n.length;i++)for(let j=i+1;j<n.length;j++)for(const fee of FEES)jobs.push({a:n[i],b:n[j],fee});const out=[],errors=[];for(let i=0;i<jobs.length;i+=8){const batch=jobs.slice(i,i+8);const rows=await Promise.all(batch.map(async p=>{try{const address=await this.factory.getPool(TOKENS[p.a].address,TOKENS[p.b].address,p.fee);return address&&lower(address)!==zero?{address,poolFee:p.fee,a:p.a,b:p.b}:null}catch(error){errors.push({pair:p.a+"/"+p.b,fee:p.fee,error:String(error?.shortMessage||error?.message||error)});return null}}));for(const r of rows)if(r)out.push({address:r.address,fee:r.poolFee,a:r.a,b:r.b})}return{pools:out,errors}}
 async readPool(p){try{const c=new ethers.Contract(p.address,poolAbi,this.provider);const [token0,token1,fee,liquidity,s0]=await Promise.all([c.token0(),c.token1(),c.fee(),c.liquidity(),c.slot0()]);if(BigInt(liquidity)===0n)return null;const t0=Object.values(TOKENS).find(x=>lower(x.address)===lower(token0)),t1=Object.values(TOKENS).find(x=>lower(x.address)===lower(token1));if(!t0||!t1)return null;const spot=spotFromSqrt(s0[0],t0,t1);if(!spot)return null;return {...p,token0,token1,fee:Number(fee),liquidity:String(liquidity),sqrtPriceX96:String(s0[0]),tick:Number(s0[1]),spot}}catch{return null}}
 async scan(){const discovery=await this.discover();const discovered=discovery.pools;const rows=[];for(let i=0;i<discovered.length;i+=8){const batch=await Promise.all(discovered.slice(i,i+8).map(p=>this.readPool(p)));for(const r of batch)if(r)rows.push(r)}const groups=new Map();for(const r of rows){const k=[r.a,r.b].sort().join(":");if(!groups.has(k))groups.set(k,[]);groups.get(k).push(r)}const opportunities=[],diagnostics={pairGroups:groups.size,pairComparisons:0,edgeCandidates:0,quoteErrors:0};for(const ps of groups.values()){for(const buy of ps)for(const sell of ps){if(buy.address===sell.address)continue;diagnostics.pairComparisons++;const edge=(buy.spot/sell.spot-1)*10000;if(!Number.isFinite(edge)||edge<=1)continue;diagnostics.edgeCandidates++;
        let quote;try{const amountIn=10n**BigInt(TOKENS[buy.a].decimals);const first=await this.quoter.exactInput(TOKENS[buy.a].address,buy.fee,TOKENS[buy.b].address,amountIn);const second=await this.quoter.exactInput(TOKENS[buy.b].address,sell.fee,TOKENS[buy.a].address,first.amountOut);quote={amountIn,amountOut:first.amountOut,roundTripOut:second.amountOut,gasEstimate:first.gasEstimate+second.gasEstimate,roundTripBps:Number(second.amountOut*10000n/amountIn)-10000};}catch{diagnostics.quoteErrors++;continue}
        const gross=Math.max(edge,quote.roundTripBps);if(gross<=0)continue;const liquidityPenalty=Math.min(200,Math.max(2,500000/Math.max(1,Number(buy.liquidity)/1e12)+500000/Math.max(1,Number(sell.liquidity)/1e12)));const gas=8;const net=Math.max(0,gross-liquidityPenalty-20);const payload={kind:"uniswap-v3-cross-pool",chain_id:1,token_in:TOKENS[buy.a].address,token_out:TOKENS[buy.b].address,buy_pool:buy.address,sell_pool:sell.address,gross_edge_bps:gross,estimated_gas_usd:gas,estimated_slippage_usd:this.notionalUsd*liquidityPenalty/10000,estimated_net_profit_usd:this.notionalUsd*net/10000,observed_at:new Date().toISOString(),model:"quoter-v2-round-trip",quote_valid:true,quote_gas_units:quote.gasEstimate.toString(),round_trip_bps:quote.roundTripBps,executable:false,notional_usd:this.notionalUsd,buy_fee:buy.fee,sell_fee:sell.fee};payload.fingerprint=fp(payload);opportunities.push(payload)}}return{pools:rows.length,opportunities:opportunities.sort((a,b)=>b.estimated_net_profit_usd-a.estimated_net_profit_usd).slice(0,100),rpcErrors:discovery.errors.slice(0,12),diagnostics}}}
