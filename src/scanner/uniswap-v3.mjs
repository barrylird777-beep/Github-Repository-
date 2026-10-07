import {ethers} from "ethers";
import crypto from "node:crypto";
const FACTORY="0x1F98431c8aD98523631AE4a59f267346ea31F984";
const TOKENS={
 WETH:{address:"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",decimals:18},
 USDC:{address:"0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",decimals:6},
 USDT:{address:"0xdAC17F958D2ee523a2206206994597C13D831ec7",decimals:6},
 DAI:{address:"0x6B175474E89094C44Da98b954EedeAC495271d0F",decimals:18},
 WBTC:{address:"0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",decimals:8}
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
 constructor({rpcUrl,chainId=1,notionalUsd=1000}){this.provider=new ethers.JsonRpcProvider(rpcUrl,chainId,{staticNetwork:true});this.factory=new ethers.Contract(FACTORY,factoryAbi,this.provider);this.notionalUsd=notionalUsd}
 async discover(){const n=Object.keys(TOKENS),out=[];for(let i=0;i<n.length;i++)for(let j=i+1;j<n.length;j++)for(const fee of FEES){try{const address=await this.factory.getPool(TOKENS[n[i]].address,TOKENS[n[j]].address,fee);if(address&&lower(address)!==zero)out.push({address,fee,a:n[i],b:n[j]})}catch{}}return out}
 async readPool(p){try{const c=new ethers.Contract(p.address,poolAbi,this.provider);const [token0,token1,fee,liquidity,s0]=await Promise.all([c.token0(),c.token1(),c.fee(),c.liquidity(),c.slot0()]);if(BigInt(liquidity)===0n)return null;const t0=Object.values(TOKENS).find(x=>lower(x.address)===lower(token0)),t1=Object.values(TOKENS).find(x=>lower(x.address)===lower(token1));if(!t0||!t1)return null;const spot=spotFromSqrt(s0[0],t0,t1);if(!spot)return null;return {...p,token0,token1,fee:Number(fee),liquidity:String(liquidity),sqrtPriceX96:String(s0[0]),tick:Number(s0[1]),spot}}catch{return null}}
 async scan(){const discovered=await this.discover();const rows=(await Promise.all(discovered.map(p=>this.readPool(p)))).filter(Boolean);const groups=new Map();for(const r of rows){const k=[r.a,r.b].sort().join(":");if(!groups.has(k))groups.set(k,[]);groups.get(k).push(r)}const opportunities=[];for(const ps of groups.values()){for(const buy of ps)for(const sell of ps){if(buy.address===sell.address)continue;const edge=(buy.spot/sell.spot-1)*10000;if(!Number.isFinite(edge)||edge<=5)continue;const liquidityPenalty=Math.min(200,Math.max(2,500000/Math.max(1,Number(buy.liquidity)/1e12)+500000/Math.max(1,Number(sell.liquidity)/1e12)));const gas=8;const net=Math.max(0,edge-liquidityPenalty-20);const payload={kind:"uniswap-v3-cross-pool",chain_id:1,token_in:buy.a===sell.a?TOKENS[buy.b].address:TOKENS[buy.a].address,token_out:buy.a===sell.a?TOKENS[buy.a].address:TOKENS[buy.b].address,buy_pool:buy.address,sell_pool:sell.address,gross_edge_bps:edge,estimated_gas_usd:gas,estimated_slippage_usd:this.notionalUsd*liquidityPenalty/10000,estimated_net_profit_usd:this.notionalUsd*net/10000,observed_at:new Date().toISOString(),model:"spot-price-intelligence",executable:false,notional_usd:this.notionalUsd,buy_fee:buy.fee,sell_fee:sell.fee};payload.fingerprint=fp(payload);opportunities.push(payload)}}return{pools:rows.length,opportunities:opportunities.sort((a,b)=>b.estimated_net_profit_usd-a.estimated_net_profit_usd).slice(0,100)}}}
