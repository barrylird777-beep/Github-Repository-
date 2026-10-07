import {ethers} from "ethers";
const QUOTER_V2="0x61fFE014bA17989E743c5F6cB21bF9697530B21e";
const abi=["function quoteExactInput(bytes path,uint256 amountIn) returns(uint256 amountOut,uint160[] sqrtPriceX96AfterList,uint32[] initializedTicksCrossedList,uint256 gasEstimate)"];
export class QuoteEngine{
 constructor(provider){this.provider=provider;this.quoter=new ethers.Contract(QUOTER_V2,abi,provider)}
 path(a,fee,b){return ethers.solidityPacked(["address","uint24","address"],[ethers.getAddress(a),fee,ethers.getAddress(b)])}
 async exactInput(tokenIn,fee,tokenOut,amountIn){const r=await this.quoter.quoteExactInput.staticCall(this.path(tokenIn,fee,tokenOut),amountIn);return{amountOut:BigInt(r[0]),gasEstimate:BigInt(r[3])}}
}