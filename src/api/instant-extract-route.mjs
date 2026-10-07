import {extract} from "../extract/instant-extract.mjs";

const MAX_BODY=32768;

async function readJson(req){
 let body="";
 for await(const chunk of req){
  body+=chunk;
  if(Buffer.byteLength(body,"utf8")>MAX_BODY)throw new Error("request body too large");
 }
 if(!body)return{};
 try{return JSON.parse(body)}catch{throw new Error("invalid JSON")}
}

function send(res,status,payload){
 res.writeHead(status,{
  "content-type":"application/json; charset=utf-8",
  "cache-control":"no-store",
  "x-content-type-options":"nosniff"
 });
 res.end(JSON.stringify(payload));
}

export async function handleInstantExtract(req,res){
 if(req.method!=="POST"){send(res,405,{ok:false,error:"method not allowed"});return true;}
 try{
  const body=await readJson(req);
  if(typeof body.url!=="string"||!body.url.trim()){send(res,400,{ok:false,error:"url is required"});return true;}
  const result=await extract(body.url);
  send(res,200,{ok:true,result});
 }catch(error){
  send(res,400,{ok:false,error:error instanceof Error?error.message:String(error)});
 }
 return true;
}
