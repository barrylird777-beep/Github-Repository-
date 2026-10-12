import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
function checksum(event){return crypto.createHash("sha256").update(JSON.stringify(event)).digest("hex")}
export class Wal{
 constructor(dir){this.dir=path.resolve(dir);this.file=path.join(this.dir,"events.wal");this.checkpointFile=path.join(this.dir,"events.checkpoint.json");this.appendChain=Promise.resolve();}
 async init(){await fs.mkdir(this.dir,{recursive:true,mode:0o700});try{const handle=await fs.open(this.file,"wx",0o600);await handle.sync();await handle.close();await this.syncDirectory();}catch(error){if(error.code!=="EEXIST")throw error;}return this;}
 async syncDirectory(){const handle=await fs.open(this.dir,"r");try{await handle.sync();}finally{await handle.close();}}
 async append(event){const write=async()=>{const record={...event,wal_checksum:checksum(event)};const handle=await fs.open(this.file,"a",0o600);try{await handle.writeFile(JSON.stringify(record)+"\n","utf8");await handle.sync();}finally{await handle.close();}return record;};const pending=this.appendChain.then(write,write);this.appendChain=pending.then(()=>undefined,()=>undefined);return pending;}
 async replay(handler=async()=>{}){await this.appendChain;let text="";try{text=await fs.readFile(this.file,"utf8");}catch(error){if(error.code==="ENOENT")return 0;throw error;}let count=0;for(const line of text.split("\n")){if(!line.trim())continue;const record=JSON.parse(line);const {wal_checksum,...event}=record;if(wal_checksum!==checksum(event))throw new Error("WAL checksum mismatch");await handler(event);count++;}return count;}
 async checkpoint(state){const temp=this.checkpointFile+".tmp";const handle=await fs.open(temp,"w",0o600);try{await handle.writeFile(JSON.stringify({updated_at:new Date().toISOString(),...state},null,2),"utf8");await handle.sync();}finally{await handle.close();}await fs.rename(temp,this.checkpointFile);await this.syncDirectory();}
}
