import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

function checksum(event){return crypto.createHash("sha256").update(JSON.stringify(event)).digest("hex")}
let appendChain=Promise.resolve();
export class Wal{
  constructor(dir){this.dir=dir;this.file=path.join(dir,"events.wal");this.checkpoint=path.join(dir,"events.checkpoint.json")}
  async init(){await fs.mkdir(this.dir,{recursive:true});try{await fs.access(this.file)}catch{await fs.writeFile(this.file,"","utf8")}}
  async append(event){
    const run=async()=>{
      await this.init();
      const record={...event,wal_checksum:checksum(event)};
      const handle=await fs.open(this.file,"a",0o600);
      try{await handle.writeFile(JSON.stringify(record)+"\n","utf8");await handle.sync();}
      finally{await handle.close();}
      return record;
    };
    const next=appendChain.then(run,run);appendChain=next.then(()=>undefined,()=>undefined);return next;
  }
  async replay(handler=async()=>{}){let text="";try{text=await fs.readFile(this.file,"utf8")}catch{return 0}let count=0;for(const line of text.split("\n")){if(!line.trim())continue;const record=JSON.parse(line);const {wal_checksum,...event}=record;if(wal_checksum!==checksum(event))throw new Error("WAL checksum mismatch");await handler(event);count++}return count}
  async checkpoint(state){const tmp=this.checkpoint+".tmp";await fs.writeFile(tmp,JSON.stringify({updated_at:new Date().toISOString(),...state},null,2),"utf8");await fs.rename(tmp,this.checkpoint)}
}