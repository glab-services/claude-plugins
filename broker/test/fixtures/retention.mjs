import assert from 'node:assert/strict';
import { Store } from '../../store.mjs';
import { Broker } from '../../broker.mjs';
import { config } from '../../config.mjs';
import { compact } from '../../retention.mjs';
import { httpFixture } from '../http.mjs';

export async function retentionFixture(t,{file=':memory:',...overrides}={}) {
  const store=new Store(file),settings={...config({}),topicRetention:100,closedRetention:50,
    retentionBatch:2,retentionMaxBatches:2,retentionBlockerLimit:2,sseHeartbeat:60000,...overrides};
  store.run("UPDATE broker_meta SET value='0' WHERE key='retention_epoch'");let clock=1000;
  const broker=new Broker(store,settings,['alice','bob','eve'],{now:()=>clock,log:()=>{}}),wire=await httpFixture(t,broker,{topic:(user,session)=>store.get('SELECT topic FROM agents WHERE session_id=?',session)?.topic??(user==='alice'?null:'compact/API')});
  t.after(()=>store.close());
  const call=async(path,args,user='alice',workspace='default')=>{
    const requestId=args?.id??(path.startsWith('/queue/item?')?Number(new URL(path,'http://fixture').searchParams.get('id')):undefined);
    const target=args?.topic??(path.startsWith('/topics/')?args?.name:undefined)??(requestId?store.get('SELECT topic FROM messages WHERE id=?',requestId)?.topic:undefined)??topic;
    const binding=user==='alice'?null:target;
    const session=user+'-'+workspace+(binding&&binding!==topic?'-'+binding.replaceAll('/','_'):'');
    const payload=path==='/send'&&args?.to&&!args.to.startsWith('topic:')?{...args,topic}:args;
    const res=await wire.request(path,{user,session,headers:{'X-Workspace':workspace,...(binding?{'X-Topic':binding}:{})},...(payload===undefined?{}:{method:'POST',body:JSON.stringify(payload)})});
    const data=await res.json();assert.ok(res.ok,JSON.stringify(data));return data;
  };
  const topic='compact/API';
  const add=async(name=topic,mode='broadcast',workspace='default')=>{
    await call('/topics',{name,mode},'alice',workspace);
    store.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)',workspace,name,'bob',broker.now());
    for(const user of ['alice','bob'])await call('/topics/subscribe',{name},user,workspace);
  };
  const send=async(args={},user='alice',workspace='default')=>call('/send',{to:'topic:'+topic,body:'synthetic compaction body',...args},user,workspace);
  const ack=async(id,user='bob',workspace='default')=>call('/ack',{id},user,workspace);
  const row=id=>store.get('SELECT * FROM messages WHERE id=?',id);
  const run=(now=2000)=>compact(store,settings,now,{publish:m=>broker.publish(m),log:()=>{}});
  const tick=()=>{clock+=1000;};
  await add();return {...wire,store,settings,broker,call,topic,add,send,ack,row,run,tick};
}
