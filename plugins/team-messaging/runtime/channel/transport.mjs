import http from 'node:http';
import https from 'node:https';
import { Readable } from 'node:stream';

// Dedicated agents do not inherit Node's env-proxy global agents. No pooled
// idle sockets remain after short API requests; SSE ends via its abort signal.
const agents={
  'http:':new http.Agent({keepAlive:false,proxyEnv:{}}),
  'https:':new https.Agent({keepAlive:false,proxyEnv:{}}),
};
export function brokerFetch(input,options={}) {
  const url=new URL(input);
  if(!['127.0.0.1','localhost','[::1]'].includes(url.hostname))return globalThis.fetch(input,options);
  return new Promise((resolve,reject)=>{
    const client=url.protocol==='https:'?https:http;
    const request=client.request(url,{method:options.method??'GET',headers:Object.fromEntries(new Headers(options.headers)),
      signal:options.signal,agent:agents[url.protocol]},incoming=>{
      if(incoming.statusCode>=300&&incoming.statusCode<400&&options.redirect==='error') {
        incoming.resume();reject(new Error('BROKER_REDIRECT_REFUSED'));return;
      }
      const headers=new Headers();for(const [name,value] of Object.entries(incoming.headers))
        if(value!==undefined)for(const item of Array.isArray(value)?value:[value])headers.append(name,item);
      const noBody=[204,205,304].includes(incoming.statusCode);
      if(noBody)incoming.resume();
      resolve(new Response(noBody?null:Readable.toWeb(incoming),{status:incoming.statusCode,statusText:incoming.statusMessage,headers}));
    });
    request.once('error',reject);request.end(options.body);
  });
}
