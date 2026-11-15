/* Meal Planner Plus — connects the planner to Base44 (accounts, shared household data, AI). */
(function(){
var B=null;try{B=window.parent&&window.parent!==window?window.parent.mpBridge:null}catch(e){B=null}
window.MPB=B;
if(!B){window.claude={use:function(){return Promise.resolve(null)}};return}
var E=B.base44.entities.SyncDoc,HH=B.household.id;
function members(){return B.household.members||[]}
var cache={},subs=[],loaded=false,inflight={};
function clone(x){return x==null?x:JSON.parse(JSON.stringify(x))}
function notify(){subs.forEach(function(f){try{f()}catch(e){console.error(e)}})}
function ingest(r){if(!r||r.household!==HH||!r.path)return;var c=cache[r.path];if(c&&c.pend)return;cache[r.path]={id:r.id,body:r.body,j:JSON.stringify(r.body)}}
function loadAll(){var out=[];function page(cur){return E.filter({household:HH},cur?{cursor:cur,limit:1000}:{limit:1000}).then(function(p){out=out.concat(p.items||[]);return p.has_more?page(p.next_cursor):null})}
 return page(null).then(function(){var seen={};out.forEach(function(r){seen[r.path]=1;ingest(r)});Object.keys(cache).forEach(function(p){if(!seen[p]&&!cache[p].pend&&cache[p].id)delete cache[p]});loaded=true;notify()})}
var ready=loadAll().catch(function(e){console.error('load',e);loaded=true;notify()});
try{E.subscribe(function(ev){var r=ev&&ev.data;if(ev.type==='delete'){Object.keys(cache).forEach(function(p){if(cache[p].id===ev.id&&!cache[p].pend)delete cache[p]});notify();return}if(r){if(!r.id)r.id=ev.id;ingest(r);notify()}})}catch(e){}
setInterval(function(){loadAll().catch(function(){})},45000);
function snap(path,body){return {id:path.split('/').pop(),exists:body!==undefined&&body!==null,data:function(){return clone(body)},metadata:{fromCache:!loaded,hasPendingWrites:false}}}
function write(path,body){var c=cache[path];var prev=inflight[path]||Promise.resolve();cache[path]={id:c&&c.id,body:clone(body),j:JSON.stringify(body),pend:1};notify();
 var p=prev.then(function(){var cur=cache[path];var id=cur&&cur.id;return (id?E.update(id,{body:body,members:members()}):E.create({household:HH,path:path,body:body,members:members()})).then(function(r){if(cache[path]){cache[path].id=(r&&r.id)||cache[path].id;delete cache[path].pend}})});
 inflight[path]=p.catch(function(){});return p}
function docRef(path){return {id:path.split('/').pop(),path:path,
 get:function(){return ready.then(function(){var c=cache[path];return snap(path,c?c.body:undefined)})},
 set:function(d){return write(path,clone(d))},
 update:function(d){var c=cache[path];return write(path,Object.assign({},c?clone(c.body):{},d))},
 delete:function(){var c=cache[path];delete cache[path];notify();return c&&c.id?E.delete(c.id).then(function(){}):Promise.resolve()},
 acquire:function(){return Promise.resolve({acquired:true})},
 onSnapshot:function(next){var lastJ=null,first=true;function f(){if(!loaded)return;var c=cache[path];var j=c?c.j:'__none';if(!first&&j===lastJ)return;first=false;lastJ=j;next(snap(path,c?c.body:undefined))}subs.push(f);setTimeout(f,0);return function(){var i=subs.indexOf(f);if(i>=0)subs.splice(i,1)}},
 collection:function(p){return colRef(path+'/'+p)}}}
function colRef(path){var depth=path.split('/').length+1;return {path:path,doc:function(id){return docRef(path+'/'+(id||Math.random().toString(36).slice(2)))},
 onSnapshot:function(next){var prev={},first=true;function f(){if(!loaded)return;var cur={};Object.keys(cache).forEach(function(k){if(k.indexOf(path+'/')===0&&k.split('/').length===depth)cur[k]=cache[k]});var ch=[];
   Object.keys(cur).forEach(function(k){if(prev[k]===undefined)ch.push({type:'added',doc:snap(k,cur[k].body)});else if(prev[k]!==cur[k].j)ch.push({type:'modified',doc:snap(k,cur[k].body)})});
   Object.keys(prev).forEach(function(k){if(!cur[k])ch.push({type:'removed',doc:snap(k,JSON.parse(prev[k]))})});
   if(!ch.length&&!first)return;first=false;var np={};Object.keys(cur).forEach(function(k){np[k]=cur[k].j});prev=np;
   next({docs:Object.keys(cur).map(function(k){return snap(k,cur[k].body)}),size:Object.keys(cur).length,empty:!Object.keys(cur).length,docChanges:function(){return ch},metadata:{fromCache:false,hasPendingWrites:false}})}
  subs.push(f);setTimeout(f,0);return function(){var i=subs.indexOf(f);if(i>=0)subs.splice(i,1)}},
 get:function(){return ready.then(function(){return {docs:Object.keys(cache).filter(function(k){return k.indexOf(path+'/')===0&&k.split('/').length===depth}).map(function(k){return snap(k,cache[k].body)})}})}}}
var db={doc:docRef,collection:colRef};
var Core=B.base44.integrations.Core;
function parseJSON(r){if(r&&typeof r==='object')return r;var t=String(r||'').replace(/```(?:json)?/g,'');var a=t.indexOf('{'),b=t.lastIndexOf('}');if(a<0||b<a)throw {code:'bad_json'};return JSON.parse(t.slice(a,b+1))}
var sample={limits:function(){return Promise.resolve({images:{maxCount:4}})},
 json:function(prompt,o){o=o||{};var files=o.images||[];
  return Promise.all(files.map(function(f){var file=f instanceof File?f:new File([f],'photo.jpg',{type:f.type||'image/jpeg'});return Core.UploadFile({file:file}).then(function(r){return r.file_url})}))
   .then(function(urls){var q={prompt:prompt+'\n\nReply with one JSON object only, no other text.'};if(urls.length)q.file_urls=urls;return Core.InvokeLLM(q)})
   .then(parseJSON).catch(function(e){throw (e&&e.code)?e:{code:'failed',message:String(e&&e.message||e)}})}};
var assets={upload:function(blob){var file=blob instanceof File?blob:new File([blob],'photo.jpg',{type:blob.type||'image/jpeg'});return Core.UploadFile({file:file}).then(function(r){return {url:r.file_url}})}};
window.claude={use:function(n){if(n==='db')return Promise.resolve(db);if(n==='sample')return Promise.resolve(sample);if(n==='assets')return Promise.resolve(assets);return Promise.resolve(null)}};
})();
