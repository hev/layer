// SPDX-License-Identifier: Apache-2.0
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {loadBundle} from './index.js';
const schema = {title:{type:'string',full_text_search:{tokenizer:'word_v2'}},job:{type:'string'},folder:{type:'string'}};
async function bundle(documents, s = schema) {
  const content = JSON.stringify({namespaces:[{name:'jobs',schema:s,documents}]});
  const hash = Buffer.from(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(content))).toString('hex');
  return {manifest:{version:1,namespace:'jobs',stable_as_of:123,consistency:'observed',row_count:documents.length,schema:s,tokenizers:{title:'word_v2'},content_hash:hash},content};
}
test('loads a real exported bundle and collapses prefix results to jobs', async () => {
  const input = process.env.EDGE_BUNDLE_FIXTURE ? JSON.parse(await readFile(process.env.EDGE_BUNDLE_FIXTURE,'utf8'))
    : await bundle([{id:'a',title:'electrical engineer',job:'j1'},{id:'b',title:'electrical installer',job:'j1'},{id:'c',title:'electrician',job:'j2'}]);
  const edge = await loadBundle(input);
  try {
    const result = edge.suggest('elec',{collapseBy:'job',topK:10,candidates:100});
    assert.deepEqual(new Set(result.rows.map(r=>r.job)),new Set(['j1','j2']));
    assert.equal(result.groups.find(g=>g.key==='j1').rows.length,2);
    assert.equal(edge.suggest('zzzz',{collapseBy:'job'}).rows.length,0);
  } finally {edge.free();}
});
test('rejects corruption and misleading manifests before loading',async()=>{
  const b=await bundle([]);
  await assert.rejects(loadBundle({...b,content:b.content+' '}),/hash mismatch/);
  await assert.rejects(loadBundle({...b,manifest:{...b.manifest,row_count:1}}),/manifest\/content mismatch/);
  await assert.rejects(loadBundle({...b,manifest:{...b.manifest,tokenizers:{title:'word_v4'}}}),/tokenizer/);
});
test('collapse fallback, placeholders, missing modes and candidate bounds',async()=>{
  const edge=await loadBundle(await bundle([
    {id:'a',title:'electrician',job:'No job',folder:'f'},
    {id:'b',title:'electrician',folder:'f'},
    {id:'c',title:'electrician',job:null},
    {id:'d',title:'electrician',job:''},
    {id:'e',title:'electrician'},
  ]));
  try {
    const options={collapseBy:['job','folder'],missing:{values:['No job']},candidates:100};
    const r=edge.suggest('elec',options);
    assert.equal(r.groups.length,4);
    assert.equal(r.groups.find(g=>g.key==='f').rows.length,2);
    assert.equal(r.groups.find(g=>g.key==='f').by,'folder');
    assert.equal(edge.suggest('elec',{...options,missing:{mode:'together',values:['No job']}}).groups.length,2);
    assert.equal(edge.suggest('elec',{...options,missing:{mode:'drop',values:['No job']}}).groups.length,1);
    assert.equal(edge.suggest('elec',{...options,topK:1}).groups.length,1);
    assert.throws(()=>edge.suggest('elec',{...options,topK:10,candidates:1}),/candidates/);
  } finally {edge.free();}
});
test('loads from a host-supplied URL and resolves document collapse',async()=>{
  const b=await bundle([{id:'a',title:'electrician',job:'j1'}]);
  const url=new URL('data:application/json,'+encodeURIComponent(JSON.stringify(b)));
  const edge=await loadBundle(url);
  try {
    assert.equal(edge.suggest('elec',{collapseBy:'document'}).groups[0].key,'a');
    assert.equal(edge.suggest('',{collapseBy:'document'}).rows.length,0);
  } finally {edge.free();}
});
