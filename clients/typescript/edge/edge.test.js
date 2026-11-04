// SPDX-License-Identifier: Apache-2.0
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {loadBundle} from './index.js';
const normalizedFts = {ascii_folding:false,b:0.75,case_sensitive:false,k1:1.2,k3:8,language:'english',max_token_length:39,remove_stopwords:false,stemming:false,tokenizer:'word_v2'};
const schema = {title:{type:'string',full_text_search:normalizedFts},job:{type:'string'},folder:{type:'string'}};
async function bundle(documents, s = schema) {
  const content = JSON.stringify({namespaces:[{name:'jobs',schema:s,documents}]});
  const hash = Buffer.from(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(content))).toString('hex');
  return {manifest:{version:1,namespace:'jobs',stable_as_of:123,consistency:'observed',row_count:documents.length,schema:s,tokenizers:{title:'word_v2'},content_hash:hash},content};
}
test('loads a real exported bundle and collapses prefix results to jobs', async () => {
  const input = process.env.EDGE_BUNDLE_FIXTURE ? JSON.parse(await readFile(process.env.EDGE_BUNDLE_FIXTURE,'utf8'))
    : await bundle([{id:'a',title:'electrical engineer',job:'j1'},{id:'b',title:'electrical installer',job:'j1'},{id:'c',title:'electrician',job:'j2'}]);
  assert.deepEqual(input.manifest.schema.title.full_text_search, normalizedFts);
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

test('rejects unsupported normalized options before fromStore can silently fall back',async()=>{
  for (const [option,value] of Object.entries({stemming:true,remove_stopwords:true,ascii_folding:true,
    case_sensitive:true,language:'french',max_token_length:40,k3:9,unknown:false,k1:0.1234,b:1.1})) {
    const s={...schema,title:{type:'string',full_text_search:{...normalizedFts,[option]:value}}};
    await assert.rejects(loadBundle(await bundle([],s)),/unsupported bundle tokenizer/,option);
  }
  // Tokenizer-only schemas from the initial exporter remain loadable.
  const edge=await loadBundle(await bundle([],{...schema,title:{type:'string',full_text_search:{tokenizer:'word_v2'}}}));
  edge.free();
});

test('micropuffer word_v2 honours normalized defaults and token-length boundary',async()=>{
  const {Micropuffer}=await import('micropuffer');
  const docs=[{id:'a',title:'Running THE café ABC '+ 'x'.repeat(39)+' '+ 'y'.repeat(40),job:'a'}];
  const run=fts=>{
    const engine=Micropuffer.fromStore(JSON.stringify({namespaces:[{name:'jobs',schema:{title:{type:'string',full_text_search:fts}},documents:docs}]}));
    try {
      return Object.fromEntries(['running','run','the','café','cafe','ABC','abc','x'.repeat(39),'y'.repeat(40)].map(q=>
        [q,JSON.parse(engine.query('jobs',JSON.stringify({rank_by:['title','BM25',q],top_k:10}))).rows]));
    } finally {engine.free();}
  };
  const actual=run(normalizedFts);
  assert.deepEqual(actual,run({tokenizer:'word_v2'}));
  for (const q of ['running','the','café','ABC','abc','x'.repeat(39)]) assert.equal(actual[q].length,1,q);
  for (const q of ['run','cafe','y'.repeat(40)]) assert.equal(actual[q].length,0,q);
});

test('word_v2 fromStore uses k1/b exactly in BM25; finer precision is lossy',async()=>{
  const {Micropuffer}=await import('micropuffer');
  const documents=[{id:'a',title:'cat cat dog',job:'a'},{id:'b',title:'cat dog dog dog dog',job:'b'}];
  const run=async(k1,b)=>{
    const s={...schema,title:{type:'string',full_text_search:{...normalizedFts,k1,b}}};
    const input=await bundle(documents,s);
    const engine=Micropuffer.fromStore(input.content);
    try {return JSON.parse(engine.query('jobs',JSON.stringify({rank_by:['title','BM25','cat cat'],top_k:10}))).rows;}
    finally {engine.free();}
  };
  const baseline=await run(1.2,0.75);
  for (const [k1,b] of [[1.2,0.75],[0.125,0.75],[1.2,0.25],[0.125,0.25],[0.001,0],[2,1]]) {
    const rows=await run(k1,b);
    for (const row of rows) {
      const tf=row.id==='a'?2:1, length=row.id==='a'?3:5;
      const expected=Math.log(1+0.5/2.5)*(tf*(k1+1))/(tf+k1*(1-b+b*length/4))*(2*9/10);
      assert.ok(Math.abs(row.$dist-expected)<1e-12,`${k1}/${b}: ${row.$dist} vs ${expected}`);
    }
    if (k1!==1.2 || b!==0.75) assert.notDeepEqual(rows,baseline);
    const s={...schema,title:{type:'string',full_text_search:{...normalizedFts,k1,b}}};
    const edge=await loadBundle(await bundle(documents,s));
    try {assert.equal(edge.suggest('ca',{collapseBy:'job'}).rows.length,2);}
    finally {edge.free();}
  }
  assert.deepEqual(await run(0.1254,0.75),await run(0.125,0.75));
});

test('loads a 100000-row export and answers a prefix query beyond the old cap',async()=>{
  const input=process.env.EDGE_LARGE_BUNDLE_FIXTURE
    ? JSON.parse(await readFile(process.env.EDGE_LARGE_BUNDLE_FIXTURE,'utf8'))
    : await bundle(Array.from({length:100000},(_,i)=>({id:`r${i}`,title:'electrician',job:'j1'})));
  assert.equal(input.manifest.row_count,100000);
  const edge=await loadBundle(input);
  try {assert.equal(edge.suggest('elec',{collapseBy:'job',topK:1,candidates:10}).rows[0].job,'j1');}
  finally {edge.free();}
});
