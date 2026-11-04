// SPDX-License-Identifier: Apache-2.0
// Hand-written edge entry; kept beside the generated HTTP client.
const integer = (value, name, max) => {
  if (!Number.isInteger(value) || value < 1 || value > max) throw Error(`${name} must be 1..${max}`);
  return value;
};
const canonical = value => JSON.stringify(value && typeof value === 'object'
  ? Array.isArray(value) ? value.map(v => JSON.parse(canonical(v)))
    : Object.fromEntries(Object.keys(value).sort().map(k => [k, JSON.parse(canonical(value[k]))]))
  : value);

// Match the gateway's fail-closed micropuffer 0.2.0 compatibility gate.
const ftsDefaults = {tokenizer:'word_v2',ascii_folding:false,case_sensitive:false,
  language:'english',max_token_length:39,remove_stopwords:false,stemming:false,k1:1.2,b:0.75,k3:8};
const supportedFts = fts => fts && typeof fts === 'object' && fts.tokenizer === 'word_v2'
  && Object.entries(fts).every(([key, value]) => {
    if (key === 'k1' || key === 'b') return typeof value === 'number' && Number.isFinite(value)
      && Number.isSafeInteger(Math.round(value * 1000)) && Math.round(value * 1000) / 1000 === value
      && (key === 'k1' ? value > 0 : value >= 0 && value <= 1);
    return Object.hasOwn(ftsDefaults, key) && value === ftsDefaults[key];
  });

/** Verify and load a host-supplied bundle, then build the lazy text index. */
export async function loadBundle(input, {field} = {}) {
  const bundle = typeof input === 'string' || input instanceof URL
    ? await (async () => { const r = await fetch(input); if (!r.ok) throw Error(`bundle fetch failed: ${r.status}`); return r.json(); })()
    : input;
  if (!bundle || typeof bundle.content !== 'string') throw Error('invalid bundle content');
  const {manifest: m, content} = bundle;
  if (m?.version !== 1 || m.consistency !== 'observed' || !Number.isSafeInteger(m.stable_as_of)) throw Error('unsupported bundle manifest');
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(content));
  const hash = Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, '0')).join('');
  if (hash !== m.content_hash) throw Error('bundle content hash mismatch');
  const store = JSON.parse(content);
  const ns = store.namespaces?.[0];
  if (store.namespaces?.length !== 1 || ns?.name !== m.namespace || !Array.isArray(ns.documents)
    || ns.documents.length !== m.row_count || !Number.isSafeInteger(m.row_count) || m.row_count < 0
    || canonical(ns.schema) !== canonical(m.schema)) throw Error('bundle manifest/content mismatch');
  const textFields = Object.keys(ns.schema).filter(k => ns.schema[k].full_text_search);
  if (!textFields.length || canonical(Object.fromEntries(textFields.map(k => [k, 'word_v2']))) !== canonical(m.tokenizers)
    || textFields.some(k => ns.schema[k].type !== 'string' || !supportedFts(ns.schema[k].full_text_search))) throw Error('unsupported bundle tokenizer');
  if (field === undefined) {
    if (textFields.length !== 1) throw Error('select a full-text field');
    field = textFields[0];
  }
  if (!textFields.includes(field)) throw Error('field is not a bundle full-text field');
  const {Micropuffer} = await import('micropuffer');
  const engine = Micropuffer.fromStore(content);
  const query = (text, top_k) => JSON.parse(engine.query(m.namespace, JSON.stringify({
    rank_by: [field, 'BM25', text, {last_as_prefix: true}], top_k, include_attributes: true,
  })));
  try {
    // A nonempty query initializes the lazy index, including for an empty bundle.
    query('edgeprewarm', 1);
  } catch (error) { engine.free(); throw error; }
  return {
    manifest: structuredClone(m),
    free: () => engine.free(),
    suggest(text, {collapseBy, topK = 10, candidates = Math.min(10000, topK * 10), missing = 'own'} = {}) {
      if (typeof text !== 'string') throw Error('text must be a string');
      integer(topK, 'topK', 10000); integer(candidates, 'candidates', 10000);
      if (candidates < topK) throw Error('candidates must be >= topK');
      const by = typeof collapseBy === 'string' ? [collapseBy] : collapseBy;
      if (!Array.isArray(by) || !by.length || by.length > 4 || by.some(k => typeof k !== 'string' || !k || k === 'id' || k === 'vector')) throw Error('collapseBy requires 1..4 attributes');
      const mode = typeof missing === 'string' ? missing : missing?.mode ?? 'own';
      const values = typeof missing === 'object' && missing ? missing.values ?? [] : [];
      if (!['own', 'together', 'drop'].includes(mode) || !Array.isArray(values)
        || values.some(v => !['string', 'number', 'boolean'].includes(typeof v))
        || (typeof missing === 'object' && missing && Object.keys(missing).some(k => !['mode', 'values'].includes(k)))) throw Error('invalid missing configuration');
      const attributes = by.map(k => [k, k === 'document' ? (ns.schema._hevlayer_parent_id ? '_hevlayer_parent_id' : 'id') : k]);
      if (attributes.some(([, k]) => k !== 'id' && !Object.hasOwn(ns.schema, k))) throw Error('collapse attribute is not in bundle');
      if (!text.trim()) return {rows: [], groups: []};
      const ranked = query(text, candidates).rows;
      const groups = [], index = new Map();
      for (const row of ranked) {
        const found = attributes.find(([, k]) => row[k] !== undefined && row[k] !== null && row[k] !== '' && !values.some(v => canonical(v) === canonical(row[k])));
        if (!found && mode === 'drop') continue;
        const key = found ? row[found[1]] : null;
        const slot = found ? canonical([found[0], key]) : mode === 'together' ? 'missing' : undefined;
        let group = slot === undefined ? undefined : index.get(slot);
        if (!group) {
          group = {key, by: found?.[0] ?? null, rows: []};
          groups.push(group);
          if (slot !== undefined) index.set(slot, group);
        }
        group.rows.push(row);
      }
      const selected = groups.slice(0, topK);
      return {rows: selected.map(g => g.rows[0]), groups: selected};
    },
  };
}
