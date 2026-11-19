import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const root = process.argv[2];
const read = p => JSON.parse(fs.readFileSync(p, 'utf8'));
const sha = p => crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const topPath = path.join(root, 'manifest.json');
const top = read(topPath);
const errors = [];
const kinds = {}, states = {}, totals = {};
const sources = new Set(), listed = new Set(), dates = [];
for (const entry of top.entries) {
  const source = entry.source_id;
  if (!/^\d{14}-\d-\d+\/\d{4}$/.test(source)) {
    errors.push(`unexpected source id: ${source}`);
    continue;
  }
  if (sources.has(source)) errors.push(`duplicate source: ${source}`);
  sources.add(source);
  const folder = path.join(root, source.replace('/', '_'));
  const nested = path.join(folder, 'manifest.json');
  if (!fs.existsSync(nested)) { errors.push(`missing nested manifest: ${source}`); continue; }
  if (JSON.stringify(read(nested)) !== JSON.stringify(entry)) errors.push(`nested manifest differs: ${source}`);
  const inspection = entry.inspection;
  for (const key of ['item_records_seen','item_count_reported','document_count','downloaded_document_count','document_count_reported','invalid_or_missing_body_hashes']) {
    totals[key] = (totals[key] || 0) + inspection[key];
  }
  if (!inspection.observed_index_complete) errors.push(`incomplete observed index: ${source}`);
  for (const f of entry.files) {
    if (path.basename(f.file) !== f.file || f.file === '.' || f.file === '..') {
      errors.push(`unsafe filename: ${source}: ${f.file}`); continue;
    }
    const p = path.join(folder, f.file);
    if (listed.has(p)) errors.push(`duplicate path: ${p}`);
    listed.add(p);
    kinds[f.kind] = (kinds[f.kind] || 0) + 1;
    states[f.state] = (states[f.state] || 0) + 1;
    dates.push(f.received_at);
    if (!fs.existsSync(p)) { errors.push(`missing file: ${source}: ${f.file}`); continue; }
    if (fs.statSync(p).size !== f.bytes) errors.push(`size mismatch: ${source}: ${f.file}`);
    if (sha(p) !== f.sha256) errors.push(`hash mismatch: ${source}: ${f.file}`);
  }
}
const disk = new Set(fs.readdirSync(root, {withFileTypes:true}).filter(d => d.isDirectory()).flatMap(d =>
  fs.readdirSync(path.join(root,d.name)).filter(n => n !== 'manifest.json').map(n => path.join(root,d.name,n))));
const unlisted = [...disk].filter(p => !listed.has(p)).length;
const unresolved = [...listed].filter(p => !disk.has(p)).length;
if (unlisted || unresolved) errors.push(`unlisted/unresolved files: ${unlisted}/${unresolved}`);
if (top.cohort_size !== top.entries.length) errors.push('cohort size mismatch');
if (top.observed_index_complete !== top.entries.filter(e => e.inspection.observed_index_complete).length) errors.push('observed index count mismatch');
if (top.complete_with_edital_label !== top.entries.filter(e => e.inspection.edital_or_reference_label_count > 0 && e.inspection.observed_index_complete).length) errors.push('label count mismatch');
console.log(JSON.stringify({root_manifest_sha256:sha(topPath),cohort_size_claimed:top.cohort_size,entries_checked:top.entries.length,file_entries_checked:listed.size,kinds,states,received_at_min:dates.sort()[0],received_at_max:dates.at(-1),totals,errors},null,2));
if (errors.length) process.exitCode = 1;
