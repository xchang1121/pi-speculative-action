import { readFile } from 'node:fs/promises';
let factor = 2, effects = 0;
function indexRows(rows) {
  const result = [];
  for (const row of rows) {
    let sum = 0;
    for (let n = 0; n < 20000; n++) sum += (row.value * factor + n) % 997;
    result.push({ value: sum });
  }
  return result;
}
function impure(rows) { effects++; return rows.length; }
function alias(rows) { return rows; }
let renders = 0, reads = 0;
function tool(name, file) {
  return { name, label: name, description: name,
    parameters: { type: 'object', properties: {}, additionalProperties: false },
    execute: async () => {
      reads++;
      const rows = JSON.parse(await readFile(file, 'utf8'));
      const result = indexRows(rows);
      renders++;
      return { content: [{ type: 'text', text: name }], details: { result, effects: impure(rows), alias: alias(rows) === rows } };
    },
  };
}
export { tool, indexRows, impure, alias };
export const counts = () => ({ effects, reads, renders });
export const setFactor = value => { factor = value; };

function keys(value) { return Object.keys(value).join('|'); }
function copy(value) { return { n: value.n }; }
function captured() { return { factor }; }
function change(value) { value.n++; return value.n; }
function aliasedChange(value) { const selected = value; selected.n++; return selected.n; }
function equal(left, right) { return left === right; }
function absolute(value) { return Math.abs(value); }
function ownName() { return ownName.name; }
export const rename = value => { Object.defineProperty(ownName, 'name', { value }); };
function inherited(value) { const child = {}; child.__proto__ = value; child.n++; return child.n; }
let replacement = value => ({ n: value.n });
export const replace = value => { replacement = value; };
let revives = 0;
export const reviverCalls = () => revives;
export function cases(kind, value, other) {
  switch (kind) {
    case 'keys': return keys(value);
    case 'copy': return copy(value);
    case 'capture': return captured();
    case 'change': return change(value);
    case 'alias-change': return aliasedChange(value);
    case 'identity': return alias(value);
    case 'equal': return equal(value, other);
    case 'abs': return absolute(value);
    case 'replace': return replacement(value);
    case 'parse': return JSON.parse(value);
    case 'revive': return JSON.parse(value, (_key, item) => { revives++; return item; });
    case 'optional': return ((JSON) => JSON?.parse(value))(null);
    case 'self': return ownName();
    case 'inherited': return inherited(value);
  }
}
