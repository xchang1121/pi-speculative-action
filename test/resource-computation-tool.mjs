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
function groupRows(rows, label) { return { label, values: indexRows(rows) }; }
function fullRows(rows, label, footer) { return { footer, group: groupRows(rows, label) }; }
function impure(rows) { effects++; return rows.length; }
function alias(rows) { return rows; }
let renders = 0, reads = 0;
function tool(name, file) {
  return { name, label: name, description: name,
    parameters: { type: 'object', properties: { label: { type: 'string' }, footer: { type: 'string' } }, additionalProperties: false },
    execute: async (_callID, args) => {
      reads++;
      const rows = JSON.parse(await readFile(file, 'utf8'));
      const result = args.footer !== undefined ? fullRows(rows, args.label, args.footer)
        : args.label === undefined ? indexRows(rows) : groupRows(rows, args.label);
      renders++;
      return { content: [{ type: 'text', text: name }], details: { result, effects: impure(rows), alias: alias(rows) === rows } };
    },
  };
}
export { tool, indexRows, groupRows, fullRows, impure, alias };
export const counts = () => ({ effects, reads, renders });
export const setFactor = value => { factor = value; };

function keys(value) { return Object.keys(value).join('|'); }
function copy(value) { return { n: value.n }; }
function copyGroup(value) { return { copied: copy(value) }; }
function impureGroup(value) { return { count: impure(value) }; }
function captured() { return { factor }; }
function capturedGroup() { return { nested: captured() }; }
function shadowed(factor) { return { local: factor, global: capturedGroup() }; }
function recursive(value) { return value > 0 ? recursive(value - 1) : { n: 0 }; }
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
    case 'copy-group': return copyGroup(value);
    case 'impure-group': return impureGroup(value);
    case 'capture': return captured();
    case 'shadowed': return shadowed(value);
    case 'recursive': return recursive(value);
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
