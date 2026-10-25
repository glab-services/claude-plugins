import { check } from './config.mjs';

// Literal POSIX repository paths only: no filesystem access or glob expansion.
export function metadata(values, kind, config) {
  const code = kind === 'paths' ? 'INVALID_PATHS' : 'INVALID_CONTRACTS';
  check(Array.isArray(values) && values.length <= config.planningMaxItems, code);
  return [...new Set(values.map(value => {
    check(typeof value === 'string' && value.length > 0 && value.length <= config.planningMaxItemChars &&
      value === value.trim() && !/[\p{Cc}\p{Cf}\p{Cs}\u2028\u2029]/u.test(value), code);
    if (kind !== 'paths') return value;
    check(!/^[\/]/.test(value) && !/[\\:*?\[\]{}]/.test(value), code);
    check(!/[!@+]\(/.test(value), code); // Extglobs without the already-rejected * and ? operators.
    const parts = value.split('/');
    check(!parts.includes('..'), code);
    return parts.filter(part => part && part !== '.').join('/') || '.';
  }))].sort();
}

export function workstream(row) {
  return { ...row, paths: row.paths === null ? null : JSON.parse(row.paths),
    contracts: row.contracts === null ? null : JSON.parse(row.contracts) };
}

export function pathOverlaps(a, b) {
  return a === '.' || b === '.' || a === b || a.startsWith(b + '/') || b.startsWith(a + '/');
}

export function fields(args, allowed) {
  check(Object.keys(args).every(key => allowed.includes(key)), 'INVALID_PLANNING_FIELDS');
}
