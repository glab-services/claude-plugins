import { mkdirSync, mkdtempSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';

export function testTempDir(prefix) {
  const fallback = fileURLToPath(new URL('../.tmp/', import.meta.url));
  let root = resolve(process.env.TMPDIR || fallback);
  if (root === '/tmp' || root.startsWith('/tmp/')) root = fallback;
  mkdirSync(root, { recursive: true, mode: 0o700 });
  return mkdtempSync(join(root, prefix));
}
