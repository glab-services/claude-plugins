import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve, sep } from 'node:path';
import { testTempDir } from './temp.mjs';

test('test artifacts use the ignored directory when TMPDIR is unset or points at prohibited tmpfs', () => {
  const helper = new URL('./temp.mjs', import.meta.url).href;
  const code = `import {testTempDir} from ${JSON.stringify(helper)}; console.log(testTempDir('root-check-'));`;
  for (const root of ['', '/tmp', '/tmp/nested']) {
    const child = spawnSync(process.execPath, ['--input-type=module','-e',code], { env: { ...process.env, TMPDIR: root }, encoding: 'utf8' });
    assert.equal(child.status, 0, child.stderr);
    const path = child.stdout.trim();
    assert.ok(path.startsWith(resolve(fileURLToPath(new URL('../.tmp/', import.meta.url))) + sep));
    rmSync(path, { recursive: true, force: true });
  }
});

test('test artifacts honor an explicit allowed TMPDIR and use unique names', t => {
  const root = testTempDir('custom-temp-root-');
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const helper = new URL('./temp.mjs', import.meta.url).href;
  const code = `import {testTempDir} from ${JSON.stringify(helper)}; console.log(JSON.stringify([testTempDir('unique-'),testTempDir('unique-')]));`;
  const child = spawnSync(process.execPath, ['--input-type=module','-e',code], { env:{...process.env,TMPDIR:root}, encoding:'utf8' });
  assert.equal(child.status,0,child.stderr);
  const paths = JSON.parse(child.stdout);
  assert.notEqual(paths[0],paths[1]);
  assert.ok(paths.every(path=>path.startsWith(root+sep)));
});
