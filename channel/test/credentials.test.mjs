import { test } from 'node:test';
import assert from 'node:assert/strict';
import { chmodSync, lstatSync, mkdirSync, rmSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { testTempDir } from '../../test-support/temp.mjs';
import { credentialName, credentialPath, ensurePrivateDir, preflightCredentialDir, readCredential, writeCredential } from '../credentials.mjs';

// Every case is built with mkdir/chmod on the real filesystem. The scratch
// root sits below the private $TMPDIR, so each case makes its own HOME the
// walk boundary and gives it the mode under test: a sealed ancestor above the
// HOME is never consulted. No case needs a supplementary group.
function scratch(t, homeMode) {
  const root = testTempDir('credentials-'); t.after(() => rmSync(root, { recursive: true, force: true }));
  const home = join(root, 'home'); mkdirSync(home); chmodSync(home, homeMode);
  return { home, dir: (path, mode) => { const full = join(home, path); mkdirSync(full); chmodSync(full, mode); return full; } };
}
const writable = dir => ({ code: 'INSECURE_CREDENTIAL_DIR', dir, message: 'INSECURE_CREDENTIAL_DIR', hint: `${dir} is group- or world-writable; run chmod go-w '${dir}'` });
const sealedAncestors = true;

test('with sealedAncestors, a sealed ancestor accepts group- and world-writable directories below it, up to and including the boundary', t => {
  const s = scratch(t, 0o755); s.dir('sealed', 0o700); s.dir('sealed/loose', 0o775);
  const world = s.dir('sealed/loose/world', 0o777);
  ensurePrivateDir(join(world, 'state'), { home: s.home, sealedAncestors });
  assert.equal(lstatSync(join(world, 'state')).mode & 0o777, 0o700);
  // The boundary itself may be the sealed ancestor.
  const b = scratch(t, 0o700), w = b.dir('w', 0o777);
  ensurePrivateDir(join(w, 'state'), { home: b.home, sealedAncestors });
});

test('with sealedAncestors, writable directories with no sealed ancestor are refused, naming the lowest', t => {
  const s = scratch(t, 0o755); s.dir('world', 0o777);
  const lower = s.dir('world/lower', 0o775);
  assert.throws(() => ensurePrivateDir(join(lower, 'state'), { home: s.home, sealedAncestors }), writable(lower));
  const g = scratch(t, 0o755), group = g.dir('group', 0o775);
  assert.throws(() => ensurePrivateDir(join(group, 'state'), { home: g.home, sealedAncestors }), writable(group));
  // A sealed directory below the writable one does not protect it.
  const below = scratch(t, 0o755), outer = below.dir('w', 0o777), inner = below.dir('w/sealed', 0o700);
  assert.throws(() => ensurePrivateDir(join(inner, 'state'), { home: below.home, sealedAncestors }), writable(outer));
});

test('with sealedAncestors, a user-owned symlink and a non-private leaf are still refused below a sealed ancestor', t => {
  const s = scratch(t, 0o700), real = s.dir('real', 0o700), link = join(s.home, 'link');
  symlinkSync(real, link);
  assert.throws(() => ensurePrivateDir(join(link, 'state'), { home: s.home, sealedAncestors }),
    { code: 'INSECURE_CREDENTIAL_DIR', hint: 'Credential ancestors must not contain user-owned symlinks.' });
  const leaf = s.dir('real/leaf', 0o750);
  assert.throws(() => ensurePrivateDir(leaf, { home: s.home, sealedAncestors }),
    { code: 'INSECURE_CREDENTIAL_DIR', hint: 'Use a private directory owned by your user with mode 0700 and no symlink.' });
});

// Decision 149: credential directories keep the strict rule, so a 0775 or 0777
// ancestor is refused even below a private 0700 HOME, by every credential path.
for (const mode of [0o775, 0o777]) test(`credential paths refuse a 0${mode.toString(8)} ancestor below a private 0700 HOME`, t => {
  const s = scratch(t, 0o700), config = s.dir('.config', mode), creds = join(config, 'team-messaging', 'credentials'), token = join(creds, 'x.token');
  assert.throws(() => ensurePrivateDir(creds, { home: s.home }), writable(config));
  assert.throws(() => preflightCredentialDir(creds, { home: s.home }), writable(config));
  assert.throws(() => writeCredential(token, 'synthetic-value', { home: s.home }), writable(config));
  // A token file already in place below the writable ancestor is not read either.
  chmodSync(config, 0o700); writeCredential(token, 'synthetic-value', { home: s.home }); chmodSync(config, mode);
  assert.throws(() => readCredential(token, { home: s.home }), writable(config));
});

test('the chmod hint single-quotes the path and escapes a quote inside it', t => {
  const s = scratch(t, 0o755), odd = s.dir("it's here", 0o777);
  assert.throws(() => ensurePrivateDir(join(odd, 'state'), { home: s.home }),
    { code: 'INSECURE_CREDENTIAL_DIR', hint: `${odd} is group- or world-writable; run chmod go-w '${s.home}/it'\\''s here'` });
});

// Plan project-options AM-2: only the exact join path for this URL and name.
test('credentialName accepts only the join path for this broker URL and returns its name', () => {
  const env = { HOME: '/home/u' }, url = 'https://broker.example', path = credentialPath(url, 'bob', { env });
  assert.equal(credentialName(url, path, { env }), 'bob');
  assert.equal(credentialName(url, credentialPath(url, 'bob', { env: { HOME: '/h', XDG_CONFIG_HOME: '/x' } }), { env: { HOME: '/h', XDG_CONFIG_HOME: '/x' } }), 'bob');
  for (const [candidate, otherUrl] of [[path, 'https://attacker.example'], [path.replace('/home/u/', '/home/v/'), url], ['/home/u/.config/team-messaging/credentials/plugin.token', url],
    [credentialPath(url, 'bob', { env }).replace('-bob.token', '-broker.token'), url], [path + '.bak', url], [undefined, url], ['', url], [path.replace(/[0-9a-f]{16}-/, '0000000000000000-'), url]])
    assert.throws(() => credentialName(otherUrl, candidate, { env }), { code: 'TOKEN_FILE_MISMATCH' }, String(candidate));
});
