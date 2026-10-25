#!/usr/bin/env node
// Builds the team-messaging plugin bundle and its local-directory marketplace.
// Local-path plugins load in place with no dependency install, and copied
// plugins cannot reach files outside their root (plugins/loading.md), so the
// bundle carries channel/, shared/ and node_modules itself.
import { chmodSync, cpSync, existsSync, mkdirSync, readdirSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
function fail(message) { process.stderr.write(`build-plugin: ${message}\n`); process.exit(1); }
const args = process.argv.slice(2);
if (args.length !== 2 || args[0] !== '--out') fail('use build-plugin.mjs --out <dir>');
const out = resolve(args[1]), inside = base => out === base || out.startsWith(base + sep);
// /tmp is a small shared tmpfs on the build machine; output goes under
// $TMPDIR or the ignored dist/ only, and never over existing files.
if (inside('/tmp')) fail('refusing /tmp; use $TMPDIR or dist/');
if (!((process.env.TMPDIR && inside(resolve(process.env.TMPDIR))) || inside(join(root, 'dist')))) fail('--out must be under $TMPDIR or dist/');
if (existsSync(out) && readdirSync(out).length) fail(`${out} is not empty; remove it or choose another directory`);
const plugin = join(out, 'plugins', 'team-messaging');
mkdirSync(join(out, '.claude-plugin'), { recursive: true });
cpSync(join(root, 'plugin'), plugin, { recursive: true });
mkdirSync(join(plugin, 'runtime', 'channel'), { recursive: true });
mkdirSync(join(plugin, 'runtime', 'shared'), { recursive: true });
// runtime/channel and runtime/shared keep the channel's ../shared import valid.
for (const file of readdirSync(join(root, 'channel'))) if (file.endsWith('.mjs')) cpSync(join(root, 'channel', file), join(plugin, 'runtime', 'channel', file));
cpSync(join(root, 'shared', 'address.mjs'), join(plugin, 'runtime', 'shared', 'address.mjs'));
writeFileSync(join(out, '.claude-plugin', 'marketplace.json'), JSON.stringify({
  name: 'team-messaging-local', owner: { name: 'glab-services' }, description: 'Local marketplace for the team-messaging plugin bundle.',
  plugins: [{ name: 'team-messaging', source: './plugins/team-messaging', description: 'Team agent messaging channel, hooks and skills.' }],
}, null, 2) + '\n');
const run = (command, argv, options = {}) => spawnSync(command, argv, { stdio: 'inherit', ...options });
if (run('npm', ['ci', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: plugin }).status !== 0) fail('npm ci failed in the plugin root');
for (const file of readdirSync(join(plugin, 'bin'))) chmodSync(join(plugin, 'bin', file), 0o755);
const claude = run('claude', ['--version'], { stdio: 'ignore' });
if (claude.error?.code === 'ENOENT') {
  if (process.env.PLUGIN_VALIDATE_SKIP !== '1') fail('claude is not on PATH; install it or set PLUGIN_VALIDATE_SKIP=1');
  process.stdout.write('SKIP: claude plugin validate (claude not on PATH)\n');
} else for (const target of [plugin, out]) if (run('claude', ['plugin', 'validate', target, '--strict']).status !== 0) fail(`claude plugin validate --strict failed for ${target}`);
process.stdout.write(`built ${plugin}\n`);
