import { spawnSync } from 'node:child_process';
import { available, pinnedSession, serverName } from './register.mjs';
import { readClaudeRegistration } from './client-config.mjs';
import { ChannelError } from './config.mjs';
import { unsafeOption } from './plugin-env.mjs';

export const PLUGIN_ID = 'team-messaging@team-messaging-local';
const keys = ['broker_url', 'token_file', 'workspace', 'topic', 'session_id', 'mode', 'arm'];
const run = (spawn, env, args, input) => {
  // Token variables never reach the client CLI, as register.mjs:21 does.
  const { BROKER_TOKEN: _token, TEAM_BROKER_TOKEN: _legacyToken, ...childEnv } = env;
  try { return spawn('claude', args, { env: childEnv, encoding: 'utf8', timeout: 10000, stdio: 'pipe', ...(input === undefined ? {} : { input }) }); }
  catch { return { status: null }; }
};
// Skills paste these values into shell commands (plugin-env.mjs SAFE charsets).
function refuseUnsafe(values) {
  const key = unsafeOption(Object.fromEntries(Object.entries(values).map(([name, value]) => [name.toUpperCase(), value])));
  if (key) throw new ChannelError('UNSAFE_OPTION', `Refusing to save ${key.toLowerCase()}: plugin options may contain only letters, digits and : / . _ ~ % - [ ] characters.`);
}
// Shape recorded from claude 2.1.309 with a stub plugin under a scratch HOME:
// `plugin configure <id> --json` prints {pluginId, displayName, schema, inputs,
// choices, configured, unconfigured}; inputs maps every option to its saved
// value, "" when unset, or its default. An uninstalled id exits 1 with a
// stderr message. Any other shape cannot prove "no binding", so it is a
// conflict.
export function readPluginConfig(id = PLUGIN_ID, { spawn = spawnSync, env = process.env } = {}) {
  const found = run(spawn, env, ['plugin', 'configure', id, '--json']);
  if (found.error?.code === 'ENOENT' || found.status === null) return { status: 'missing' };
  if (found.status !== 0) return { status: 'not_installed' };
  let data;
  try { data = JSON.parse(found.stdout); } catch { return { status: 'unrecognised' }; }
  const inputs = data?.inputs;
  if (!inputs || typeof inputs !== 'object' || Array.isArray(inputs) || !Array.isArray(data.configured) || !keys.every(key => typeof inputs[key] === 'string')) return { status: 'unrecognised' };
  // Keep only the binding strings; the parsed tree is dropped here.
  const values = Object.fromEntries(keys.map(key => [key, inputs[key]]));
  data = null;
  return { status: 'ok', values };
}
// configure keeps every option left out, so all seven keys are written each
// time; topic "" clears an earlier topic for a workspace-level binding.
export function writePluginConfig(id = PLUGIN_ID, values, { spawn = spawnSync, env = process.env } = {}) {
  refuseUnsafe(values);
  const result = run(spawn, env, ['plugin', 'configure', id, '--values-stdin'], JSON.stringify(Object.fromEntries(keys.map(key => [key, values[key]]))));
  return result.error?.code === 'ENOENT' || result.status === null ? 'manual' : result.status === 0 ? 'registered' : 'failed';
}
// Statuses mirror register.mjs: registered, skipped (same active binding),
// conflict (another binding or unverifiable), manual (no claude CLI or plugin).
export async function pluginAdapter({ url, workspace, topic, path, session }, { spawn = spawnSync, env = process.env, probe, cwd = process.cwd() } = {}) {
  // Checked again before anything is read or saved, whatever the caller validated.
  refuseUnsafe({ broker_url: url, token_file: path, workspace, topic: topic ?? '', session_id: session ?? '' });
  const name = serverName(url, workspace, topic, 'claude'), legacy = readClaudeRegistration(name, { env, cwd }).status === 0
    ? { legacy_registration: { server_name: name, hint: `An older user-scope MCP registration for this binding exists; remove it with claude mcp remove --scope user ${name} so the session does not run two channels.` } } : {};
  const result = (status, extra = {}) => ({ client: 'claude-plugin', plugin_id: PLUGIN_ID, status, ...extra, ...legacy });
  if (spawn === spawnSync && !available('claude', env)) return result('manual', { hint: 'Install Claude Code and the team-messaging plugin, then rerun this command.' });
  const current = readPluginConfig(PLUGIN_ID, { spawn, env });
  if (current.status === 'missing' || current.status === 'not_installed') return result('manual', { hint: `Install the plugin (claude plugin install ${PLUGIN_ID}), then rerun this command.` });
  if (current.status !== 'ok') return result('conflict', { hint: 'The plugin configuration could not be read in a known shape; it was not modified.' });
  const saved = current.values, bound = ['broker_url', 'token_file', 'workspace', 'session_id'].some(key => saved[key] !== '');
  const conflict = () => result('conflict', { hint: 'The plugin is bound to another broker, credential or workspace, or its binding could not be verified. It was not modified.' });
  if (bound) {
    if (saved.broker_url !== url || saved.token_file !== path || saved.workspace !== workspace || saved.topic !== (topic ?? '') || !/^[A-Za-z0-9_.-]{1,128}$/.test(saved.session_id) || saved.session_id === 'broker') return conflict();
    let status; try { status = await probe?.(saved.session_id); } catch {}
    if (['active', 'unregistered'].includes(status?.state)) return result('skipped', { hint: 'already configured' });
    if (status?.error !== 'SESSION_ENDED') return conflict();
  }
  const values = { broker_url: url, token_file: path, workspace, topic: topic ?? '', session_id: session ?? pinnedSession('claude-plugin', url, workspace, topic),
    mode: ['poll', 'push'].includes(saved.mode) ? saved.mode : 'poll', arm: ['rewake', 'bash', 'off'].includes(saved.arm) ? saved.arm : 'rewake' };
  const status = writePluginConfig(PLUGIN_ID, values, { spawn, env });
  return result(status, status === 'registered' ? { session_id: values.session_id, hint: 'Restart Claude Code to apply the plugin configuration.' } : {});
}
