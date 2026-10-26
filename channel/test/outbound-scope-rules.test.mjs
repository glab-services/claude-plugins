// Project-scoped sharing A1: the outbound guard's rules, one table per rule.
// The module is loaded with await import() inside each test, so on a tree
// without channel/outbound-scope.mjs every test fails on its own (plan B1)
// instead of the whole file failing to load.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const load = () => import('../outbound-scope.mjs');
const repo = fileURLToPath(new URL('../../', import.meta.url));
const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
// Token shapes are assembled at run time so this file never holds a literal
// credential (AGENTS.md: never print secrets).
const hex = n => 'ab12'.repeat(Math.ceil(n / 4)).slice(0, n), alnum = n => 'Zq9x'.repeat(Math.ceil(n / 4)).slice(0, n);
const shapes = {
  tmc: ['tm', 'c_', hex(32), '_', alnum(43)].join(''),
  akia: ['AK', 'IA', 'Q7ZR4M2K8T3W5N6P'].join(''),
  ghp: ['gh', 'p_', alnum(36)].join(''),
};
async function check(value, { cls = 'free', field = 'body', exempt } = {}) {
  const { checkOutbound, projectDirNames, projectFileNames } = await load();
  return checkOutbound([{ field, value, cls }], exempt ?? { names: [], strip: [], dirs: projectDirNames(repo), files: projectFileNames?.(repo) });
}
const live = () => {
  const info = os.userInfo();
  return { user: info.username, home: info.homedir, host: os.hostname() };
};
const generic = new Set(['root', 'node', 'admin', 'user', 'ubuntu', 'debian', 'vscode', 'runner', 'codespace', 'dev', 'test', 'pi', 'ec2-user', 'centos', 'fedora', 'docker', 'app', 'build', 'ci', 'home', 'localhost']);
const checked = value => value.length >= 4 && !generic.has(value.toLowerCase());
// Replaces the node:os identity for one test; the guard reads it at call
// time. Every restore goes back to the values saved at load, so two calls in
// one test cannot leave a replacement behind.
const original = { userInfo: os.userInfo, hostname: os.hostname, HOME: process.env.HOME };
function identity(t, { username, homedir, hostname, home }) {
  const real = original.userInfo();
  os.userInfo = () => ({ ...real, ...(username !== undefined ? { username } : {}), ...(homedir !== undefined ? { homedir } : {}) });
  if (hostname !== undefined) os.hostname = () => hostname;
  if (home !== undefined) process.env.HOME = home;
  t.after(() => { os.userInfo = original.userInfo; os.hostname = original.hostname; if (original.HOME === undefined) delete process.env.HOME; else process.env.HOME = original.HOME; });
}

test('every reason refuses its AM-S4 case and reports only rule and field', async () => {
  const { RULES } = await load(), { user, home, host } = live();
  const cases = [
    ['private_key', ['-----BEGIN ', 'OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQ\n-----END ', 'OPENSSH PRIVATE KEY-----'].join('')],
    // r1 L2-2: content shapes run before the path rules.
    ['system_file', 'root:x:0:0:root:/root:/bin/bash\ndaemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin'],
    ['system_file', 'daemon:*:19000:0:99999:7:::'],
    ['secret', `token ${shapes.tmc}`], ['secret', `key ${shapes.akia}`], ['secret', `pat ${shapes.ghp}`],
    ['secret_assignment', ['DB_PASS', 'WORD=', 'hunter2', 'hunter2'].join('')],
    ['env_dump', 'SHELL=/bin/bash\nPATH=/usr/local/bin:/usr/bin\nLANG=C.UTF-8'],
    ['os_release', 'PRETTY_NAME="Ubuntu 24.04 LTS"\nNAME="Ubuntu"\nVERSION_ID="24.04"\nID=ubuntu\nID_LIKE=debian'],
    ['kernel_info', 'Linux buildbox 6.8.0-38-generic #38-Ubuntu SMP PREEMPT_DYNAMIC Fri Jun  7 15:25:01 UTC 2024 x86_64 x86_64 x86_64 GNU/Linux'],
    ['cpu_info', 'processor\t: 0\nvendor_id\t: GenuineIntel\nmodel name\t: Intel(R) Core(TM) i7-1185G7'],
    ['mem_info', 'MemTotal:       32617016 kB\nMemFree:         1124012 kB'],
    ['net_config', '1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN\n    inet 127.0.0.1/8 scope host lo'],
    ['net_config', 'eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500\n        inet 10.0.0.5  netmask 255.255.255.0  broadcast 10.0.0.255'],
    ['package_listing', '||/ Name           Version      Architecture Description\nii  adduser        3.137ubuntu1 all          add and remove users'],
    ['package_listing', 'bash-5.1.8-9.el9.x86_64\ncoreutils-8.32-35.el9.x86_64'],
    ['package_listing', '/usr/lib\n├── corepack@0.29.4\n└── npm@10.8.2'],
    // The home listing names the live home, so the identity rule reports it.
    ['machine_identity', `total 24\ndrwxr-x--- 12 u u 4096 Oct 26 09:00 ${home}/projects\n-rw-r--r--  1 u u  807 Oct 26 09:00 ${home}/.profile`],
    ['absolute_path', 'C:\\Users\\x\\notes.txt'], ['absolute_path', 'see file:///etc/hosts'], ['absolute_path', 'cat /etc/hosts'], ['absolute_path', 'share \\\\fileserver\\share$\\x'],
    // Our credential path is a home path before it is a sensitive name.
    ['home_path', '~/.config/team-messaging/credentials/0123456789abcdef-bob.token'], ['home_path', 'cd ~'], ['home_path', 'ls $HOME/projects'],
    ['parent_escape', '../other-repo/a'], ['parent_escape', 'src\\..\\..\\x'], ['parent_escape', 'see ../../other-repo/secret'],
    ['sensitive_name', 'the key is under .ssh/config'], ['sensitive_name', 'copy id_ed25519.pub'],
    ['opaque_blob', 'QUJD'.repeat(60)],
    ['mixed_script', 'see /h\u043eme/x'], ['non_ascii_path', 'cat /\u1d07tc/passwd'],
  ];
  if (checked(host)) cases.push(['machine_identity', `ran on ${host} today`]);
  if (checked(user)) cases.push(['machine_identity', `logged in as ${user}`]);
  for (const [rule, value] of cases) assert.deepEqual(await check(value), { rule, field: 'body' }, `${rule}: ${JSON.stringify(value.slice(0, 40))}`);
  assert.deepEqual(new Set(cases.map(([rule]) => rule)), new Set(RULES), 'one case for each reason (FA-12)');
  // 17 from the plan, plus mixed_script and non_ascii_path from review r2 SS2.
  assert.equal(RULES.length, 19);
});

test('accepts project content', async () => {
  const { BROKER_ROUTES } = await load(), { user } = live();
  const accepted = [
    'channel/server.mjs\nchannel/outbound-scope.mjs\ndocs/setup.md\nREADME.md',
    // FD-3: project source with a ../ import that stays inside the tree.
    read('channel/server.mjs'),
    read('package.json'),
    `Reply with send(to=user:alice, kind=answer); the thread is on topic:dev/api in ws:default; ping user:${user}`,
    'https://example.com/a/b', '</channel>', 'a/b and/or c', '//comment', '~ approx', 'task-1234567890abcdefghij',
    'TM_TOKEN_FILE="${user_config.token_file}"', 'const token = readCredential(file)',
    // FA-10: example files and the README token example.
    read('channel/env.example'), read('broker/env.example'), read('channel/README.md').split('\n')[21],
    'BROKER_URL=http://127.0.0.1:8091\nBROKER_TOKEN=replace-with-your-token\nNAME=your-name\nDATA=/path/to/x\nHOME_DIR=${HOME_DIR}',
    'sha512-' + 'A'.repeat(86) + '==',
    'call /inbox?since=0 then /send.',
  ];
  for (const value of accepted) assert.equal(await check(value), null, JSON.stringify(value.slice(0, 60)));
  assert.match(read('channel/README.md').split('\n')[21], /TEAM_BROKER_TOKEN=/, 'the README line FA-10 names');
  // The route list equals the broker's routes, read from broker/server.mjs now.
  const routes = [...new Set([...read('broker/server.mjs').matchAll(/'(?:GET|POST) (\/[^' ]*)'/g)].map(match => match[1]))].sort();
  assert.deepEqual([...BROKER_ROUTES].sort(), routes);
  for (const route of BROKER_ROUTES) assert.equal(await check(`the broker route ${route} answers`), null, route);
});

test('pip freeze and brew list shapes pass (residual R-3)', async () => {
  // Identical to a project's requirements.txt or a list of names, so the
  // guard cannot tell them apart; L3 denies Bash in a team-message window.
  assert.equal(await check('requests==2.31.0\nurllib3==2.2.1\ncertifi==2024.2.2'), null);
  assert.equal(await check('git\nnode\nwget\njq'), null);
});

test('identity exemptions', async t => {
  identity(t, { username: 'carol-dev', hostname: 'brokerhost.example', home: '/home/carol-dev', homedir: '/home/carol-dev' });
  const { checkOutbound } = await load();
  const body = value => [{ field: 'body', value, cls: 'free' }];
  // The own broker name equal to the user name, and a teammate name.
  assert.deepEqual(checkOutbound(body('ask carol-dev'), { names: [], strip: [] }), { rule: 'machine_identity', field: 'body' });
  assert.equal(checkOutbound(body('ask carol-dev'), { names: ['carol-dev'], strip: [] }), null);
  assert.equal(checkOutbound(body('ask dana about it'), { names: ['dana'], strip: [] }), null);
  // The broker URL host is removed before the hostname check.
  assert.deepEqual(checkOutbound(body('the broker is brokerhost.example port 8091'), { names: [], strip: [] }), { rule: 'machine_identity', field: 'body' });
  assert.equal(checkOutbound(body('the broker is brokerhost.example port 8091'), { names: [], strip: ['brokerhost.example'] }), null);
  // A 3-character user name is not checked (residual R-4).
  os.userInfo = () => ({ username: 'bob', homedir: '/home/carol-dev' });
  assert.equal(checkOutbound(body('bob reviewed it'), { names: [], strip: [] }), null);
});

test('generic account names and a home of /root (FD-4)', async t => {
  for (const username of ['root', 'node']) {
    identity(t, { username, homedir: '/root', home: '/root', hostname: 'localhost' });
    for (const value of ['the project root holds package.json', 'run node --test channel/test', 'src/rootfs/init.mjs']) assert.equal(await check(value), null, `${username}: ${value}`);
  }
  assert.deepEqual(await check('see /root/notes'), { rule: 'machine_identity', field: 'body' });
});

test('the refusal never echoes the match', async () => {
  const result = await check(`root:x:0:0:root:/root:/bin/bash ${shapes.tmc}`);
  assert.deepEqual(Object.keys(result).sort(), ['field', 'rule']);
  assert.ok(!JSON.stringify(result).includes('root:x') && !JSON.stringify(result).includes(shapes.tmc));
});

test('NFKC and format characters', async () => {
  assert.deepEqual(await check('/e\u200btc/passwd'), { rule: 'absolute_path', field: 'body' });
  assert.deepEqual(await check('\uff0fetc\uff0fpasswd'), { rule: 'absolute_path', field: 'body' });
});

test('field classes: names get the content rules, paths refuse any absolute path', async () => {
  assert.deepEqual(await check('.ssh', { cls: 'name', field: 'name' }), { rule: 'sensitive_name', field: 'name' });
  assert.deepEqual(await check(shapes.ghp, { cls: 'name', field: 'to' }), { rule: 'secret', field: 'to' });
  // A name never gets the path rules: a topic with a slash is not a path.
  assert.equal(await check('dev/api', { cls: 'name', field: 'topic' }), null);
  assert.deepEqual(await check('/srcdir/a.js', { cls: 'path', field: 'paths[0]' }), { rule: 'absolute_path', field: 'paths[0]' });
  assert.equal(await check('/srcdir/a.js', { cls: 'free' }), null, 'free text judges the first segment');
});

test('linear on 256 KiB, under 1 s', async () => {
  // FA-9: one worst case per row, 256 KiB each.
  const size = 256 * 1024, fill = unit => unit.repeat(Math.ceil(size / unit.length)).slice(0, size);
  const worst = [
    fill('-----BEGIN A '), fill('a:1:'), fill('ghp_'), fill('://a:'), 'A' + 'TOKEN'.repeat(52428), fill('TOKEN='), fill('A=/x '), fill('A=b\n'),
    fill('ID=x\n'), 'Linux a 1.2 '.repeat(21845), fill('processor : '), fill('MemTotal: '), fill('1: a: <A>'), fill('inet 1.1.1.1 '),
    fill('ii  a '), fill('a-1'), fill('/lib\n'), fill('a'), fill('=/'), fill('/a '), fill('~'), fill('~a'), fill('a=../'), fill('../a/'),
    fill('.ssh'), fill('A'.repeat(199) + ' '), fill('a/'), fill('=a'), fill('\\\\a'), fill('x:\\'), fill('$HOME'),
    // Review r2: mixed-script words, non-ASCII path tokens, invisible fillers, URLs and tildes.
    fill('a\u0430'), fill('a\u0430 '), fill('/\u00e9'), fill('/a\u00e9 '), fill('\u3164a'), fill('http://localhost/'), fill('a://b'), fill('x:///'), fill('~~'), fill('{a,'), fill('-/'), fill('..../'),
  ];
  for (const value of worst) {
    const started = performance.now();
    await check(value);
    const ms = performance.now() - started;
    assert.ok(ms < 1000, `${JSON.stringify(value.slice(0, 16))} took ${Math.round(ms)} ms`);
  }
});

test('AM-S1 static', async () => {
  const { checkOutbound } = await load();
  const source = read('channel/outbound-scope.mjs'), server = read('channel/server.mjs').split('\n');
  assert.ok(!source.includes('process.env'), 'the guard reads no environment');
  assert.equal(checkOutbound.length, 2);
  const guard = server.map((line, i) => [line, i]).filter(([line]) => /^\s*guardOutbound\(request\.params\.name, args\);$/.test(line));
  assert.equal(guard.length, 1, 'exactly one unconditional guard line');
  const [line, at] = guard[0], previous = server[at - 1];
  assert.match(previous, /^\s*const args = parsed\.data;$/, 'right after the parse');
  assert.equal(line.match(/^\s*/)[0], previous.match(/^\s*/)[0], 'at the same indentation, so no condition wraps it');
});

test('the project tree walk and the identity read fail closed', async t => {
  const { checkOutbound, projectDirNames } = await load();
  const { mkdirSync, chmodSync, rmSync } = await import('node:fs');
  const { join } = await import('node:path');
  const { testTempDir } = await import('../../test-support/temp.mjs');
  // No root, or one that does not exist: no names, so every `../` token is refused.
  assert.equal(projectDirNames('relative/root'), null);
  assert.equal(projectDirNames('/nonexistent-scope-root-x'), null);
  assert.deepEqual(checkOutbound([{ field: 'body', value: '../shared/address.mjs', cls: 'free' }], { names: [], strip: [], dirs: null }), { rule: 'parent_escape', field: 'body' });
  // FD-3 caps: an unreadable directory is skipped, and the walk stops at 5000 directories.
  const small = testTempDir('scope-tree-'), big = testTempDir('scope-tree-');
  t.after(() => { chmodSync(join(small, 'locked'), 0o700); for (const dir of [small, big]) rmSync(dir, { recursive: true, force: true }); });
  mkdirSync(join(small, 'locked', 'inner'), { recursive: true }); chmodSync(join(small, 'locked'), 0o000);
  const names = projectDirNames(small);
  assert.ok(names.has('locked') && !names.has('inner'));
  assert.equal(projectDirNames(small), names, 'cached for 60 s');
  for (let i = 0; i < 5010; i++) mkdirSync(join(big, `d${i}`));
  assert.equal(projectDirNames(big).size, 5001, 'the root name plus the first 5000 directories');
  // An empty field list entry and missing exemptions are handled.
  assert.equal(checkOutbound([{ field: 'body', value: '', cls: 'free' }, { field: 'n', value: 7, cls: 'name' }], undefined), null);
  // The identity read: a failing userInfo, an unknown hostname, an empty or root home.
  identity(t, { hostname: undefined });
  os.userInfo = () => { throw new Error('no passwd entry'); };
  os.hostname = () => undefined;
  const saved = os.homedir;
  t.after(() => { os.homedir = saved; });
  for (const home of ['', '/', '/nonexistent-home-x']) {
    os.homedir = () => home;
    assert.equal(checkOutbound([{ field: 'body', value: 'channel/server.mjs', cls: 'free' }], {}), null, JSON.stringify(home));
  }
  os.homedir = () => '/nonexistent-home-x';
  assert.deepEqual(checkOutbound([{ field: 'body', value: 'cat /nonexistent-home-x/notes', cls: 'free' }], {}), { rule: 'machine_identity', field: 'body' });
});

test('budgets keep hostile input linear and fail closed', async () => {
  // More than 4096 relative tokens holding `..` exhaust the parent-escape budget.
  assert.deepEqual(await check('a/../b '.repeat(4100)), { rule: 'parent_escape', field: 'body' });
  // More distinct absolute first segments than the lstat cache holds.
  assert.equal(await check(Array.from({ length: 4200 }, (_, i) => `/zz-scope-${i}`).join(' ')), null);
});

// Review r1 regressions. Each case below was accepted by the r0 guard.
test('S1: a path is judged wherever it starts, not only after the START set', async () => {
  for (const value of ['**/etc/passwd**', '<code>/etc/shadow</code>', 'x|/etc/passwd', 'see >/etc', '#/etc/hosts', '@/etc/hosts', '!/etc/hosts', '&/etc/hosts', '+/etc/hosts', '$/etc/hosts'])
    assert.deepEqual(await check(value), { rule: 'absolute_path', field: 'body' }, value);
  for (const value of ['**../other-repo/a**', 'x|../../other-repo/a']) assert.deepEqual(await check(value), { rule: 'parent_escape', field: 'body' }, value);
  for (const value of ['*~/.bashrc*', 'x|~/notes']) assert.deepEqual(await check(value), { rule: 'home_path', field: 'body' }, value);
  for (const value of ['a/b and/or c', 'src/etc/x.mjs', '</channel>', 'x / 2']) assert.equal(await check(value), null, value);
});

// Review r2 SS2 replaced the r1 look-alike fold with the mixed_script rule,
// so a Cyrillic or Greek letter inside a Latin word reports mixed_script.
test('S2: combining marks, variation selectors and slash look-alikes are folded; look-alike letters are mixed_script', async t => {
  const { user, host } = live();
  for (const mark of ['\u034f', '\ufe0f', '\u180b', '\u20dd', '\u{e0100}'])
    assert.deepEqual(await check(`cat /et${mark}c/passwd`), { rule: 'absolute_path', field: 'body' }, JSON.stringify(mark));
  for (const slash of ['\u2215', '\u2044', '\u29f8', '\u2571']) assert.deepEqual(await check(`cat ${slash}etc${slash}passwd`), { rule: 'absolute_path', field: 'body' }, JSON.stringify(slash));
  assert.deepEqual(await check('cat /\u0435tc/passwd'), { rule: 'mixed_script', field: 'body' }, 'Cyrillic e in /etc');
  assert.deepEqual(await check('cat /\u03bfpt/x'), { rule: 'mixed_script', field: 'body' }, 'Greek omicron in /opt');
  assert.deepEqual(await check(['gh', 'p_', 'Zq9x'.repeat(4), '\ufe0f', 'Zq9x'.repeat(5)].join('')), { rule: 'secret', field: 'body' });
  identity(t, { username: 'carol-dev', hostname: 'buildhost-7' });
  assert.deepEqual(await check('logged in as car\u034fol-dev'), { rule: 'machine_identity', field: 'body' });
  assert.deepEqual(await check('logged in as c\u0430rol-dev'), { rule: 'mixed_script', field: 'body' }, 'Cyrillic a');
  assert.deepEqual(await check('ran on build\u180bhost-7'), { rule: 'machine_identity', field: 'body' });
  assert.ok(user && host);
});

test('S6: quoted or listed shapes and the new network, CPU and kernel shapes are refused', async () => {
  const cases = [
    ['system_file', '> root:x:0:0:root:/root:/bin/bash'], ['system_file', '* daemon:*:19000:0:99999:7:::'],
    ['env_dump', '  | SHELL=/bin/bash'], ['os_release', '> ID=ubuntu\n> VERSION_ID="24.04"'],
    ['cpu_info', '- processor : 0'], ['mem_info', '> MemTotal:       32617016 kB'],
    ['net_config', '| 2: eth0: <BROADCAST,UP> mtu 1500\n|     inet 10.0.0.5/24 brd 10.0.0.255'],
    ['package_listing', '+ ii  adduser  3.137ubuntu1  all  add and remove users'],
    ['net_config', 'the subnet is 172.20.0.0/16'], ['net_config', 'link fe80::1c2b:3cff:fe4d:5e6f/64 scope link'], ['net_config', 'mine is 192.168.1.23'],
    ['net_config', 'reach 10.12.0.7 then'], ['net_config', 'link-local 169.254.10.2'], ['net_config', 'eth0             UP             10.1.2.3/24 fe80::1/64'],
    ['cpu_info', 'Model name:            AMD Ryzen 7 5800X'], ['cpu_info', 'CPU(s):                16'],
    ['kernel_info', 'running 6.8.0-38-generic now'], ['kernel_info', 'kernel 5.14.0-362.8.1.el9_3.x86_64'],
  ];
  for (const [rule, value] of cases) assert.deepEqual(await check(value), { rule, field: 'body' }, value);
  for (const value of ['listen on 127.0.0.1:8091', 'bind 0.0.0.0 port 80', 'version 1.2.3', 'see 10.5 of the spec', 'node 24.21.0']) assert.equal(await check(value), null, value);
});

test('C3: a ../ path to a file name of the project tree passes', async () => {
  for (const value of ["import { x } from '../config.mjs'", 'see ../README.md', 'read ../package.json']) assert.equal(await check(value), null, value);
  assert.deepEqual(await check('see ../not-a-project-file-x.mjs'), { rule: 'parent_escape', field: 'body' });
});

test('C4: code that reads a secret by reference passes; digits are not secrets', async () => {
  for (const value of ['const API_KEY = process.env.API_KEY', 'TOKEN = os.environ["TOKEN"]', 'const ACCESS_TOKEN = getToken()', 'MAX_TOKENS = 128000000', 'SECRET_ARN=config.secrets.arn'])
    assert.equal(await check(value), null, value);
  assert.deepEqual(await check(['DB_PASS', 'WORD=', 'hunter2', 'hunter2'].join('')), { rule: 'secret_assignment', field: 'body' });
});

test('C5: /dev/null, /dev/stdout and /dev/stderr pass exactly', async () => {
  for (const value of ['run it > /dev/null 2>&1', 'log to /dev/stdout', 'or /dev/stderr.']) assert.equal(await check(value), null, value);
  assert.deepEqual(await check('dd of=/dev/sda'), { rule: 'absolute_path', field: 'body' });
});

test('C6: host names that are ordinary words are not identity checks (R-4)', async t => {
  for (const hostname of ['server', 'laptop', 'workstation', 'staging', 'production', 'machine']) {
    identity(t, { hostname, username: 'carol-dev' });
    assert.equal(await check(`deploy to the ${hostname} first`), null, hostname);
  }
});

// Review r2 regressions.
test('SS1: default-ignorable and filler characters are removed before any check', async () => {
  for (const [label, mark] of [['Lo filler U+3164', '\u3164'], ['Lo filler U+FFA0', '\uffa0'], ['Lo filler U+115F', '\u115f'], ['Lo filler U+1160', '\u1160'], ['So braille blank U+2800', '\u2800'],
    ['Mn U+034F', '\u034f'], ['Cf U+2060', '\u2060'], ['VS U+FE0E', '\ufe0e'], ['unassigned default-ignorable U+2065', '\u2065']])
    assert.deepEqual(await check(`cat /et${mark}c/passwd`), { rule: 'absolute_path', field: 'body' }, label);
  assert.equal(await check('ship it \u{1f389}\ufe0f and \u{1f468}\u200d\u{1f4bb}'), null, 'emoji still pass');
});

test('SS2: words that mix Latin with another script, and non-ASCII letters in paths, are refused', async t => {
  identity(t, { username: 'carol-dev', hostname: 'buildhost-7' });
  const cases = [
    ['mixed_script', 'logged in as carol-d\u0435v'], ['mixed_script', 'logged in as c\u0475rol-dev'], ['mixed_script', 'ran on build\u0570ost-7'], ['mixed_script', 'see /\u0570ome/carol-dev'],
    ['non_ascii_path', 'cat /\u1d07tc/passwd'], ['non_ascii_path', 'cat /b\u0131n/sh'], ['non_ascii_path', 'open ~/caf\u00e9/x'],
  ];
  for (const [rule, value] of cases) assert.deepEqual(await check(value), { rule, field: 'body' }, value);
  for (const value of ['\u041f\u0440\u0438\u0432\u0435\u0442, \u043a\u0430\u043a \u0434\u0435\u043b\u0430?', '\u039a\u03b1\u03bb\u03b7\u03bc\u03ad\u03c1\u03b1 \u03ba\u03cc\u03c3\u03bc\u03b5', 'a na\u00efve caf\u00e9 r\u00e9sum\u00e9', 'Hello \u041c\u0438\u0440'])
    assert.equal(await check(value), null, value);
});

test('SS3: a slash after a non-alphanumeric is a path start; scheme URLs are stripped only for remote hosts', async () => {
  for (const value of ['-/etc/passwd', '_/etc/passwd', 'x./etc/passwd', '.../etc/passwd',
    'http://localhost/etc/passwd', 'http://127.0.0.2:80/etc/shadow', 'http://[::1]/etc/hosts', 'vscode://file/etc/passwd', 'foo:///etc/passwd'])
    assert.deepEqual(await check(value), { rule: 'absolute_path', field: 'body' }, value);
  for (const value of ['./src/a', '../shared/x', 'a_b/c', 'https://example.com/etc/passwd', 'http://127.0.0.1:8091', 'see http://localhost:3000/api/users'])
    assert.equal(await check(value), null, value);
});

test('SS4: tilde look-alikes are home paths', async () => {
  for (const value of ['\u223c/.bashrc', '\u02dc/.bashrc', '\uff5e/.bashrc']) assert.deepEqual(await check(value), { rule: 'home_path', field: 'body' }, JSON.stringify(value));
});

test('CC2: references with trailing punctuation pass; a literal secret is still refused', async () => {
  for (const value of ['const API_KEY = process.env.API_KEY;', 'SECRET = settings.SECRET,', 'TOKEN: config.token)']) assert.equal(await check(value), null, value);
  assert.deepEqual(await check(['API_KEY = "s3cr', 'et-value-77";'].join('')), { rule: 'secret_assignment', field: 'body' });
});

test('CC7: a tilde next to another tilde is not a home path', async () => {
  for (const value of ['~~strike~~', '~~old~~, new']) assert.equal(await check(value), null, value);
  for (const value of ['~', '~/x', '~root/x']) assert.deepEqual(await check(value), { rule: 'home_path', field: 'body' }, value);
});

// Final fixes (manager, after review r2).
test('final 1: any non-ASCII code point in a path-like token is non_ascii_path', async () => {
  for (const value of ['cat /e✓tc/passwd', 'cat /et©c/hosts', 'ls /us•r/lib']) assert.deepEqual(await check(value), { rule: 'non_ascii_path', field: 'body' }, JSON.stringify(value));
  assert.deepEqual(await check('cat ∕etc∕passwd'), { rule: 'absolute_path', field: 'body' }, 'mapped slash look-alikes are not counted');
  assert.equal(await check('it costs €5 at a/b'), null, 'a symbol outside any path token passes');
});

test('final 2: a micro sign after a digit is not mixed_script', async t => {
  for (const value of ['latency 5µs', 'a 10μm filter']) assert.equal(await check(value), null, value);
  identity(t, { username: 'carol-dev', hostname: 'buildhost-7' });
  assert.deepEqual(await check('logged in as cаrol-dev'), { rule: 'mixed_script', field: 'body' });
  assert.deepEqual(await check('see μarol'), { rule: 'mixed_script', field: 'body' }, 'a mu not after a digit still counts');
});

test('final 3: a dotted value is a reference only on a known receiver, a call or an index', async () => {
  assert.deepEqual(await check(['PASS', 'WORD=Hunter2.secret'].join('')), { rule: 'secret_assignment', field: 'body' });
  for (const value of ['const API_KEY = process.env.API_KEY;', 'TOKEN = os.environ["TOKEN"]', 'const ACCESS_TOKEN = getToken()', 'SECRET = settings.SECRET,', 'API_KEY: this.apiKey']) assert.equal(await check(value), null, value);
});
