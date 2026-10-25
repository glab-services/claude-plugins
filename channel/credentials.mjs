import { accessSync, constants, mkdirSync, lstatSync, openSync, fstatSync, readFileSync, writeFileSync, fsyncSync, closeSync, linkSync, unlinkSync } from 'node:fs';
import { basename, dirname, join, resolve, parse, sep, isAbsolute } from 'node:path';
import { realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { createHash, randomBytes } from 'node:crypto';
import { ChannelError, requireValue } from './config.mjs';

export function credentialPath(url, user, { env = process.env, home = env.HOME || homedir() } = {}) {
  requireValue(typeof user === 'string' && /^[a-zA-Z0-9_.-]{1,128}$/.test(user) && !/\s/.test(user) && user !== 'broker', 'NAME_REQUIRED', 'Pass --name with a valid developer name other than broker.');
  return join(env.XDG_CONFIG_HOME || join(home, '.config'), 'team-messaging', 'credentials', `${createHash('sha256').update(url).digest('hex').slice(0,16)}-${user}.token`);
}
// Plan project-options AM-2: the plugin options are user-editable, and a
// cloned repo's settings.json env block can set them while they are unsaved
// (findings U-2). A token file is therefore used only when it is exactly the
// path join writes for this normalised broker URL and the name in its
// basename, checked before anything is read or sent. A repo-chosen URL can
// then only reach a credential the user minted for that same URL. Returns the
// name; hand-named token files are refused on purpose.
export function credentialName(url, path, { env = process.env, home } = {}) {
  const name = typeof path === 'string' ? /^[0-9a-f]{16}-([a-zA-Z0-9_.-]{1,128})\.token$/.exec(basename(path))?.[1] : undefined;
  let expected;
  try { if (name !== undefined) expected = credentialPath(url, name, { env, ...(home ? { home } : {}) }); } catch {}
  requireValue(expected !== undefined && expected === path, 'TOKEN_FILE_MISMATCH', 'The token file is not the credential join saved for this broker URL; run /team-messaging:join or /team-messaging:bind again.');
  return name;
}
// Decision 146, narrowed by decision 149: a directory no other non-root user
// can traverse (a real directory of our own uid with no group or world bits)
// seals everything below it, so a group- or world-writable directory under it
// cannot be reached by anyone else. Claude Code leaves its plugin data
// directories 0775 under umask 002 below a private ~/.claude. This applies
// only to watcher state (stateDir passes `sealedAncestors`). Credential paths
// stay strict: a process that kept a cwd or open fd below the seal could still
// rename entries, and Node has no openat to close that race for token writes.
// For watcher state the worst case is a missed or repeated wake.
const sealed=stat=>stat.isDirectory()&&stat.uid===process.getuid()&&(stat.mode&0o077)===0;
// Decision 148: the refusal names the directory and its fix. The path is
// single-quoted for the shell, with any quote inside it escaped.
function writableError(dir) {
  const error=new ChannelError('INSECURE_CREDENTIAL_DIR',`${dir} is group- or world-writable; run chmod go-w '${dir.replaceAll("'","'\\''")}'`);
  error.dir=dir;return error;
}
// With `sealedAncestors`, `pending` holds the writable directories below `dir`
// that still need a sealed ancestor. They are carried into a symlink's
// resolved chain, because that is where they physically live: a sealed
// directory above the symlink does not protect its target.
function ancestors(dir,home,sealedAncestors=false,seen=new Set(),pending=[]) {
  const selectedHome=home||process.env.HOME||homedir(),absolute=resolve(dir),root=parse(absolute).root;
  const boundary=isAbsolute(selectedHome)&&(absolute===resolve(selectedHome)||absolute.startsWith(resolve(selectedHome)+sep))?resolve(selectedHome):root;
  for(let path=absolute;;path=dirname(path)) {
    const stat=lstatSync(path,{throwIfNoEntry:false});
    if(stat?.isSymbolicLink()) {
      if(!seen.has(path)) {seen.add(path);requireValue(stat.uid===0,'INSECURE_CREDENTIAL_DIR','Credential ancestors must not contain user-owned symlinks.');}
      ancestors(realpathSync(path),home,sealedAncestors,seen,pending);pending=[];
    } else if(stat) {
      if(!seen.has(path)) {
        seen.add(path);requireValue(stat.isDirectory()&&(stat.uid===0||stat.uid===process.getuid()),
          'INSECURE_CREDENTIAL_DIR','Credential ancestors must be owned by your user or root and not group/world-writable.');
      }
      if(sealedAncestors&&sealed(stat))pending=[];
      else if((stat.mode&0o022)!==0) {
        // The strict rule refuses the first writable directory at once.
        if(!sealedAncestors)throw writableError(path);
        pending.push(path);
      }
    }
    if(path===boundary||path===root)break;
  }
  // The lowest unsealed writable directory is named: the one closest to the state.
  if(pending.length)throw writableError(pending[0]);
}
// `sealedAncestors` is for watcher state only (decision 149); every
// credential directory uses the default strict ancestor rule.
export function ensurePrivateDir(dir, { mkdir = mkdirSync, home, sealedAncestors = false } = {}) {
  try {
    ancestors(dirname(dir),home,sealedAncestors);
    mkdir(dir, { recursive: true, mode: 0o700 });
    ancestors(dirname(dir),home,sealedAncestors);
    const stat = lstatSync(dir);
    // Review-checked guard: directory ownership must be the current uid.
    requireValue(stat.isDirectory() && !stat.isSymbolicLink() && stat.uid === process.getuid() && (stat.mode & 0o077) === 0,
      'INSECURE_CREDENTIAL_DIR', 'Use a private directory owned by your user with mode 0700 and no symlink.');
  } catch (error) {
    if (error instanceof ChannelError) throw error;
    throw new ChannelError('INSECURE_CREDENTIAL_DIR', 'Use a writable private credential directory owned by your user.');
  }
}
export function preflightCredentialDir(dir,options) {
  ensurePrivateDir(dir,options);
  try {
    requireValue((lstatSync(dir).mode & 0o200) !== 0, 'CREDENTIAL_DIR_NOT_WRITABLE', 'Make the private credential directory writable before joining.');
    accessSync(dir, constants.W_OK);
  } catch {
    throw new ChannelError('CREDENTIAL_DIR_NOT_WRITABLE', 'Make the private credential directory writable before joining.');
  }
}
export function writeCredential(path, token, { open = openSync, home } = {}) {
  const dir = dirname(path); ensurePrivateDir(dir,{home});
  const tmp = `${path}.${randomBytes(16).toString('hex')}.tmp`;
  let fd, linked = false;
  try {
    fd = open(tmp, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
    writeFileSync(fd, token);
    // Review-checked guard: sync the bytes before publishing their name.
    fsyncSync(fd); closeSync(fd); fd = undefined;
    try { linkSync(tmp, path); linked = true; }
    catch (error) { if (error.code === 'EEXIST') throw new ChannelError('CREDENTIAL_EXISTS', 'The credential already exists; reuse it without overwriting.'); throw error; }
    // Review-checked guard: directory fsync makes the published entry durable.
    const directory = openSync(dir, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    try { fsyncSync(directory); } finally { closeSync(directory); }
  } catch (error) {
    if (linked) { try { unlinkSync(path); } catch {} }
    if (error instanceof ChannelError) throw error;
    throw new ChannelError('CREDENTIAL_WRITE_FAILED', 'Could not durably save the credential in its private directory.');
  } finally {
    if (fd !== undefined) closeSync(fd);
    try { unlinkSync(tmp); } catch (error) { if (error.code !== 'ENOENT') throw new ChannelError('CREDENTIAL_WRITE_FAILED', 'Could not clean up a temporary credential file.'); }
  }
}
export function readCredential(path,{home}={}) {
  let fd;
  try {
    fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    ancestors(dirname(path),home);
    const stat = fstatSync(fd);
    const current=lstatSync(path);
    // Review-checked guard: verify uid on this exact open handle.
    requireValue(stat.isFile() && stat.uid === process.getuid() && (stat.mode & 0o777) === 0o600 &&
      current.isFile() && current.dev===stat.dev && current.ino===stat.ino,
      'INSECURE_TOKEN_FILE', 'Use a regular token file owned by your user with mode 0600, never a symlink.');
    return readFileSync(fd, 'utf8');
  } catch (error) {
    if (error instanceof ChannelError) throw error;
    throw new ChannelError('INSECURE_TOKEN_FILE', 'Use a readable regular token file owned by your user with mode 0600, never a symlink.');
  } finally { if (fd !== undefined) closeSync(fd); }
}
