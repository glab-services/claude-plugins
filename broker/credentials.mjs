import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

const digest = secret => createHash('sha256').update(secret).digest('hex');
export function mint(prefix) {
  if (!['tmc', 'tmi'].includes(prefix)) throw new Error('INVALID_CREDENTIAL_PREFIX');
  const id = randomBytes(16).toString('hex'), secret = randomBytes(32).toString('base64url');
  return { id, secret, value: `${prefix}_${id}_${secret}`, sha256: digest(secret) };
}
export function parse(prefix, value) {
  if (!['tmc', 'tmi'].includes(prefix) || typeof value !== 'string') return null;
  const match = new RegExp(`^${prefix}_([0-9a-f]{32})_([A-Za-z0-9_-]{43})$`).exec(value);
  return match && match[0] === value ? { id: match[1], secret: match[2] } : null;
}
export function verify(row, secret) {
  if (!row || typeof row.secret_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(row.secret_sha256)) return false;
  return timingSafeEqual(Buffer.from(row.secret_sha256, 'hex'), Buffer.from(digest(secret), 'hex'));
}
export function activeCredential(store, id, user) {
  return Boolean(store.get(`SELECT 1 FROM credentials c JOIN broker_users u ON u.name=c.user
    WHERE c.id=? AND c.user=? AND c.deactivated_at IS NULL AND u.deactivated_at IS NULL`, id, user));
}
export function authenticate(store, staticTokens, header) {
  if (typeof header !== 'string' || !header.startsWith('Bearer ')) return null;
  const value = header.slice(7), user = staticTokens.get(value);
  if (user) return { user, credential_id: null };
  const parsed = parse('tmc', value);
  if (!parsed) return null;
  const row = store.get(`SELECT c.* FROM credentials c JOIN broker_users u ON u.name=c.user
    WHERE c.id=? AND c.deactivated_at IS NULL AND u.deactivated_at IS NULL`, parsed.id);
  return verify(row, parsed.secret) ? { user: row.user, credential_id: row.id } : null;
}
