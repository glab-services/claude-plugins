import { test } from 'node:test';
import assert from 'node:assert/strict';
import { config } from '../config.mjs';

test('webhook configuration rejects URL credentials with a sanitized hint', () => {
  for (const authority of ['user:fixture-password@','user@',':fixture-password@']) {
    assert.throws(() => config({ BROKER_WEBHOOK_URL: `http://${authority}127.0.0.1/hook` }), error => {
      assert.equal(error.code,'INVALID_WEBHOOK_URL');
      assert.match(error.hint,/credentials/);
      assert.equal(JSON.stringify(error).includes('fixture-password'),false);
      return true;
    });
  }
  assert.equal(config({ BROKER_WEBHOOK_URL:'https://example.com/hook' }).webhookUrl,'https://example.com/hook');
});
