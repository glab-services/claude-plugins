import { ChannelError } from './config.mjs';

export async function* events(response, maxFrameBytes, { onChunk = () => {} } = {}) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = '';
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (!done && value.byteLength) onChunk(); // Heartbeat comments count even though they emit no message event.
      pending += done ? decoder.decode() : decoder.decode(value, { stream: true });
      let match;
      while ((match = /\r?\n\r?\n/.exec(pending))) {
        const frame = pending.slice(0, match.index);
        if (Buffer.byteLength(frame) > maxFrameBytes) throw new ChannelError('FRAME_TOO_LARGE', 'Increase CHANNEL_MAX_FRAME_BYTES to match the broker message limit.');
        pending = pending.slice(match.index + match[0].length);
        const lines = frame.split(/\r?\n/);
        const data = lines.filter(line => line.startsWith('data:')).map(line => line.slice(5).replace(/^ /, '')).join('\n');
        if (data) yield { event: lines.find(line => line.startsWith('event:'))?.slice(6).trim() ?? 'message', data: JSON.parse(data) };
      }
      if (Buffer.byteLength(pending) > maxFrameBytes) throw new ChannelError('FRAME_TOO_LARGE', 'Increase CHANNEL_MAX_FRAME_BYTES to match the broker message limit.');
      if (done) break;
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
