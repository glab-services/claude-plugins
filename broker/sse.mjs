// res.write(false) accepts the frame; it asks the producer to pause until drain.
export function sseWriter(res, { sseBufferBytes, sseDrainTimeout }) {
  const queue = [];
  let queuedBytes = 0;
  let waiting = false;
  let timer;
  const producers = new Set();
  const open = () => !res.destroyed && !res.writableEnded;
  function notify() {
    if (open() && (waiting || queue.length)) return;
    for (const resolve of producers) resolve(open());
    producers.clear();
  }
  function cleanup() {
    clearTimeout(timer);
    res.off('drain', drained);
    queue.length = 0;
    queuedBytes = 0;
    notify();
  }
  function drained() {
    clearTimeout(timer);
    waiting = false;
    flush();
  }
  function flush() {
    while (!waiting && !res.destroyed && !res.writableEnded && queue.length) {
      const { frame, bytes } = queue.shift();
      queuedBytes -= bytes;
      if (!res.write(frame)) {
        waiting = true;
        res.once('drain', drained);
        timer = setTimeout(() => res.destroy(), sseDrainTimeout);
        timer.unref();
      }
    }
    notify();
  }
  res.once('close', cleanup);
  const write = frame => {
    if (res.destroyed || res.writableEnded) return;
    const bytes = Buffer.byteLength(frame);
    // Cap only the in-flight write window; the backlog producer waits on ready().
    if (queuedBytes + res.writableLength + bytes > sseBufferBytes) return res.destroy();
    queue.push({ frame, bytes });
    queuedBytes += bytes;
    flush();
  };
  write.ready = () => {
    if (!open() || (!waiting && !queue.length)) return Promise.resolve(open());
    return new Promise(resolve => producers.add(resolve));
  };
  return write;
}
