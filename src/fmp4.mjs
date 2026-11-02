import { EventEmitter } from 'node:events';

export const MAX_BOX_BYTES = 16 * 1024 * 1024; // 16 MiB per ISO BMFF box
export const MAX_BUFFER_BYTES = 32 * 1024 * 1024; // 32 MiB accumulation limit

/**
 * Inspects the next ISO BMFF box header at offset 0 of `buffer`.
 * Returns null if not enough bytes are buffered yet, or `{ type, headerSize, boxSize }`
 * once the box size and 4CC type are known.
 */
export function peekBoxHeader(buffer) {
  if (buffer.length < 8) {
    return null;
  }
  const size32 = buffer.readUInt32BE(0);
  const type = buffer.toString('ascii', 4, 8);
  if (size32 === 1) {
    if (buffer.length < 16) {
      return null;
    }
    const size64 = buffer.readBigUInt64BE(8);
    if (size64 < 16n || size64 > BigInt(MAX_BOX_BYTES)) {
      throw new Error(`Invalid or oversized 64-bit MP4 box "${type}": ${size64}`);
    }
    return { type, headerSize: 16, boxSize: Number(size64) };
  }
  if (size32 < 8 || size32 > MAX_BOX_BYTES) {
    throw new Error(`Invalid or oversized 32-bit MP4 box "${type}": ${size32}`);
  }
  return { type, headerSize: 8, boxSize: size32 };
}

/**
 * Streaming ISO BMFF fMP4 segmenter for HomeKit Secure Video (`CameraRecordingDelegate`).
 *
 * Emits:
 * - `'init'` (Buffer): concatenated `[ftyp, moov]` initialization segment.
 * - `'fragment'` (Buffer): concatenated `[optional prft/sidx, moof, mdat]` media fragment.
 * - `'error'` (Error): if malformed or oversized MP4 boxes are encountered.
 */
export class Fmp4StreamSegmenter extends EventEmitter {
  constructor() {
    super();
    this.buffer = Buffer.alloc(0);
    this.initBoxes = [];
    this.initEmitted = false;
    this.pendingFragmentBoxes = [];
    this.hasMoofInPending = false;
  }

  push(chunk) {
    if (!Buffer.isBuffer(chunk) || chunk.length === 0) {
      return;
    }
    if (this.buffer.length + chunk.length > MAX_BUFFER_BYTES) {
      const err = new Error(`fMP4 stream buffer exceeded ${MAX_BUFFER_BYTES} bytes`);
      this.reset();
      this.emit('error', err);
      return;
    }
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);

    try {
      while (this.buffer.length >= 8) {
        const header = peekBoxHeader(this.buffer);
        if (!header || this.buffer.length < header.boxSize) {
          break;
        }
        const boxBuf = this.buffer.subarray(0, header.boxSize);
        this.buffer = this.buffer.subarray(header.boxSize);
        this.#handleBox(header.type, Buffer.from(boxBuf));
      }
    } catch (err) {
      this.reset();
      this.emit('error', err);
    }
  }

  #handleBox(type, boxBuf) {
    if (!this.initEmitted) {
      this.initBoxes.push(boxBuf);
      if (type === 'moov') {
        const initSegment = Buffer.concat(this.initBoxes);
        this.initBoxes = [];
        this.initEmitted = true;
        this.emit('init', initSegment);
      }
      return;
    }

    if (type === 'ftyp' || type === 'moov') {
      if (type === 'ftyp') {
        this.initBoxes = [boxBuf];
        this.pendingFragmentBoxes = [];
        this.hasMoofInPending = false;
      } else if (type === 'moov') {
        this.initBoxes.push(boxBuf);
        const initSegment = Buffer.concat(this.initBoxes);
        this.initBoxes = [];
        this.emit('init', initSegment);
      }
      return;
    }

    if (type === 'moof') {
      this.hasMoofInPending = true;
      this.pendingFragmentBoxes.push(boxBuf);
      return;
    }

    if (type === 'mdat') {
      if (this.hasMoofInPending) {
        this.pendingFragmentBoxes.push(boxBuf);
        const fragment = Buffer.concat(this.pendingFragmentBoxes);
        this.pendingFragmentBoxes = [];
        this.hasMoofInPending = false;
        this.emit('fragment', fragment);
      }
      return;
    }

    // Auxiliary fragment boxes such as 'prft', 'sidx', 'emsg'
    if (this.pendingFragmentBoxes.length < 8) {
      this.pendingFragmentBoxes.push(boxBuf);
    }
  }

  reset() {
    this.buffer = Buffer.alloc(0);
    this.initBoxes = [];
    this.initEmitted = false;
    this.pendingFragmentBoxes = [];
    this.hasMoofInPending = false;
  }
}

/**
 * Bounded prebuffer and sequence-indexed live fragment hub for HomeKit Secure Video
 * (`handleRecordingStreamRequest`) and simultaneous local `.mp4` clip archiving.
 */
export class PrebufferHub extends EventEmitter {
  constructor({ maxPrebufferFragments = 2, maxRetainedFragments = 32, includeAudio = true, generation = 0 } = {}) {
    super();
    this.maxPrebufferFragments = Math.max(1, maxPrebufferFragments);
    this.maxRetainedFragments = Math.max(this.maxPrebufferFragments + 4, maxRetainedFragments);
    this.includeAudio = Boolean(includeAudio);
    this.generation = generation;
    this.initSegment = null;
    this.fragments = [];
    this.closed = false;
    this.plannedClose = false;
    this.failedClose = false;
    this.sequence = 0;
  }

  setMaxPrebufferFragments(count) {
    this.maxPrebufferFragments = Math.max(1, count);
    this.maxRetainedFragments = Math.max(this.maxPrebufferFragments + 4, this.maxRetainedFragments);
    while (this.fragments.length > this.maxRetainedFragments) {
      this.fragments.shift();
    }
  }

  /**
   * Returns at most `maxPrebufferFragments` most recent fragments for stream start.
   */
  getPrebufferSnapshot() {
    if (this.fragments.length <= this.maxPrebufferFragments) {
      return [...this.fragments];
    }
    return this.fragments.slice(this.fragments.length - this.maxPrebufferFragments);
  }

  hasFragmentsAfter(seq) {
    return this.fragments.some((f) => f.seq > seq);
  }

  onInit(initBuf) {
    this.initSegment = initBuf;
    this.closed = false;
    this.plannedClose = false;
    this.failedClose = false;
    this.emit('init', initBuf);
  }

  onFragment(fragmentBuf) {
    const item = {
      seq: ++this.sequence,
      timestamp: Date.now(),
      data: fragmentBuf,
    };
    this.fragments.push(item);
    while (this.fragments.length > this.maxRetainedFragments) {
      this.fragments.shift();
    }
    this.emit('fragment', item);
  }

  markPlannedEnd() {
    this.plannedClose = true;
    this.failedClose = false;
    this.closed = true;
    this.emit('ended');
  }

  markFailedEnd() {
    if (!this.plannedClose) {
      this.failedClose = true;
    }
    this.closed = true;
    this.emit('ended');
  }

  markEnded({ planned = false } = {}) {
    if (planned) {
      this.markPlannedEnd();
    } else {
      this.markFailedEnd();
    }
  }

  clear() {
    this.initSegment = null;
    this.fragments = [];
    this.markFailedEnd();
  }

  waitForInit(signal, timeoutMs = 8000) {
    if (this.initSegment) {
      return Promise.resolve(this.initSegment);
    }
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        return reject(new Error('Aborted while waiting for fMP4 init segment'));
      }
      if (this.closed) {
        return reject(new Error('Stream ended before fMP4 init segment arrived'));
      }
      let timer = null;
      const cleanup = () => {
        if (timer) clearTimeout(timer);
        this.removeListener('init', onInit);
        this.removeListener('ended', onEnded);
        signal?.removeEventListener('abort', onAbort);
      };
      const onInit = (buf) => {
        cleanup();
        resolve(buf);
      };
      const onEnded = () => {
        cleanup();
        reject(new Error('Stream ended before fMP4 init segment arrived'));
      };
      const onAbort = () => {
        cleanup();
        reject(new Error('Aborted while waiting for fMP4 init segment'));
      };
      timer = setTimeout(() => {
        cleanup();
        reject(new Error(`Timed out after ${timeoutMs}ms waiting for fMP4 init segment`));
      }, timeoutMs);
      this.once('init', onInit);
      this.once('ended', onEnded);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }

  /**
   * Returns the next consecutive fragment with `seq > lastSeq` without dropping burst
   * fragments that arrived before or between awaits. Throws on sequence gap overflow.
   */
  waitForFragmentAfter(lastSeq = 0, signal, timeoutMs = 10000) {
    if (signal?.aborted) {
      return Promise.resolve(null);
    }
    if (lastSeq > 0 && this.fragments.length > 0 && this.fragments[0].seq > lastSeq + 1) {
      return Promise.reject(
        new Error(`Prebuffer fragment sequence gap: expected ${lastSeq + 1}, oldest retained is ${this.fragments[0].seq}`),
      );
    }
    const buffered = this.fragments.find((f) => f.seq > lastSeq);
    if (buffered) {
      return Promise.resolve(buffered);
    }
    if (this.closed) {
      return Promise.resolve(null);
    }

    return new Promise((resolve) => {
      let timer = null;
      const cleanup = () => {
        if (timer) clearTimeout(timer);
        this.removeListener('fragment', onFragment);
        this.removeListener('ended', onEnded);
        signal?.removeEventListener('abort', onAbort);
      };
      const onFragment = (item) => {
        if (item.seq <= lastSeq) return;
        cleanup();
        resolve(item);
      };
      const onEnded = () => {
        cleanup();
        const remaining = this.fragments.find((f) => f.seq > lastSeq) || null;
        resolve(remaining);
      };
      const onAbort = () => {
        cleanup();
        resolve(null);
      };
      timer = setTimeout(() => {
        cleanup();
        resolve(null);
      }, timeoutMs);
      this.on('fragment', onFragment);
      this.once('ended', onEnded);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }

  waitForNextFragment(signal, timeoutMs = 10000) {
    return this.waitForFragmentAfter(this.sequence, signal, timeoutMs);
  }
}
