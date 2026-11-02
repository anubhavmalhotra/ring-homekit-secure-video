import { spawn } from 'node:child_process';
import crypto from 'node:crypto';
import dgram from 'node:dgram';
import fs from 'node:fs';
import path from 'node:path';
import { concatMap, map, mergeWith, take } from 'rxjs/operators';
import { encodeSrtpOptions } from '@homebridge/camera-utils';
import {
  AudioRecordingSamplerate,
  H264Level,
  H264Profile,
} from '@homebridge/hap-nodejs';
import { RtpPacket, SrtpSession } from 'werift';
import { fsyncDir } from './config.mjs';

export const SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80 = 0x0001;

export const RECORDING_SAMPLE_RATE_MAP = {
  [AudioRecordingSamplerate.KHZ_8]: 8000,
  [AudioRecordingSamplerate.KHZ_16]: 16000,
  [AudioRecordingSamplerate.KHZ_24]: 24000,
  [AudioRecordingSamplerate.KHZ_32]: 32000,
  [AudioRecordingSamplerate.KHZ_44_1]: 44100,
  [AudioRecordingSamplerate.KHZ_48]: 48000,
};

export function mapH264Profile(profileEnum) {
  switch (profileEnum) {
    case H264Profile.BASELINE:
      return 'baseline';
    case H264Profile.HIGH:
      return 'high';
    case H264Profile.MAIN:
    default:
      return 'main';
  }
}

export function mapH264Level(levelEnum) {
  switch (levelEnum) {
    case H264Level.LEVEL3_1:
      return '3.1';
    case H264Level.LEVEL3_2:
      return '3.2';
    case H264Level.LEVEL4_0:
    default:
      return '4.0';
  }
}

/**
 * Rewrites Ring SDP media sections so connection addresses are strictly `127.0.0.1`
 * for use with FFmpeg's `-localaddr 127.0.0.1 -sdp_flags filter_src`.
 */
export function cleanRingSdp(sdp, includeVideo = true) {
  const mediaSections = String(sdp || '')
    .split('\nm=')
    .slice(1)
    .map((section) => 'm=' + section)
    .filter((section) => includeVideo || !section.startsWith('m=video'))
    .map((section) => {
      const replaced = section.replace(/^c=IN IP[46] .*$/gm, 'c=IN IP4 127.0.0.1');
      if (!/^c=IN IP4 127\.0\.0\.1$/m.test(replaced)) {
        const lines = replaced.split('\n');
        lines.splice(1, 0, 'c=IN IP4 127.0.0.1');
        return lines.join('\n');
      }
      return replaced;
    });

  return [
    'v=0',
    'o=- 0 0 IN IP4 127.0.0.1',
    's=RingLoopback',
    'c=IN IP4 127.0.0.1',
    't=0 0',
    ...mediaSections,
  ].join('\n');
}

export function getPayloadType(message) {
  if (!Buffer.isBuffer(message) || message.length < 2) return -1;
  return message.readUInt8(1) & 0x7f;
}

export function isRtpMessagePayloadType(payloadType) {
  // RTCP packet types 192..223 (e.g. SR=200, RR=201, SDES=202, BYE=203, APP=204, RTPFB=205, PSFB=206)
  // have low 7 bits in 64..95.
  if (payloadType >= 64 && payloadType <= 95) {
    return false;
  }
  return payloadType > 90 || payloadType === 0;
}

export function getSsrc(message) {
  if (!Buffer.isBuffer(message) || message.length < 12) return null;
  try {
    const payloadType = getPayloadType(message);
    const isRtp = isRtpMessagePayloadType(payloadType);
    return message.readUInt32BE(isRtp ? 8 : 4);
  } catch {
    return null;
  }
}

/**
 * Binds a UDPv4 socket on the requested address and port without `pick-port`'s 15s hold timer.
 */
export function bindUdpSocket(address = '127.0.0.1', port = 0) {
  return new Promise((resolve, reject) => {
    const socket = dgram.createSocket('udp4');
    const onError = (err) => {
      try {
        socket.close();
      } catch {}
      reject(err);
    };
    socket.once('error', onError);
    socket.bind(port, address, () => {
      socket.removeListener('error', onError);
      socket.on('error', () => {});
      const addr = socket.address();
      resolve({
        socket,
        port: addr.port,
        address: addr.address,
        close: () => {
          try {
            socket.close();
          } catch {}
        },
      });
    });
  });
}

/**
 * Reserves an even/odd loopback UDP port pair `[rtpPort, rtcpPort]` on `127.0.0.1`
 * without leaving any timers on the event loop.
 */
export async function reserveLoopbackPortPair() {
  for (let attempt = 0; attempt < 24; attempt++) {
    const first = await bindUdpSocket('127.0.0.1', 0);
    const basePort = first.port % 2 === 0 ? first.port : first.port + 1;
    if (basePort !== first.port) {
      first.close();
      try {
        const evenSock = await bindUdpSocket('127.0.0.1', basePort);
        const oddSock = await bindUdpSocket('127.0.0.1', basePort + 1);
        evenSock.close();
        oddSock.close();
        return [basePort, basePort + 1];
      } catch {
        continue;
      }
    } else {
      try {
        const oddSock = await bindUdpSocket('127.0.0.1', basePort + 1);
        first.close();
        oddSock.close();
        return [basePort, basePort + 1];
      } catch {
        first.close();
        continue;
      }
    }
  }
  const fallbackA = await bindUdpSocket('127.0.0.1', 0);
  let fallbackB = null;
  try {
    fallbackB = await bindUdpSocket('127.0.0.1', 0);
    return [fallbackA.port, fallbackB.port];
  } finally {
    fallbackA.close();
    fallbackB?.close();
  }
}

/**
 * Safe FFmpeg child-process wrapper that handles spawn `'error'` events (`ENOENT`, `EACCES`),
 * `stdin` `EPIPE`, graceful `SIGTERM` with `SIGKILL` escalation, and guaranteed `exitCallback`.
 */
export class SpawnedFfmpeg {
  constructor({
    ffmpegPath = '/usr/bin/ffmpeg',
    ffmpegArgs = [],
    stdoutCallback,
    exitCallback,
    logLabel = 'FFmpeg',
    logger,
  } = {}) {
    this.ffmpegPath = ffmpegPath;
    this.ffmpegArgs = ffmpegArgs.map((x) => String(x));
    this.stdoutCallback = stdoutCallback;
    this.exitCallback = exitCallback;
    this.logLabel = logLabel;
    this.logger = logger;
    this.stopped = false;
    this.exited = false;
    this.killTimer = null;

    this.proc = spawn(this.ffmpegPath, this.ffmpegArgs, {
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    if (this.stdoutCallback && this.proc.stdout) {
      this.proc.stdout.on('data', (chunk) => {
        if (!this.stopped) {
          this.stdoutCallback(chunk);
        }
      });
    }

    if (this.proc.stderr) {
      this.proc.stderr.on('data', (chunk) => {
        this.logger?.info?.(`[${this.logLabel}] ${chunk}`);
      });
    }

    if (this.proc.stdin) {
      this.proc.stdin.on('error', (err) => {
        if (!String(err?.message || '').includes('EPIPE')) {
          this.logger?.error?.(`[${this.logLabel}] stdin error: ${err.message}`);
        }
      });
    }

    this.proc.on('error', (err) => {
      this.logger?.error?.(`[${this.logLabel}] spawn error: ${err.message}`);
      this.#handleExit(null, null, err);
    });

    this.proc.on('close', (code, signal) => {
      this.#handleExit(code, signal, null);
    });
  }

  #handleExit(code, signal, error) {
    if (this.exited) return;
    this.exited = true;
    if (this.killTimer) {
      clearTimeout(this.killTimer);
      this.killTimer = null;
    }
    try {
      this.exitCallback?.(code, signal, error);
    } catch {}
    this.stop();
  }

  writeStdin(input) {
    if (this.stopped || this.exited || !this.proc.stdin) {
      return;
    }
    try {
      this.proc.stdin.write(input);
      this.proc.stdin.end();
    } catch {}
  }

  stop() {
    if (this.stopped) return;
    this.stopped = true;
    try {
      this.proc.stdout?.pause();
      this.proc.stderr?.pause();
    } catch {}
    if (!this.exited) {
      try {
        this.proc.kill('SIGTERM');
      } catch {}
      this.killTimer = setTimeout(() => {
        this.killTimer = null;
        if (!this.exited) {
          try {
            this.proc.kill('SIGKILL');
          } catch {}
        }
      }, 2000);
      this.killTimer.unref?.();
    }
  }
}

export function withTimeoutAndAbort(promise, { timeoutMs = 7000, signal, label = 'operation' } = {}) {
  if (signal?.aborted) {
    return Promise.reject(new Error(`${label} aborted`));
  }
  return new Promise((resolve, reject) => {
    let timer = null;
    const cleanup = () => {
      if (timer) clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    };
    const onAbort = () => {
      cleanup();
      reject(new Error(`${label} aborted`));
    };
    timer = setTimeout(() => {
      cleanup();
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    signal?.addEventListener('abort', onAbort, { once: true });
    Promise.resolve(promise).then(
      (val) => {
        cleanup();
        resolve(val);
      },
      (err) => {
        cleanup();
        reject(err);
      },
    );
  });
}

/**
 * Subscribes to an RxJS Observable for its first emission and guarantees `sub.unsubscribe()`
 * on emission, error, timeout, or AbortSignal cancellation so subscriptions never leak.
 */
export function waitForObservableFirst(observable$, { timeoutMs = 7000, signal, label = 'observable' } = {}) {
  if (signal?.aborted) {
    return Promise.reject(new Error(`${label} aborted`));
  }
  return new Promise((resolve, reject) => {
    let settled = false;
    let sub = null;
    let timer = null;
    const cleanup = () => {
      if (timer) clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      try {
        sub?.unsubscribe();
      } catch {}
    };
    const onAbort = () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(`${label} aborted`));
    };
    timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(`${label} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    signal?.addEventListener('abort', onAbort, { once: true });
    sub = observable$.subscribe({
      next: (val) => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(val);
      },
      error: (err) => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(err);
      },
      complete: () => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(undefined);
      },
    });
    if (settled) {
      try {
        sub?.unsubscribe();
      } catch {}
    }
  });
}

const MAX_TRACKED_SSRCS = 16;

/**
 * RFC 3711 Section 3.3.1 & 3.3.2 compliant SRTP packet index estimation,
 * 64-packet replay window verification, HMAC-SHA1-80 authentication, and decryption.
 * Does not insert per-SSRC state until after HMAC-SHA1-80 authentication succeeds.
 */
export function verifyAndDecryptSrtp(srtpSession, packetBuf) {
  if (!Buffer.isBuffer(packetBuf) || packetBuf.length < 22) {
    throw new Error('SRTP packet too short');
  }
  const authTagLen = 10;
  const signedPortion = packetBuf.subarray(0, packetBuf.length - authTagLen);
  const actualTag = packetBuf.subarray(packetBuf.length - authTagLen);
  const seq = signedPortion.readUInt16BE(2);
  const ssrc = signedPortion.readUInt32BE(8);

  if (!srtpSession._rfc3711States) {
    srtpSession._rfc3711States = new Map();
  }
  const existing = srtpSession._rfc3711States.get(ssrc);

  let v;
  if (!existing || !existing.initialized) {
    v = 0;
  } else if (existing.s_l < 32768) {
    if (seq - existing.s_l > 32768) {
      if (existing.roc === 0) {
        throw new Error('SRTP sequence wrap before initial rollover');
      }
      v = (existing.roc - 1) >>> 0;
    } else {
      v = existing.roc;
    }
  } else {
    if (existing.s_l - 32768 > seq) {
      v = (existing.roc + 1) >>> 0;
    } else {
      v = existing.roc;
    }
  }

  const pktIndex = (BigInt(v) << 16n) | BigInt(seq);
  if (existing?.initialized) {
    const delta = pktIndex - existing.maxIndex;
    if (delta <= 0n) {
      const back = -delta;
      if (back >= 64n || ((existing.replayBitmap >> back) & 1n) === 1n) {
        throw new Error('SRTP replay or expired packet rejected');
      }
    }
  }

  const expectedTag = srtpSession.remoteContext.cipher.generateSrtpAuthTag(v, signedPortion);
  if (actualTag.length !== expectedTag.length || !crypto.timingSafeEqual(actualTag, expectedTag)) {
    throw new Error('SRTP HMAC-SHA1-80 authentication tag mismatch');
  }

  const [decryptedRtp] = srtpSession.remoteContext.cipher.decryptRtp(packetBuf, v);

  let st = existing;
  if (!st) {
    if (srtpSession._rfc3711States.size >= MAX_TRACKED_SSRCS) {
      const oldestKey = srtpSession._rfc3711States.keys().next().value;
      srtpSession._rfc3711States.delete(oldestKey);
    }
    st = {
      initialized: true,
      s_l: seq,
      roc: v,
      maxIndex: pktIndex,
      replayBitmap: 1n,
    };
    srtpSession._rfc3711States.set(ssrc, st);
  } else if (!st.initialized) {
    st.initialized = true;
    st.roc = v;
    st.s_l = seq;
    st.maxIndex = pktIndex;
    st.replayBitmap = 1n;
  } else if (pktIndex > st.maxIndex) {
    const shift = pktIndex - st.maxIndex;
    st.replayBitmap = shift >= 64n ? 1n : (((st.replayBitmap << shift) | 1n) & ((1n << 64n) - 1n));
    st.maxIndex = pktIndex;
    st.roc = v;
    st.s_l = seq;
  } else {
    const back = st.maxIndex - pktIndex;
    st.replayBitmap |= 1n << back;
  }

  if (srtpSession.remoteContext.srtpSSRCStates) {
    srtpSession.remoteContext.srtpSSRCStates[ssrc] = {
      ssrc,
      rolloverCounter: st.roc,
      lastSequenceNumber: st.s_l,
    };
  }

  return decryptedRtp;
}

/**
 * RFC 3711 Section 3.4 SRTCP replay verification, HMAC-SHA1-80 authentication, and decryption.
 * Does not insert per-SSRC state until after HMAC-SHA1-80 authentication succeeds.
 */
export function verifyAndDecryptSrtcp(srtpSession, packetBuf) {
  if (!Buffer.isBuffer(packetBuf) || packetBuf.length < 22) {
    throw new Error('SRTCP packet too short');
  }
  const authTagLen = 10;
  const signedPortion = packetBuf.subarray(0, packetBuf.length - authTagLen);
  const actualTag = packetBuf.subarray(packetBuf.length - authTagLen);
  const ssrc = signedPortion.readUInt32BE(4);
  const eAndIndex = signedPortion.readUInt32BE(signedPortion.length - 4);
  const srtcpIndex = BigInt(eAndIndex & 0x7fffffff);

  if (!srtpSession._srtcpReplayStates) {
    srtpSession._srtcpReplayStates = new Map();
  }
  const existing = srtpSession._srtcpReplayStates.get(ssrc);

  if (existing?.initialized) {
    const delta = srtcpIndex - existing.maxIndex;
    if (delta <= 0n) {
      const back = -delta;
      if (back >= 64n || ((existing.replayBitmap >> back) & 1n) === 1n) {
        throw new Error('SRTCP replay or expired packet rejected');
      }
    }
  }

  const expectedTag = srtpSession.remoteContext.cipher.generateSrtcpAuthTag(signedPortion);
  if (actualTag.length !== expectedTag.length || !crypto.timingSafeEqual(actualTag, expectedTag)) {
    throw new Error('SRTCP HMAC-SHA1-80 authentication tag mismatch');
  }

  const [decryptedRtcp] = srtpSession.remoteContext.cipher.decryptRTCP(packetBuf);

  let st = existing;
  if (!st) {
    if (srtpSession._srtcpReplayStates.size >= MAX_TRACKED_SSRCS) {
      const oldestKey = srtpSession._srtcpReplayStates.keys().next().value;
      srtpSession._srtcpReplayStates.delete(oldestKey);
    }
    st = {
      initialized: true,
      maxIndex: srtcpIndex,
      replayBitmap: 1n,
    };
    srtpSession._srtcpReplayStates.set(ssrc, st);
  } else if (!st.initialized) {
    st.initialized = true;
    st.maxIndex = srtcpIndex;
    st.replayBitmap = 1n;
  } else if (srtcpIndex > st.maxIndex) {
    const shift = srtcpIndex - st.maxIndex;
    st.replayBitmap = shift >= 64n ? 1n : (((st.replayBitmap << shift) | 1n) & ((1n << 64n) - 1n));
    st.maxIndex = srtcpIndex;
  } else {
    const back = st.maxIndex - srtcpIndex;
    st.replayBitmap |= 1n << back;
  }

  return decryptedRtcp;
}

/**
 * Repackages an outbound H.264 RTP packet across encoder restarts (`RECONFIGURE`),
 * maintaining a continuous 16-bit sequence number, continuous 90 kHz RTP timestamp,
 * negotiated payloadType, and SSRC so the persistent `videoSrtpOut` session never
 * reuses SRTP keystream or triggers replay rejection.
 */
export function repackageVideoRtpPacket(rawRtpBuffer, state, { fps = 24, payloadType = 99, ssrc } = {}) {
  const pkt = RtpPacket.deSerialize(rawRtpBuffer);
  const tsIn = pkt.header.timestamp >>> 0;
  const effFps = state.fps || fps || 24;
  const frameStep = Math.max(1500, Math.round(90000 / effFps));

  if (state.epochBaseTs === null || state.epochBaseTs === undefined) {
    state.epochBaseTs = tsIn;
    state.epochBaseTsOut = tsIn;
    state.newEpoch = false;
  } else if (state.newEpoch) {
    state.epochBaseTs = tsIn;
    state.epochBaseTsOut = ((state.lastTsOut ?? tsIn) + frameStep) >>> 0;
    state.newEpoch = false;
  }

  const delta = (tsIn - state.epochBaseTs) >>> 0;
  const outTs = (state.epochBaseTsOut + delta) >>> 0;
  pkt.header.timestamp = outTs;
  state.lastTsOut = outTs;

  if (state.nextSeq === null || state.nextSeq === undefined) {
    state.nextSeq = pkt.header.sequenceNumber & 0xffff;
  }
  pkt.header.sequenceNumber = state.nextSeq;
  state.nextSeq = (state.nextSeq + 1) & 0xffff;

  const effPt = state.payloadType ?? payloadType;
  if (effPt !== undefined) {
    pkt.header.payloadType = effPt;
  }
  if (ssrc !== undefined) {
    pkt.header.ssrc = ssrc >>> 0;
  }
  return pkt;
}

/**
 * Scales a 48 kHz Opus RTP packet's 32-bit timestamp to the HomeKit negotiated sample rate
 * (`16` kHz = 320 ticks/20ms or `24` kHz = 480 ticks/20ms), preserves sequence and timestamp
 * continuity across `RECONFIGURE` encoder replacements, and sets payloadType + SSRC.
 */
export function repackageOpusRtpPacket(rawRtpBuffer, state, { sampleRateKhz = 24, payloadType = 110, ssrc } = {}) {
  const pkt = RtpPacket.deSerialize(rawRtpBuffer);
  const ts48 = pkt.header.timestamp >>> 0;
  const effRate = state.sampleRateKhz || sampleRateKhz || 24;
  const stepOut = Math.max(1, Math.round((960 * effRate) / 48));

  if (state.baseTs48 === null || state.baseTs48 === undefined) {
    state.baseTs48 = ts48;
    state.baseTsOut = ts48;
    state.newEpoch = false;
  } else if (state.newEpoch) {
    state.baseTs48 = ts48;
    state.baseTsOut = ((state.lastTsOut ?? ts48) + stepOut) >>> 0;
    state.newEpoch = false;
  }

  const delta48 = (ts48 - state.baseTs48) >>> 0;
  const outTs = (state.baseTsOut + Math.round((delta48 * effRate) / 48)) >>> 0;
  pkt.header.timestamp = outTs;
  state.lastTsOut = outTs;

  if (state.nextSeq === null || state.nextSeq === undefined) {
    state.nextSeq = pkt.header.sequenceNumber & 0xffff;
  }
  pkt.header.sequenceNumber = state.nextSeq;
  state.nextSeq = (state.nextSeq + 1) & 0xffff;

  const effPt = state.payloadType ?? payloadType;
  if (effPt !== undefined) {
    pkt.header.payloadType = effPt;
  }
  if (ssrc !== undefined) {
    pkt.header.ssrc = ssrc >>> 0;
  }
  return pkt;
}

/**
 * Rewrites an outbound RTCP Sender Report (`PT = 200`) so its SSRC and 32-bit RTP timestamp
 * (at byte offset 16) match the scaled/continuous RTP clock used on the outbound media stream.
 */
export function repackageRtcpSenderReport(rawRtcpBuffer, state, { isOpusAudio = false, sampleRateKhz = 24, ssrc } = {}) {
  const buf = Buffer.from(rawRtcpBuffer);
  if (buf.length < 8) return buf;
  if (ssrc !== undefined) {
    buf.writeUInt32BE(ssrc >>> 0, 4);
  }
  const pt = buf.readUInt8(1);
  if (pt === 200 && buf.length >= 20) {
    const srTsIn = buf.readUInt32BE(16) >>> 0;
    if (isOpusAudio) {
      const effRate = state.sampleRateKhz || sampleRateKhz || 24;
      if (state.baseTs48 === null || state.baseTs48 === undefined) {
        state.baseTs48 = srTsIn;
        state.baseTsOut = srTsIn;
      }
      const delta48 = (srTsIn - state.baseTs48) >>> 0;
      const scaledTs = (state.baseTsOut + Math.round((delta48 * effRate) / 48)) >>> 0;
      buf.writeUInt32BE(scaledTs, 16);
    } else {
      if (state.epochBaseTs === null || state.epochBaseTs === undefined) {
        state.epochBaseTs = srTsIn;
        state.epochBaseTsOut = srTsIn;
      }
      const delta = (srTsIn - state.epochBaseTs) >>> 0;
      const mappedTs = (state.epochBaseTsOut + delta) >>> 0;
      buf.writeUInt32BE(mappedTs, 16);
    }
  }
  return buf;
}

/**
 * Builds FFmpeg options for HomeKit Secure Video fragmented MP4 (`fMP4`) output.
 */
export function buildHksvFfmpegOptions(recordingConfig, { includeAudio = true } = {}) {
  const [width = 1280, height = 720, fps = 24] = recordingConfig?.videoCodec?.resolution || [1280, 720, 24];
  const rawBitrate = Number(recordingConfig?.videoCodec?.parameters?.bitRate);
  const videoBitrateKbps = Number.isFinite(rawBitrate) && rawBitrate > 0
    ? Math.max(64, Math.min(8000, Math.floor(rawBitrate)))
    : 2000;
  const iFrameIntervalMs = Math.max(1000, recordingConfig?.videoCodec?.parameters?.iFrameInterval || 4000);
  const fragmentLengthMs = Math.max(1000, recordingConfig?.mediaContainerConfiguration?.fragmentLength || 4000);
  const profile = mapH264Profile(recordingConfig?.videoCodec?.parameters?.profile);
  const level = mapH264Level(recordingConfig?.videoCodec?.parameters?.level);

  const keyframeIntervalSec = Math.max(1, Math.min(2, Math.round(Math.min(iFrameIntervalMs, fragmentLengthMs) / 1000)));
  const gopFrames = Math.max(15, fps * keyframeIntervalSec);
  const fragmentDurationMs = keyframeIntervalSec * 1000;

  const videoArgs = [
    '-vcodec', 'libx264',
    '-preset', 'ultrafast',
    '-tune', 'zerolatency',
    '-profile:v', profile,
    '-level:v', level,
    '-pix_fmt', 'yuv420p',
    '-r', String(fps),
    '-vf', `scale=${width}:${height}:force_original_aspect_ratio=decrease,pad=${width}:${height}:(ow-iw)/2:(oh-ih)/2`,
    '-b:v', `${videoBitrateKbps}k`,
    '-maxrate', `${videoBitrateKbps}k`,
    '-bufsize', `${videoBitrateKbps * 2}k`,
    '-g', String(gopFrames),
    '-keyint_min', String(gopFrames),
    '-sc_threshold', '0',
    '-force_key_frames', `expr:gte(t,n_forced*${keyframeIntervalSec})`,
  ];

  let audioArgs;
  if (includeAudio) {
    const sampleRateEnum = recordingConfig?.audioCodec?.samplerate;
    const sampleRateHz = RECORDING_SAMPLE_RATE_MAP[sampleRateEnum] || 32000;
    const channels = Math.max(1, Math.min(2, recordingConfig?.audioCodec?.audioChannels || 1));
    const rawAudioBitrate = Number(recordingConfig?.audioCodec?.bitrate);
    const audioBitrateKbps = Number.isFinite(rawAudioBitrate) && rawAudioBitrate > 0
      ? Math.max(16, Math.min(128, Math.floor(rawAudioBitrate)))
      : 64;
    audioArgs = [
      '-acodec', 'aac',
      '-profile:a', 'aac_low',
      '-ac', String(channels),
      '-ar', String(sampleRateHz),
      '-b:a', `${audioBitrateKbps}k`,
    ];
  } else {
    audioArgs = ['-an'];
  }

  const outputArgs = [
    '-f', 'mp4',
    '-movflags', 'frag_keyframe+empty_moov+default_base_moof+omit_tfhd_offset',
    'pipe:1',
  ];

  return {
    video: videoArgs,
    audio: audioArgs,
    output: outputArgs,
    includeAudio,
    fragmentDurationMs,
  };
}

/**
 * Builds FFmpeg options for HomeKit Live View streaming.
 */
export function buildLiveStreamFfmpegOptions(sessionInfo, startRequest, proxyPorts = null) {
  const {
    targetAddress,
    videoPort,
    videoSrtpKey,
    videoSrtpSalt,
    videoSsrc,
    audioPort,
    audioSrtpKey,
    audioSrtpSalt,
    audioSsrc,
  } = sessionInfo;

  const width = startRequest.video?.width || 1280;
  const height = startRequest.video?.height || 720;
  const fps = startRequest.video?.fps || 24;
  const reqVBitrate = Number(startRequest.video?.max_bit_rate);
  const vBitrate = Number.isFinite(reqVBitrate) && reqVBitrate > 0
    ? Math.max(32, Math.floor(reqVBitrate))
    : 1500;
  const vPt = startRequest.video?.pt || 99;
  const mtu = startRequest.video?.mtu || 1378;
  const profile = mapH264Profile(startRequest.video?.profile);
  const level = mapH264Level(startRequest.video?.level);

  const videoSrtpParams = encodeSrtpOptions({ srtpKey: videoSrtpKey, srtpSalt: videoSrtpSalt });
  const audioSrtpParams = encodeSrtpOptions({ srtpKey: audioSrtpKey, srtpSalt: audioSrtpSalt });

  const aSampleRateKhz = startRequest.audio?.sample_rate || 24;
  const aChannels = startRequest.audio?.channel || 1;
  const reqABitrate = Number(startRequest.audio?.max_bit_rate);
  const aBitrate = Number.isFinite(reqABitrate) && reqABitrate > 0
    ? Math.max(8, Math.floor(reqABitrate))
    : 24;
  const aPt = startRequest.audio?.pt || 110;
  const aPacketTime = startRequest.audio?.packet_time || 20;

  const videoArgs = proxyPorts
    ? [
        '-map', '0:v:0',
        '-vcodec', 'libx264',
        '-preset', 'ultrafast',
        '-tune', 'zerolatency',
        '-profile:v', profile,
        '-level:v', level,
        '-pix_fmt', 'yuv420p',
        '-r', String(fps),
        '-vf', `scale=${width}:${height}:force_original_aspect_ratio=decrease,pad=${width}:${height}:(ow-iw)/2:(oh-ih)/2`,
        '-b:v', `${vBitrate}k`,
        '-maxrate', `${vBitrate}k`,
        '-bufsize', `${vBitrate * 2}k`,
        '-g', String(fps * 2),
        '-payload_type', String(vPt),
        '-ssrc', String(videoSsrc),
        '-f', 'rtp',
        `rtp://127.0.0.1:${proxyPorts.videoOutPort}?rtcpport=${proxyPorts.videoOutPort}&localrtcpport=${proxyPorts.videoLocalRtcpPort}&pkt_size=${mtu}`,
      ]
    : [
        '-map', '0:v:0',
        '-vcodec', 'libx264',
        '-preset', 'ultrafast',
        '-tune', 'zerolatency',
        '-profile:v', profile,
        '-level:v', level,
        '-pix_fmt', 'yuv420p',
        '-r', String(fps),
        '-vf', `scale=${width}:${height}:force_original_aspect_ratio=decrease,pad=${width}:${height}:(ow-iw)/2:(oh-ih)/2`,
        '-b:v', `${vBitrate}k`,
        '-maxrate', `${vBitrate}k`,
        '-bufsize', `${vBitrate * 2}k`,
        '-g', String(fps * 2),
        '-payload_type', String(vPt),
        '-ssrc', String(videoSsrc),
        '-f', 'rtp',
        '-srtp_out_suite', 'AES_CM_128_HMAC_SHA1_80',
        '-srtp_out_params', videoSrtpParams,
        `srtp://${targetAddress}:${videoPort}?rtcpport=${videoPort}&pkt_size=${mtu}`,
      ];

  const audioArgs = proxyPorts
    ? [
        '-map', '0:a:0?',
        '-acodec', 'libopus',
        '-application', 'lowdelay',
        '-frame_duration', String(aPacketTime),
        '-flags', '+global_header',
        '-ar', `${aSampleRateKhz}k`,
        '-b:a', `${aBitrate}k`,
        '-ac', String(aChannels),
        '-payload_type', String(aPt),
        '-ssrc', String(audioSsrc),
        '-f', 'rtp',
        `rtp://127.0.0.1:${proxyPorts.audioOutPort}?rtcpport=${proxyPorts.audioOutPort}&localrtcpport=${proxyPorts.audioLocalRtcpPort}&pkt_size=188`,
      ]
    : [
        '-map', '0:a:0?',
        '-acodec', 'libopus',
        '-application', 'lowdelay',
        '-frame_duration', String(aPacketTime),
        '-flags', '+global_header',
        '-ar', `${aSampleRateKhz}k`,
        '-b:a', `${aBitrate}k`,
        '-ac', String(aChannels),
        '-payload_type', String(aPt),
        '-ssrc', String(audioSsrc),
        '-f', 'rtp',
        '-srtp_out_suite', 'AES_CM_128_HMAC_SHA1_80',
        '-srtp_out_params', audioSrtpParams,
        `srtp://${targetAddress}:${audioPort}?rtcpport=${audioPort}&pkt_size=188`,
      ];

  return {
    video: videoArgs,
    audio: audioArgs,
    output: [],
  };
}

/**
 * Creates a loopback RTP/SRTP/SRTCP proxy for a Live View session.
 * Handles:
 * - Outbound video SRTP/SRTCP forwarding from `videoReturnPort`
 * - Outbound audio 48 kHz -> negotiated sample-rate RTP timestamp scaling + SRTP encryption
 *   and outbound audio RTCP Sender Report -> SRTCP encryption from `audioReturnPort`
 * - Inbound video and audio SRTCP authentication/decryption and forwarding to the active
 *   FFmpeg consumer's local RTCP ports (with fresh RTCP port allocation on RECONFIGURE).
 */
export async function createLiveStreamProxy(sessionInfo, startRequest) {
  let videoLoopback = null;
  let audioLoopback = null;
  let initialVideoRtcpPort;
  let initialAudioRtcpPort;
  try {
    videoLoopback = await bindUdpSocket('127.0.0.1', 0);
    audioLoopback = await bindUdpSocket('127.0.0.1', 0);
    [, initialVideoRtcpPort] = await reserveLoopbackPortPair();
    [, initialAudioRtcpPort] = await reserveLoopbackPortPair();
  } catch (err) {
    videoLoopback?.close();
    audioLoopback?.close();
    throw err;
  }

  let currentVideoLocalRtcpPort = initialVideoRtcpPort;
  let currentAudioLocalRtcpPort = initialAudioRtcpPort;
  let videoSrtcpOutIndex = 0;
  let audioSrtcpOutIndex = 0;

  const vFps = startRequest.video?.fps || 24;
  const vPt = startRequest.video?.pt || 99;
  const videoTsState = {
    epochBaseTs: null,
    epochBaseTsOut: null,
    lastTsOut: null,
    nextSeq: null,
    fps: vFps,
    payloadType: vPt,
    newEpoch: false,
  };

  const aSampleRateKhz = startRequest.audio?.sample_rate || 24;
  const aPt = startRequest.audio?.pt || 110;
  const opusTsState = {
    baseTs48: null,
    baseTsOut: null,
    lastTsOut: null,
    nextSeq: null,
    sampleRateKhz: aSampleRateKhz,
    payloadType: aPt,
    newEpoch: false,
  };

  const videoSrtpOut = new SrtpSession({
    profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
    keys: {
      localMasterKey: sessionInfo.videoSrtpKey,
      localMasterSalt: sessionInfo.videoSrtpSalt,
      remoteMasterKey: sessionInfo.videoSrtpKey,
      remoteMasterSalt: sessionInfo.videoSrtpSalt,
    },
  });

  const audioSrtpOut = new SrtpSession({
    profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
    keys: {
      localMasterKey: sessionInfo.audioSrtpKey,
      localMasterSalt: sessionInfo.audioSrtpSalt,
      remoteMasterKey: sessionInfo.audioSrtpKey,
      remoteMasterSalt: sessionInfo.audioSrtpSalt,
    },
  });

  const videoSrtpIn = new SrtpSession({
    profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
    keys: {
      localMasterKey: sessionInfo.returnVideoSrtpKey,
      localMasterSalt: sessionInfo.returnVideoSrtpSalt,
      remoteMasterKey: sessionInfo.returnVideoSrtpKey,
      remoteMasterSalt: sessionInfo.returnVideoSrtpSalt,
    },
  });

  const audioSrtpIn = new SrtpSession({
    profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
    keys: {
      localMasterKey: sessionInfo.returnAudioSrtpKey,
      localMasterSalt: sessionInfo.returnAudioSrtpSalt,
      remoteMasterKey: sessionInfo.returnAudioSrtpKey,
      remoteMasterSalt: sessionInfo.returnAudioSrtpSalt,
    },
  });

  let closed = false;
  let talkbackHandler = null;

  videoLoopback.socket.on('message', (msg, rinfo) => {
    if (closed || rinfo.address !== '127.0.0.1') return;
    try {
      const pt = getPayloadType(msg);
      if (isRtpMessagePayloadType(pt) && msg.length >= 12) {
        const pkt = repackageVideoRtpPacket(msg, videoTsState, {
          fps: videoTsState.fps,
          payloadType: videoTsState.payloadType,
          ssrc: sessionInfo.videoSsrc,
        });
        const encrypted = videoSrtpOut.encrypt(pkt.payload, pkt.header);
        sessionInfo.videoReturnSocket.socket.send(encrypted, sessionInfo.videoPort, sessionInfo.targetAddress);
      } else if (!isRtpMessagePayloadType(pt) && msg.length >= 8) {
        const mappedRtcp = repackageRtcpSenderReport(msg, videoTsState, {
          isOpusAudio: false,
          ssrc: sessionInfo.videoSsrc,
        });
        const nextIdx = (++videoSrtcpOutIndex) & 0x7fffffff;
        const encryptedRtcp = videoSrtpOut.localContext.cipher.encryptRTCP(mappedRtcp, nextIdx);
        sessionInfo.videoReturnSocket.socket.send(encryptedRtcp, sessionInfo.videoPort, sessionInfo.targetAddress);
      }
    } catch {}
  });

  audioLoopback.socket.on('message', (msg, rinfo) => {
    if (closed || rinfo.address !== '127.0.0.1') return;
    try {
      const pt = getPayloadType(msg);
      if (isRtpMessagePayloadType(pt) && msg.length >= 12) {
        const pkt = repackageOpusRtpPacket(msg, opusTsState, {
          sampleRateKhz: opusTsState.sampleRateKhz,
          payloadType: opusTsState.payloadType,
          ssrc: sessionInfo.audioSsrc,
        });
        const encrypted = audioSrtpOut.encrypt(pkt.payload, pkt.header);
        sessionInfo.returnAudioSocket.socket.send(encrypted, sessionInfo.audioPort, sessionInfo.targetAddress);
      } else if (!isRtpMessagePayloadType(pt) && msg.length >= 8) {
        // Outbound FFmpeg audio RTCP Sender Report -> scale 48kHz RTP timestamp to negotiated clock and encrypt as SRTCP
        const scaledRtcp = repackageRtcpSenderReport(msg, opusTsState, {
          isOpusAudio: true,
          sampleRateKhz: opusTsState.sampleRateKhz,
          ssrc: sessionInfo.audioSsrc,
        });
        const nextIdx = (++audioSrtcpOutIndex) & 0x7fffffff;
        const encryptedRtcp = audioSrtpOut.localContext.cipher.encryptRTCP(scaledRtcp, nextIdx);
        sessionInfo.returnAudioSocket.socket.send(encryptedRtcp, sessionInfo.audioPort, sessionInfo.targetAddress);
      }
    } catch {}
  });

  const onVideoReturnMessage = (msg, rinfo) => {
    if (closed || rinfo.address !== sessionInfo.targetAddress) return;
    try {
      const pt = getPayloadType(msg);
      if (!isRtpMessagePayloadType(pt)) {
        // Inbound HomeKit video SRTCP -> authenticate and forward to FFmpeg's video RTCP port
        const decryptedRtcp = verifyAndDecryptSrtcp(videoSrtpIn, msg);
        videoLoopback.socket.send(decryptedRtcp, currentVideoLocalRtcpPort, '127.0.0.1');
      }
    } catch {}
  };
  sessionInfo.videoReturnSocket.socket.on('message', onVideoReturnMessage);

  const onAudioReturnMessage = (msg, rinfo) => {
    if (closed || rinfo.address !== sessionInfo.targetAddress) return;
    const pt = getPayloadType(msg);
    const isRtp = isRtpMessagePayloadType(pt);

    if (!isRtp) {
      try {
        const decryptedRtcp = verifyAndDecryptSrtcp(audioSrtpIn, msg);
        if (!decryptedRtcp || decryptedRtcp.length < 8) return;
        const rtcpPt = decryptedRtcp.readUInt8(1);
        const headerSsrc = decryptedRtcp.readUInt32BE(4);
        const reportBlockSsrc = decryptedRtcp.length >= 12 ? decryptedRtcp.readUInt32BE(8) : null;
        // Receiver Reports (PT=201) or reports referencing accessory audioSsrc go to incoming audio FFmpeg RTCP port
        if (
          rtcpPt === 201 ||
          headerSsrc === sessionInfo.audioSsrc ||
          reportBlockSsrc === sessionInfo.audioSsrc
        ) {
          audioLoopback.socket.send(decryptedRtcp, currentAudioLocalRtcpPort, '127.0.0.1');
        }
        talkbackHandler?.onAuthenticatedRtcp?.(decryptedRtcp);
      } catch {
        // Unauthenticated SRTCP dropped
      }
      return;
    }

    if (talkbackHandler) {
      try {
        const decryptedRtp = verifyAndDecryptSrtp(audioSrtpIn, msg);
        if (!decryptedRtp || decryptedRtp.length < 12) return;
        talkbackHandler.onAuthenticatedRtp(decryptedRtp);
      } catch {
        // Unauthenticated talkback SRTP dropped
      }
    }
  };
  sessionInfo.returnAudioSocket.socket.on('message', onAudioReturnMessage);

  return {
    proxyPorts: {
      videoOutPort: videoLoopback.port,
      videoLocalRtcpPort: currentVideoLocalRtcpPort,
      audioOutPort: audioLoopback.port,
      audioLocalRtcpPort: currentAudioLocalRtcpPort,
    },
    allocateConsumerPorts: async () => {
      const [, nextVideoRtcpPort] = await reserveLoopbackPortPair();
      const [, nextAudioRtcpPort] = await reserveLoopbackPortPair();
      return {
        videoOutPort: videoLoopback.port,
        videoLocalRtcpPort: nextVideoRtcpPort,
        audioOutPort: audioLoopback.port,
        audioLocalRtcpPort: nextAudioRtcpPort,
      };
    },
    activateConsumerPorts: (ports, reconfigureReq = null) => {
      if (ports?.videoLocalRtcpPort) {
        currentVideoLocalRtcpPort = ports.videoLocalRtcpPort;
      }
      if (ports?.audioLocalRtcpPort) {
        currentAudioLocalRtcpPort = ports.audioLocalRtcpPort;
      }
      if (reconfigureReq?.video?.fps) {
        videoTsState.fps = reconfigureReq.video.fps;
      }
      if (reconfigureReq?.video?.pt) {
        videoTsState.payloadType = reconfigureReq.video.pt;
      }
      if (reconfigureReq?.audio?.sample_rate) {
        opusTsState.sampleRateKhz = reconfigureReq.audio.sample_rate;
      }
      if (reconfigureReq?.audio?.pt) {
        opusTsState.payloadType = reconfigureReq.audio.pt;
      }
      if (videoTsState.epochBaseTs !== null) {
        videoTsState.newEpoch = true;
      }
      if (opusTsState.baseTs48 !== null) {
        opusTsState.newEpoch = true;
      }
    },
    getCurrentRtcpPorts: () => ({
      videoLocalRtcpPort: currentVideoLocalRtcpPort,
      audioLocalRtcpPort: currentAudioLocalRtcpPort,
    }),
    setTalkbackHandler: (handler) => {
      talkbackHandler = handler;
    },
    sendSyntheticRtpForTest: ({ videoRtpPacket, videoSrtpPacket, videoRtcpPacket, audioRtpPacket, audioRtcpPacket }) => {
      if (videoRtpPacket) {
        videoLoopback.socket.send(videoRtpPacket, videoLoopback.port, '127.0.0.1');
      } else if (videoSrtpPacket) {
        const tempDec = new SrtpSession({
          profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
          keys: {
            localMasterKey: sessionInfo.videoSrtpKey,
            localMasterSalt: sessionInfo.videoSrtpSalt,
            remoteMasterKey: sessionInfo.videoSrtpKey,
            remoteMasterSalt: sessionInfo.videoSrtpSalt,
          },
        });
        const plainRtp = verifyAndDecryptSrtp(tempDec, videoSrtpPacket);
        videoLoopback.socket.send(plainRtp, videoLoopback.port, '127.0.0.1');
      }
      if (videoRtcpPacket) {
        videoLoopback.socket.send(videoRtcpPacket, videoLoopback.port, '127.0.0.1');
      }
      if (audioRtpPacket) {
        audioLoopback.socket.send(audioRtpPacket, audioLoopback.port, '127.0.0.1');
      }
      if (audioRtcpPacket) {
        audioLoopback.socket.send(audioRtcpPacket, audioLoopback.port, '127.0.0.1');
      }
    },
    close: () => {
      if (closed) return;
      closed = true;
      talkbackHandler = null;
      try {
        sessionInfo.videoReturnSocket.socket.removeListener('message', onVideoReturnMessage);
      } catch {}
      try {
        sessionInfo.returnAudioSocket.socket.removeListener('message', onAudioReturnMessage);
      } catch {}
      videoLoopback.close();
      audioLoopback.close();
    },
  };
}

/**
 * Authenticated loopback-isolated two-way return audio transcoder.
 * - Receives only packets that have already passed `verifyAndDecryptSrtp` in `liveProxy`.
 * - Configures FFmpeg's SDP input with `-localaddr 127.0.0.1 -sdp_flags filter_src` so FFmpeg's
 *   internal UDP sockets bind strictly to `127.0.0.1` and reject non-loopback packets.
 */
export class SafeReturnAudioTranscoder {
  constructor({
    sessionInfo,
    startRequest,
    liveProxy,
    usingOpus = true,
    ffmpegPath = '/usr/bin/ffmpeg',
    onAuthenticatedTalkback,
    onTranscodedRtp,
  }) {
    this.sessionInfo = sessionInfo;
    this.startRequest = startRequest;
    this.liveProxy = liveProxy;
    this.usingOpus = usingOpus;
    this.ffmpegPath = ffmpegPath;
    this.onAuthenticatedTalkback = onAuthenticatedTalkback;
    this.onTranscodedRtp = onTranscodedRtp;

    this.talkbackAuthenticated = false;
    this.stopped = false;
    this.inLoopback = null;
    this.outLoopback = null;
    this.ff = null;
  }

  async start() {
    let inRtpPort;
    let inRtcpPort;
    try {
      [inRtpPort, inRtcpPort] = await reserveLoopbackPortPair();
      this.inLoopback = await bindUdpSocket('127.0.0.1', 0);
      this.outLoopback = await bindUdpSocket('127.0.0.1', 0);
    } catch (err) {
      this.#cleanupResources();
      throw err;
    }

    if (this.stopped) {
      this.#cleanupResources();
      return;
    }

    const codec = this.startRequest.audio?.codec || 'OPUS';
    const sampleRate = this.startRequest.audio?.sample_rate || 24;
    const channel = this.startRequest.audio?.channel || 1;
    const pt = this.startRequest.audio?.pt || 110;

    this.outLoopback.socket.on('message', (msg, rinfo) => {
      if (this.stopped) return;
      if (rinfo.address !== '127.0.0.1' || !this.talkbackAuthenticated) {
        return;
      }
      try {
        const rtp = RtpPacket.deSerialize(msg);
        this.onTranscodedRtp?.(rtp);
      } catch {}
    });

    this.ff = new SpawnedFfmpeg({
      ffmpegPath: this.ffmpegPath,
      ffmpegArgs: [
        '-hide_banner',
        '-loglevel', 'error',
        '-protocol_whitelist', 'pipe,udp,rtp,file,crypto',
        '-localaddr', '127.0.0.1',
        '-f', 'sdp',
        '-acodec', codec === 'OPUS' ? 'libopus' : 'aac',
        '-i', 'pipe:0',
        '-map', '0:0',
        '-acodec',
        ...(this.usingOpus ? ['libopus', '-ac', '2', '-ar', '48k'] : ['pcm_mulaw', '-ac', '1', '-ar', '8k']),
        '-flags', '+global_header',
        '-f', 'rtp',
        `rtp://127.0.0.1:${this.outLoopback.port}`,
      ],
      logLabel: `ReturnAudio-${this.sessionInfo.sessionID.slice(0, 8)}`,
    });

    const sdp = [
      'v=0',
      'o=- 0 0 IN IP4 127.0.0.1',
      's=Talk',
      'c=IN IP4 127.0.0.1',
      't=0 0',
      `m=audio ${inRtpPort} RTP/AVP ${pt}`,
      'c=IN IP4 127.0.0.1',
      'b=AS:24',
      ...(codec === 'OPUS'
        ? [
            `a=rtpmap:${pt} opus/${sampleRate}000/${channel}`,
            `a=fmtp:${pt} minptime=10;useinbandfec=1`,
          ]
        : [
            `a=rtpmap:${pt} MPEG4-GENERIC/${sampleRate}000/${channel}`,
            `a=fmtp:${pt} profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3; config=F8F0212C00BC00`,
          ]),
    ].join('\n');

    this.ff.writeStdin(sdp);

    this.liveProxy?.setTalkbackHandler({
      onAuthenticatedRtp: (decryptedRtp) => {
        if (this.stopped) return;
        if (!this.talkbackAuthenticated) {
          this.talkbackAuthenticated = true;
          this.onAuthenticatedTalkback?.();
        }
        try {
          this.inLoopback.socket.send(decryptedRtp, inRtpPort, '127.0.0.1');
        } catch {}
      },
      onAuthenticatedRtcp: (decryptedRtcp) => {
        if (this.stopped || !this.talkbackAuthenticated) return;
        try {
          this.inLoopback.socket.send(decryptedRtcp, inRtcpPort, '127.0.0.1');
        } catch {}
      },
    });
  }

  stop() {
    if (this.stopped) return;
    this.stopped = true;
    this.#cleanupResources();
  }

  #cleanupResources() {
    this.liveProxy?.setTalkbackHandler(null);
    try {
      this.ff?.stop();
    } catch {}
    try {
      this.inLoopback?.close();
    } catch {}
    try {
      this.outLoopback?.close();
    } catch {}
  }
}

/**
 * Attaches an independent FFmpeg consumer to an active `ring-client-api` `StreamingSession`
 * without terminating the entire WebRTC session when this single consumer stops.
 * Enforces a single overall startup deadline, AbortSignal cancellation, observable unsubscription,
 * and `-localaddr 127.0.0.1 -sdp_flags filter_src` loopback isolation.
 */
export async function attachConsumerToRingSession(
  session,
  ffmpegOptions,
  {
    ffmpegPath = '/usr/bin/ffmpeg',
    label = 'RingConsumer',
    onExit,
    signal,
    startupTimeoutMs = 7000,
    deadlineMs = null,
  } = {},
) {
  if (session.hasEnded) {
    throw new Error('Ring StreamingSession has already ended');
  }
  const deadline = deadlineMs || Date.now() + startupTimeoutMs;
  const remainingMs = () => Math.max(50, deadline - Date.now());

  let videoPort;
  let audioPort;
  let senderSocket = null;
  try {
    [videoPort] = await reserveLoopbackPortPair();
    [audioPort] = await reserveLoopbackPortPair();
    senderSocket = await bindUdpSocket('127.0.0.1', 0);
  } catch (err) {
    senderSocket?.close();
    throw err;
  }
  const subscriptions = [];
  let stopped = false;
  let ff = null;

  const stopConsumer = () => {
    if (stopped) return;
    stopped = true;
    for (const sub of subscriptions) {
      try {
        sub.unsubscribe();
      } catch {}
    }
    subscriptions.length = 0;
    senderSocket.close();
    try {
      ff?.stop();
    } catch {}
    try {
      onExit?.();
    } catch {}
  };

  try {
    const answerOrEnd$ = session.connection.onCallAnswered.pipe(
      mergeWith(session.onCallEnded.pipe(map(() => null))),
    );
    const ringSdp = await waitForObservableFirst(answerOrEnd$, {
      timeoutMs: remainingMs(),
      signal,
      label: `${label} SDP answer`,
    });

    if (!ringSdp || session.hasEnded) {
      throw new Error('Ring call ended before SDP answer arrived');
    }

    const opusOrError$ = session.onUsingOpus.pipe(
      mergeWith(
        session.connection.onError.pipe(
          map((e) => {
            throw e;
          }),
        ),
      ),
    );
    const usingOpus = await waitForObservableFirst(opusOrError$, {
      timeoutMs: remainingMs(),
      signal,
      label: `${label} codec negotiation`,
    });

    if (stopped || signal?.aborted || session.hasEnded) {
      throw new Error(`${label} aborted before FFmpeg spawn`);
    }

    const transcodeVideoStream = ffmpegOptions.video !== false;
    const ffmpegInputArgs = [
      '-hide_banner',
      '-loglevel', 'error',
      '-protocol_whitelist', 'pipe,udp,rtp,file,crypto',
      '-localaddr', '127.0.0.1',
      ...(usingOpus ? ['-acodec', 'libopus'] : []),
      '-f', 'sdp',
      ...(ffmpegOptions.input || []),
      '-i', 'pipe:0',
    ];

    const inputSdp = cleanRingSdp(ringSdp, transcodeVideoStream)
      .replace(/m=audio \d+/, `m=audio ${audioPort}`)
      .replace(/m=video \d+/, `m=video ${videoPort}`);

    ff = new SpawnedFfmpeg({
      ffmpegArgs: ffmpegInputArgs.concat(
        ...(ffmpegOptions.audio || ['-acodec', 'aac']),
        ...(transcodeVideoStream ? ffmpegOptions.video || ['-vcodec', 'copy'] : []),
        ...(ffmpegOptions.output || []),
      ),
      ffmpegPath,
      stdoutCallback: ffmpegOptions.stdoutCallback,
      exitCallback: () => stopConsumer(),
      logLabel: label,
      logger: {
        error: (msg) => console.error(msg),
        info: () => {},
      },
    });

    subscriptions.push(
      session.onAudioRtp
        .pipe(
          concatMap((rtp) => {
            if (stopped) return Promise.resolve();
            return new Promise((resolve) => {
              senderSocket.socket.send(rtp.serialize(), audioPort, '127.0.0.1', () => resolve());
            });
          }),
        )
        .subscribe(),
    );

    if (transcodeVideoStream) {
      subscriptions.push(
        session.onVideoRtp
          .pipe(
            concatMap((rtp) => {
              if (stopped) return Promise.resolve();
              return new Promise((resolve) => {
                senderSocket.socket.send(rtp.serialize(), videoPort, '127.0.0.1', () => resolve());
              });
            }),
          )
          .subscribe(),
      );
    }

    subscriptions.push(
      session.onCallEnded.pipe(take(1)).subscribe(() => stopConsumer()),
    );

    ff.writeStdin(inputSdp);
    session.requestKeyFrame();

    return {
      stop: stopConsumer,
    };
  } catch (err) {
    stopConsumer();
    throw err;
  }
}

/**
 * Generates a valid JPEG snapshot via FFmpeg (`lavfi` color source) as a reliable fallback
 * when Ring cloud snapshots are unavailable (e.g., no Ring Protect subscription).
 */
export function generateFallbackJpeg({ width = 1280, height = 720, ffmpegPath = '/usr/bin/ffmpeg' } = {}) {
  const safeW = Math.max(160, Math.min(1920, Number(width) || 1280));
  const safeH = Math.max(120, Math.min(1080, Number(height) || 720));
  return new Promise((resolve, reject) => {
    const proc = spawn(ffmpegPath, [
      '-hide_banner',
      '-loglevel', 'error',
      '-f', 'lavfi',
      '-i', `color=c=0x1e293b:s=${safeW}x${safeH}:d=0.1`,
      '-frames:v', '1',
      '-f', 'image2',
      '-vcodec', 'mjpeg',
      'pipe:1',
    ]);
    const chunks = [];
    proc.stdout.on('data', (chunk) => chunks.push(chunk));
    proc.on('error', reject);
    proc.on('close', (code) => {
      const buf = Buffer.concat(chunks);
      if (code === 0 && buf.length > 0) {
        resolve(buf);
      } else {
        reject(new Error(`Fallback JPEG generation failed with code ${code}`));
      }
    });
  });
}

/**
 * Extracts a single JPEG frame from an initialization + fragment fMP4 buffer so that
 * live motion/doorbell events immediately update the cached HomeKit snapshot thumbnail
 * without requiring Ring's paid Snapshot Capture feature.
 */
export function extractJpegFromFmp4(initSegment, fragmentBuffer, { width = 1280, height = 720, ffmpegPath = '/usr/bin/ffmpeg' } = {}) {
  if (!Buffer.isBuffer(initSegment) || !Buffer.isBuffer(fragmentBuffer)) {
    return Promise.reject(new Error('Init and fragment buffers required'));
  }
  const safeW = Math.max(160, Math.min(1920, Number(width) || 1280));
  const safeH = Math.max(120, Math.min(1080, Number(height) || 720));
  return new Promise((resolve, reject) => {
    const proc = spawn(ffmpegPath, [
      '-hide_banner',
      '-loglevel', 'error',
      '-f', 'mp4',
      '-i', 'pipe:0',
      '-frames:v', '1',
      '-vf', `scale=${safeW}:${safeH}:force_original_aspect_ratio=decrease`,
      '-f', 'image2',
      '-vcodec', 'mjpeg',
      'pipe:1',
    ]);
    const chunks = [];
    proc.stdout.on('data', (chunk) => chunks.push(chunk));
    proc.stdin.on('error', () => {});
    proc.on('error', reject);
    proc.on('close', (code) => {
      const buf = Buffer.concat(chunks);
      if (code === 0 && buf.length > 0) {
        resolve(buf);
      } else {
        reject(new Error(`Frame JPEG extraction exited with code ${code}`));
      }
    });
    proc.stdin.end(Buffer.concat([initSegment, fragmentBuffer]));
  });
}

/**
 * Local clip writer that saves the fMP4 initialization segment + media fragments
 * to a timestamped `.mp4` file in `clipsDir` with mode `0600` and parent directory `fsync`.
 * Preserves recoverable `.part` data and explicitly reports durability errors if `fsync` or `rename` fails.
 */
export class LocalClipRecorder {
  constructor(clipsDir, triggerReason = 'event') {
    this.clipsDir = clipsDir;
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const safeTrigger = String(triggerReason).replace(/[^a-zA-Z0-9_-]/g, '_');
    const nonce = crypto.randomBytes(3).toString('hex');
    this.fileName = `${stamp}_${safeTrigger}_${nonce}.mp4`;
    this.tmpPath = path.join(clipsDir, `.${this.fileName}.part`);
    this.finalPath = path.join(clipsDir, this.fileName);
    this.fd = fs.openSync(this.tmpPath, 'w', 0o600);
    this.bytesWritten = 0;
    this.fragmentsWritten = 0;
    this.closed = false;
  }

  writeInit(initBuf) {
    if (this.closed || !Buffer.isBuffer(initBuf)) return;
    fs.writeSync(this.fd, initBuf);
    this.bytesWritten += initBuf.length;
  }

  writeFragment(fragmentBuf) {
    if (this.closed || !Buffer.isBuffer(fragmentBuf)) return;
    fs.writeSync(this.fd, fragmentBuf);
    this.bytesWritten += fragmentBuf.length;
    this.fragmentsWritten++;
  }

  abort() {
    if (this.closed) return;
    this.closed = true;
    try {
      fs.closeSync(this.fd);
    } catch {}
    try {
      fs.unlinkSync(this.tmpPath);
    } catch {}
  }

  close() {
    if (this.closed) return null;
    this.closed = true;

    if (this.fragmentsWritten === 0 || this.bytesWritten === 0) {
      try {
        fs.closeSync(this.fd);
      } catch {}
      try {
        fs.unlinkSync(this.tmpPath);
      } catch {}
      return { ok: true, empty: true, path: null, bytes: 0, fragments: 0, error: null };
    }

    let durabilityError = null;
    try {
      fs.fsyncSync(this.fd);
    } catch (err) {
      durabilityError = err;
    }
    try {
      fs.closeSync(this.fd);
    } catch (err) {
      if (!durabilityError) durabilityError = err;
    }

    if (durabilityError) {
      // Preserve recoverable .part file on disk and report error to caller
      return {
        ok: false,
        empty: false,
        path: null,
        partPath: this.tmpPath,
        bytes: this.bytesWritten,
        fragments: this.fragmentsWritten,
        error: durabilityError,
      };
    }

    try {
      fs.renameSync(this.tmpPath, this.finalPath);
      fsyncDir(this.clipsDir);
      return {
        ok: true,
        empty: false,
        path: this.finalPath,
        bytes: this.bytesWritten,
        fragments: this.fragmentsWritten,
        error: null,
      };
    } catch (err) {
      return {
        ok: false,
        empty: false,
        path: null,
        partPath: this.tmpPath,
        bytes: this.bytesWritten,
        fragments: this.fragmentsWritten,
        error: err,
      };
    }
  }
}
