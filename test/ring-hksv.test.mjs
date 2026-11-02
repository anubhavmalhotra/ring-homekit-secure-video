import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { ReplaySubject, Subject } from 'rxjs';
import {
  Characteristic,
  HDSProtocolError,
  Service,
  StreamRequestTypes,
} from '@homebridge/hap-nodejs';
import { RtpHeader, RtpPacket, SrtpSession } from 'werift';
import {
  FORBIDDEN_PINCODES,
  acquireStateDirLock,
  loadOrCreateConfig,
  pruneClips,
  validateConfig,
  writeJsonAtomic,
} from '../src/config.mjs';
import {
  Fmp4StreamSegmenter,
  MAX_BOX_BYTES,
  PrebufferHub,
  peekBoxHeader,
} from '../src/fmp4.mjs';
import {
  LocalClipRecorder,
  SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
  SpawnedFfmpeg,
  attachConsumerToRingSession,
  bindUdpSocket,
  buildHksvFfmpegOptions,
  cleanRingSdp,
  repackageOpusRtpPacket,
  repackageRtcpSenderReport,
  repackageVideoRtpPacket,
  verifyAndDecryptSrtp,
  verifyAndDecryptSrtcp,
  waitForObservableFirst,
  withTimeoutAndAbort,
} from '../src/media-pipeline.mjs';
import { RingCameraMediaSource, RingHksvDoorbell } from '../src/hksv-delegate.mjs';
import { SyntheticFfmpegMediaSource } from '../src/synthetic-source.mjs';
import { selectRingCamera, startService } from '../src/service.mjs';
import { authenticateAndPersistRing, parseCliArgs } from '../src/cli.mjs';

function makeBox(type, payload = Buffer.alloc(0), use64Bit = false) {
  assert.equal(type.length, 4);
  if (use64Bit) {
    const header = Buffer.alloc(16);
    header.writeUInt32BE(1, 0);
    header.write(type, 4, 4, 'ascii');
    header.writeBigUInt64BE(BigInt(16 + payload.length), 8);
    return Buffer.concat([header, payload]);
  }
  const header = Buffer.alloc(8);
  header.writeUInt32BE(8 + payload.length, 0);
  header.write(type, 4, 4, 'ascii');
  return Buffer.concat([header, payload]);
}

const SAMPLE_SELECTED_CONFIG_BASE64 =
  'AR0BBKAPAAACCAEAAAAAAAAAAwsBAQACBgEEoA8AAAIkAQEAAhIBAQECAQIDBNAHAAAEBKAPAAADCwECsAQCAkAGAwEYAxQBAQACDwEBAQIBAAMBAwQEQAAAAA==';

class DeterministicTestMediaSource {
  constructor({ startupDelayMs = 10, failFirstAttach = false } = {}) {
    this.name = 'Front Door';
    this.model = 'Ring Video Doorbell Pro';
    this.serialNumber = 'RING-TEST-001';
    this.firmwareRevision = '2.4.0';
    this.hasBattery = true;
    this.batteryLevel = 88;
    this.hasLowBattery = false;
    this.isCharging = true;
    this.startupDelayMs = startupDelayMs;
    this.failFirstAttach = failFirstAttach;
    this.attachedConsumers = new Map();
    this.attachCallCount = 0;
    this.speakerActivations = 0;
    this.talkbackPackets = [];
  }

  startEventListeners() {}
  stopEventListeners() {}

  async attachFfmpeg(consumerId, ffmpegOptions, { onExit } = {}) {
    const callIdx = ++this.attachCallCount;
    await new Promise((r) => setTimeout(r, this.startupDelayMs));
    if (this.failFirstAttach && callIdx === 1) {
      throw new Error('Simulated first attachFfmpeg failure');
    }

    let stopped = false;
    const ftyp = makeBox('ftyp', Buffer.from('isomiso2avc1mp41'));
    const moov = makeBox('moov', Buffer.from('mock_moov_header_bytes'));
    const moof1 = makeBox('moof', Buffer.from('mock_moof_seq_1'));
    const mdat1 = makeBox('mdat', crypto.randomBytes(256));
    const moof2 = makeBox('moof', Buffer.from('mock_moof_seq_2'));
    const mdat2 = makeBox('mdat', crypto.randomBytes(256));

    const timer1 = setTimeout(() => {
      if (stopped) return;
      ffmpegOptions.stdoutCallback?.(Buffer.concat([ftyp, moov, moof1, mdat1]));
    }, 10);

    const timer2 = setTimeout(() => {
      if (stopped) return;
      ffmpegOptions.stdoutCallback?.(Buffer.concat([moof2, mdat2]));
    }, 45);

    const emitExtraFragment = (seqTag = 'extra') => {
      if (stopped) return;
      const moof = makeBox('moof', Buffer.from(`mock_moof_${seqTag}`));
      const mdat = makeBox('mdat', crypto.randomBytes(128));
      ffmpegOptions.stdoutCallback?.(Buffer.concat([moof, mdat]));
    };

    const stop = () => {
      if (stopped) return;
      stopped = true;
      clearTimeout(timer1);
      clearTimeout(timer2);
      this.attachedConsumers.delete(consumerId);
      onExit?.();
    };

    const mockSession = {
      isUsingOpus: Promise.resolve(true),
      activateCameraSpeaker: () => {
        this.speakerActivations++;
      },
      sendAudioPacket: (rtp) => {
        this.talkbackPackets.push(rtp);
      },
    };

    this.attachedConsumers.set(consumerId, { ffmpegOptions, stop, emitExtraFragment, triggerExit: stop });
    return { session: mockSession, stop };
  }

  async fetchCloudSnapshot() {
    return Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x02, 0xff, 0xd9]);
  }

  destroy() {
    for (const c of [...this.attachedConsumers.values()]) {
      c.stop();
    }
    this.attachedConsumers.clear();
  }
}

test('config validation enforces permissions, valid pincode, port isolation from 21064, alwaysOnPrebuffer=true default, and install.sh syntax', () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-cfg-'));
  try {
    const { config, paths } = loadOrCreateConfig(tmpDir);
    assert.equal(config.hapPort, 21065);
    assert.equal(config.alwaysOnPrebuffer, true);
    assert.ok(!FORBIDDEN_PINCODES.has(config.pincode));
    assert.match(config.username, /^([0-9A-F]{2}:){5}[0-9A-F]{2}$/);

    const statRoot = fs.statSync(paths.root);
    assert.equal(statRoot.mode & 0o777, 0o700);
    const statCfg = fs.statSync(paths.configFile);
    assert.equal(statCfg.mode & 0o777, 0o600);

    assert.throws(() => validateConfig({ ...config, hapPort: 21064 }), /reserved/);
    assert.throws(() => validateConfig({ ...config, pincode: '123-45-678' }), /forbidden/);

    const symTarget = path.join(tmpDir, 'target.json');
    const symLink = path.join(tmpDir, 'link.json');
    fs.writeFileSync(symTarget, '{}');
    fs.symlinkSync(symTarget, symLink);
    assert.throws(() => writeJsonAtomic(symLink, { a: 1 }), /symlink/);

    // Verify install.sh has valid bash syntax
    const installerPath = path.resolve(path.dirname(new URL(import.meta.url).pathname), '../install.sh');
    execFileSync('/bin/bash', ['-n', installerPath]);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('pruneClips protects activePartPaths, accounts for active bytes, startService checkOnly does not unlink active .part files, and acquireStateDirLock prevents concurrent daemon clobbering', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-clips-'));
  try {
    const { paths } = loadOrCreateConfig(tmpDir, { ffmpegPath: process.execPath });
    const now = Date.now();
    const oldClip = path.join(paths.clipsDir, 'old.mp4');
    const stalePart = path.join(paths.clipsDir, '.stale.mp4.part');
    const activePart = path.join(paths.clipsDir, '.active.mp4.part');
    const clip1 = path.join(paths.clipsDir, 'clip1.mp4');
    const clip2 = path.join(paths.clipsDir, 'clip2.mp4');
    fs.writeFileSync(oldClip, Buffer.alloc(1024));
    fs.writeFileSync(stalePart, Buffer.alloc(4096));
    fs.writeFileSync(activePart, Buffer.alloc(1500));
    fs.writeFileSync(clip1, Buffer.alloc(2048));
    fs.writeFileSync(clip2, Buffer.alloc(2048));

    const tenDaysAgo = (now - 11 * 86400_000) / 1000;
    const tenMinAgo = (now - 600_000) / 1000;
    const twoMinAgo = (now - 120_000) / 1000;
    const oneMinAgo = (now - 60_000) / 1000;
    fs.utimesSync(oldClip, tenDaysAgo, tenDaysAgo);
    fs.utimesSync(stalePart, tenMinAgo, tenMinAgo);
    fs.utimesSync(activePart, tenMinAgo, tenMinAgo);
    fs.utimesSync(clip1, twoMinAgo, twoMinAgo);
    fs.utimesSync(clip2, oneMinAgo, oneMinAgo);

    // Preflight check (--check) must NOT delete any .part files
    await startService({ stateDir: tmpDir, checkOnly: true });
    assert.equal(fs.existsSync(activePart), true);
    assert.equal(fs.existsSync(stalePart), true);

    // Exclusive state-directory lock must block a second startService invocation BEFORE pruneClips runs
    const lock = acquireStateDirLock(tmpDir);
    try {
      await assert.rejects(() => startService({ stateDir: tmpDir }), /already locked/);
      assert.equal(fs.existsSync(activePart), true);
      assert.equal(fs.existsSync(stalePart), true);
    } finally {
      lock.release();
    }

    const res = pruneClips(paths.clipsDir, {
      maxAgeMs: 10 * 86400_000,
      maxTotalBytes: 4000,
      stalePartAgeMs: 300_000,
      activePartPaths: new Set([activePart]),
      now,
    });
    assert.equal(res.removedCount, 3);
    assert.equal(fs.existsSync(activePart), true);
    assert.equal(fs.existsSync(stalePart), false);
    assert.equal(fs.existsSync(oldClip), false);
    assert.equal(fs.existsSync(clip1), false);
    assert.equal(fs.existsSync(clip2), true);
    assert.equal(res.remainingBytes, 1500 + 2048);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('LocalClipRecorder preserves .part file on fsync error and closes descriptor on initialization failure', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-dur-'));
  let doorbell = null;
  try {
    const rec = new LocalClipRecorder(tmpDir, 'ding');
    rec.writeInit(Buffer.from('init_bytes'));
    rec.writeFragment(Buffer.from('frag_bytes'));
    // Close underlying fd early so fsyncSync inside close() fails deterministically with EBADF
    fs.closeSync(rec.fd);

    const res = rec.close();
    assert.equal(res.ok, false);
    assert.ok(res.error);
    assert.equal(fs.existsSync(rec.tmpPath), true);
    assert.equal(fs.existsSync(rec.finalPath), false);

    // Verify archive initialization failure (writeInit throwing ENOSPC) closes fd and unlinks .part file
    const { config, paths } = loadOrCreateConfig(tmpDir, {
      bindAddress: '127.0.0.1',
      alwaysOnPrebuffer: false,
    });
    const mediaSource = new DeterministicTestMediaSource();
    doorbell = new RingHksvDoorbell({ config, paths, mediaSource });

    const origWriteInit = LocalClipRecorder.prototype.writeInit;
    LocalClipRecorder.prototype.writeInit = function () {
      throw new Error('ENOSPC: simulated disk full during writeInit');
    };
    try {
      await doorbell.handleRingEvent({ type: 'ding', at: Date.now() });
      const leftoverParts = fs.readdirSync(paths.clipsDir).filter((n) => n.endsWith('.part'));
      assert.equal(leftoverParts.length, 0);
    } finally {
      LocalClipRecorder.prototype.writeInit = origWriteInit;
    }
  } finally {
    doorbell?.destroy();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('Fmp4StreamSegmenter and cleanRingSdp enforce loopback c=IN IP4 127.0.0.1 and reject oversized boxes', () => {
  const rawSdp = [
    'v=0',
    'o=- 1 1 IN IP4 34.200.10.5',
    's=Ring',
    'c=IN IP4 34.200.10.5',
    't=0 0',
    'm=audio 12345 RTP/AVP 111',
    'c=IN IP4 34.200.10.5',
    'a=rtpmap:111 opus/48000/2',
    'm=video 23456 RTP/AVP 96',
    'c=IN IP4 34.200.10.5',
    'a=rtpmap:96 H264/90000',
  ].join('\n');
  const cleaned = cleanRingSdp(rawSdp, true);
  assert.ok(!cleaned.includes('34.200.10.5'));
  assert.match(cleaned, /c=IN IP4 127\.0\.0\.1/);

  const seg = new Fmp4StreamSegmenter();
  const inits = [];
  const fragments = [];
  seg.on('init', (b) => inits.push(b));
  seg.on('fragment', (b) => fragments.push(b));

  const ftyp = makeBox('ftyp', Buffer.from('isom'));
  const moov = makeBox('moov', Buffer.from('moov_payload'));
  const prft = makeBox('prft', Buffer.from('ntp_time'));
  const moof = makeBox('moof', Buffer.from('moof_payload'));
  const mdat = makeBox('mdat', Buffer.from('mdat_video_audio_bytes'), true);

  const fullStream = Buffer.concat([ftyp, moov, prft, moof, mdat]);
  for (let i = 0; i < fullStream.length; i += 3) {
    seg.push(fullStream.subarray(i, i + 3));
  }

  assert.equal(inits.length, 1);
  assert.equal(fragments.length, 1);

  const badHeader = Buffer.alloc(8);
  badHeader.writeUInt32BE(MAX_BOX_BYTES + 100, 0);
  badHeader.write('mdat', 4, 4, 'ascii');
  assert.throws(() => peekBoxHeader(badHeader), /oversized/);
});

test('RFC 3711 verifyAndDecryptSrtp/Srtcp reject replays, handle rollover 65534->65535->0->1, and do not store state for unauthenticated SSRCs', () => {
  const key = crypto.randomBytes(16);
  const salt = crypto.randomBytes(14);
  const sender = new SrtpSession({
    profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
    keys: { localMasterKey: key, localMasterSalt: salt, remoteMasterKey: key, remoteMasterSalt: salt },
  });
  const receiver = new SrtpSession({
    profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
    keys: { localMasterKey: key, localMasterSalt: salt, remoteMasterKey: key, remoteMasterSalt: salt },
  });

  // Unauthenticated SRTP and SRTCP packets with random SSRCs must NOT allocate map entries
  for (let i = 0; i < 100; i++) {
    const fakeBuf = crypto.randomBytes(32);
    fakeBuf.writeUInt32BE(1000 + i, 8);
    fakeBuf.writeUInt32BE(2000 + i, 4);
    assert.throws(() => verifyAndDecryptSrtp(receiver, fakeBuf), /authentication tag mismatch/);
    assert.throws(() => verifyAndDecryptSrtcp(receiver, fakeBuf), /authentication tag mismatch/);
  }
  assert.equal(receiver._rfc3711States.size, 0);
  assert.equal(receiver._srtcpReplayStates.size, 0);

  const seqs = [65534, 65535, 0, 1];
  let firstEncrypted = null;

  for (const seq of seqs) {
    const pkt = new RtpPacket(
      new RtpHeader({ payloadType: 110, sequenceNumber: seq, timestamp: seq * 480, ssrc: 0x12345678 }),
      Buffer.from(`payload-${seq}`),
    );
    const enc = sender.encrypt(pkt.payload, pkt.header);
    if (seq === 65534) firstEncrypted = enc;
    const dec = verifyAndDecryptSrtp(receiver, enc);
    const parsed = RtpPacket.deSerialize(dec);
    assert.equal(parsed.header.sequenceNumber, seq);
    assert.equal(parsed.payload.toString(), `payload-${seq}`);
  }

  assert.equal(receiver._rfc3711States.size, 1);
  // Replaying packet 65534 must be rejected by the 64-packet replay window
  assert.throws(() => verifyAndDecryptSrtp(receiver, firstEncrypted), /replay/);
});

test('Disabling RecordingAudioActive during an active HKSV stream aborts audio-bearing stream and restarts prebuffer with -an, and unexpected producer exit recovers prebuffer', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-audio-active-'));
  let doorbell = null;
  try {
    const { config, paths } = loadOrCreateConfig(tmpDir, { bindAddress: '127.0.0.1' });
    const mediaSource = new DeterministicTestMediaSource();
    doorbell = new RingHksvDoorbell({ config, paths, mediaSource });

    doorbell.controller.recordingManagement.handleSelectedCameraRecordingConfigurationWrite(SAMPLE_SELECTED_CONFIG_BASE64);
    doorbell.controller.recordingManagement.recordingManagementService
      .getCharacteristic(Characteristic.Active)
      .setValue(Characteristic.Active.ACTIVE);
    doorbell.controller.recordingManagement.recordingManagementService
      .getCharacteristic(Characteristic.RecordingAudioActive)
      .setValue(1);

    // Prebuffer should start automatically when recording is active + configured
    await new Promise((r) => setTimeout(r, 35));
    assert.equal(mediaSource.attachedConsumers.size, 1);

    await doorbell.handleRingEvent({ type: 'motion', motion: true, at: Date.now() });

    const ac = new AbortController();
    const gen = doorbell.handleRecordingStreamRequest(51, ac.signal);
    const received = [];

    await assert.rejects(
      async () => {
        for await (const pkt of gen) {
          received.push(pkt);
          if (received.length === 2) {
            // Disable RecordingAudioActive mid-stream!
            doorbell.controller.recordingManagement.recordingManagementService
              .getCharacteristic(Characteristic.RecordingAudioActive)
              .setValue(0);
          }
        }
      },
      (err) => err instanceof HDSProtocolError,
    );

    await new Promise((r) => setTimeout(r, 35));
    assert.equal(mediaSource.attachedConsumers.size, 1);
    const replacement = [...mediaSource.attachedConsumers.values()][0];
    assert.equal(replacement.ffmpegOptions.includeAudio, false);
    assert.deepEqual(replacement.ffmpegOptions.audio, ['-an']);

    // Simulate an unexpected producer exit while alwaysOnPrebuffer is enabled: must automatically recover!
    replacement.triggerExit();
    assert.equal(mediaSource.attachedConsumers.size, 0);
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(mediaSource.attachedConsumers.size, 1);
  } finally {
    doorbell?.destroy();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('Disabling HomeKitCameraActive stops Live View sessions and overlapping capture startup retries without leaving capture active', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-privacy-'));
  const hkVideoReceiver = await bindUdpSocket('127.0.0.1', 0);
  const hkAudioReceiver = await bindUdpSocket('127.0.0.1', 0);
  let doorbell = null;
  try {
    const { config, paths } = loadOrCreateConfig(tmpDir, {
      bindAddress: '127.0.0.1',
      alwaysOnPrebuffer: false,
    });
    const mediaSource = new DeterministicTestMediaSource({ startupDelayMs: 40 });
    doorbell = new RingHksvDoorbell({ config, paths, mediaSource });

    // Start a Live View session first
    await new Promise((resolve, reject) => {
      doorbell.prepareStream(
        {
          sessionID: 'session-priv-1',
          sourceAddress: '127.0.0.1',
          targetAddress: '127.0.0.1',
          addressVersion: 'ipv4',
          video: { port: hkVideoReceiver.port, srtpCryptoSuite: 0, srtp_key: crypto.randomBytes(16), srtp_salt: crypto.randomBytes(14) },
          audio: { port: hkAudioReceiver.port, srtpCryptoSuite: 0, srtp_key: crypto.randomBytes(16), srtp_salt: crypto.randomBytes(14) },
        },
        (err, res) => (err ? reject(err) : resolve(res)),
      );
    });
    await new Promise((resolve, reject) => {
      doorbell.handleStreamRequest(
        {
          sessionID: 'session-priv-1',
          type: StreamRequestTypes.START,
          video: { width: 640, height: 360, fps: 15, max_bit_rate: 500, pt: 99, mtu: 1378 },
          audio: { codec: 'OPUS', channel: 1, sample_rate: 24, max_bit_rate: 24, pt: 110, packet_time: 20 },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
    assert.equal(doorbell.liveSessions.size, 1);

    // Fire two overlapping capture events and disable HomeKitCameraActive mid-attachment
    const ev1 = doorbell.handleRingEvent({ type: 'motion', motion: true, at: Date.now() });
    const ev2 = doorbell.handleRingEvent({ type: 'ding', at: Date.now() });
    await new Promise((r) => setTimeout(r, 15));
    doorbell.controller.recordingManagement.operatingModeService
      .getCharacteristic(Characteristic.HomeKitCameraActive)
      .setValue(Characteristic.HomeKitCameraActive.OFF);

    await Promise.all([ev1, ev2]);
    assert.equal(doorbell.isHomeKitCameraActive(), false);
    assert.equal(doorbell.liveSessions.size, 0);
    assert.equal(mediaSource.attachedConsumers.size, 0);
    assert.equal(doorbell.motionDetected, false);
  } finally {
    doorbell?.destroy();
    hkVideoReceiver.close();
    hkAudioReceiver.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('updateRecordingConfiguration during active stream defers replacement until stream end, queued fragments drain in order, and planned max-timer stop yields final isLast=true packet', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-defer-'));
  let doorbell = null;
  try {
    const { config, paths } = loadOrCreateConfig(tmpDir, { bindAddress: '127.0.0.1' });
    const mediaSource = new DeterministicTestMediaSource();
    doorbell = new RingHksvDoorbell({ config, paths, mediaSource });

    doorbell.controller.recordingManagement.handleSelectedCameraRecordingConfigurationWrite(SAMPLE_SELECTED_CONFIG_BASE64);
    doorbell.controller.recordingManagement.recordingManagementService
      .getCharacteristic(Characteristic.Active)
      .setValue(Characteristic.Active.ACTIVE);
    doorbell.controller.recordingManagement.recordingManagementService
      .getCharacteristic(Characteristic.RecordingAudioActive)
      .setValue(1);

    await doorbell.handleRingEvent({ type: 'ding', at: Date.now() });

    const ac = new AbortController();
    const gen = doorbell.handleRecordingStreamRequest(61, ac.signal);
    const packets = [];

    const firstConsumer = [...mediaSource.attachedConsumers.values()][0];
    const readerPromise = (async () => {
      for await (const pkt of gen) {
        packets.push(pkt);
        if (packets.length === 2) {
          // Mutate recordingConfiguration while stream 61 is active: must NOT stop firstConsumer mid-stream
          doorbell.updateRecordingConfiguration({
            ...doorbell.recordingConfiguration,
            mediaContainerConfiguration: { type: 0, fragmentLength: 2000 },
          });
          assert.equal([...mediaSource.attachedConsumers.values()][0], firstConsumer);

          // Queue TWO fragments in a burst and mark planned end: BOTH must be delivered, with isLast=true only on the second!
          firstConsumer.emitExtraFragment('burst-a');
          doorbell.captureEnding = true;
          firstConsumer.emitExtraFragment('burst-b');
        }
      }
    })();

    await readerPromise;
    assert.ok(packets.length >= 4);
    assert.equal(packets[packets.length - 2].isLast, false);
    assert.equal(packets[packets.length - 1].isLast, true);
    assert.equal(doorbell.recordingConfiguration.mediaContainerConfiguration.fragmentLength, 2000);

    // Now test captureMaxTimer firing after all buffered fragments were already yielded with isLast=false:
    // must still yield a final packet with isLast=true and not throw HDSProtocolError!
    await doorbell.handleRingEvent({ type: 'motion', motion: true, at: Date.now() });
    await new Promise((r) => setTimeout(r, 60));
    const ac2 = new AbortController();
    const packets2 = [];
    const reader2 = (async () => {
      for await (const pkt of doorbell.handleRecordingStreamRequest(62, ac2.signal)) {
        packets2.push(pkt);
        if (packets2.length === 3 && !pkt.isLast) {
          // Trigger captureMaxTimer callback directly while generator is waiting for next fragment
          setTimeout(() => {
            doorbell.captureMaxTimer?._onTimeout?.();
          }, 10);
        }
      }
    })();
    await withTimeoutAndAbort(reader2, { timeoutMs: 3000, label: 'max-timer stream completion' });
    assert.ok(packets2.length >= 3);
    assert.equal(packets2[packets2.length - 1].isLast, true);
  } finally {
    doorbell?.destroy();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('Failed capture startup serializes waiting callers and RingCameraMediaSource stops timed-out call even when replacement call is active', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-fail-serial-'));
  let doorbell = null;
  try {
    const { config, paths } = loadOrCreateConfig(tmpDir, {
      bindAddress: '127.0.0.1',
      alwaysOnPrebuffer: false,
    });
    const mediaSource = new DeterministicTestMediaSource({ startupDelayMs: 25, failFirstAttach: true });
    doorbell = new RingHksvDoorbell({ config, paths, mediaSource });

    await Promise.all([
      doorbell.handleRingEvent({ type: 'ding', at: Date.now() }),
      doorbell.handleRingEvent({ type: 'motion', motion: true, at: Date.now() }),
      doorbell.handleRingEvent({ type: 'motion', motion: true, at: Date.now() }),
    ]);

    assert.equal(mediaSource.attachCallCount, 2);
    assert.equal(mediaSource.attachedConsumers.size, 1);
    doorbell.destroy();
    doorbell = null;

    // Call A times out, Call B starts (so consumers.size > 0), then Call A resolves: Call A must be stopped while Call B remains active!
    let callAStopped = false;
    let callBStopped = false;
    let callNum = 0;
    const fakeCamera = {
      name: 'Front Door',
      model: 'Ring Doorbell',
      startLiveCall: () => {
        const idx = ++callNum;
        if (idx === 1) {
          return new Promise((resolve) => {
            setTimeout(() => {
              resolve({
                hasEnded: false,
                onCallEnded: new ReplaySubject(1),
                stop: () => {
                  callAStopped = true;
                },
              });
            }, 70);
          });
        }
        return Promise.resolve({
          hasEnded: false,
          onCallEnded: new ReplaySubject(1),
          stop: () => {
            callBStopped = true;
          },
        });
      },
    };
    const ringSource = new RingCameraMediaSource(fakeCamera, { startupTimeoutMs: 20 });
    await assert.rejects(() => ringSource.acquireSession('c1'), /timed out/);
    const sessionB = await ringSource.acquireSession('c2');
    assert.ok(sessionB);
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(callAStopped, true);
    assert.equal(callBStopped, false);
    ringSource.destroy();
    assert.equal(callBStopped, true);

    const subj = new Subject();
    await assert.rejects(() => waitForObservableFirst(subj, { timeoutMs: 15, label: 'testSubj' }), /timed out/);
    assert.equal(subj.observed, false);
  } finally {
    doorbell?.destroy();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('authenticateAndPersistRing awaits asynchronous registerPushReceiver credential update and rejects when push credentials are missing or time out', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-auth-'));
  try {
    const validPnc = {
      fcm: { token: 'fcm-token-123', installation: { token: 'inst-1' } },
      gcm: { token: 'gcm-token-123', androidId: '12345' },
      keys: { privateKey: 'priv', publicKey: 'pub', authSecret: 'sec' },
      config: { bundleId: 'receiver.push.com', projectId: 'ring-17770', vapidKey: '' },
    };
    const oauthOnlyWrappedToken = Buffer.from(
      JSON.stringify({ rt: 'oauth-rt-1', hid: 'hw-1' }),
      'utf8',
    ).toString('base64');
    const pushReadyWrappedToken = Buffer.from(
      JSON.stringify({ rt: 'oauth-rt-1', hid: 'hw-1', pnc: validPnc }),
      'utf8',
    ).toString('base64');

    // 1. Ordinary OAuth rotation without push credentials must fail and not persist push-less token
    const noPushUpdates = new Subject();
    let noPushDisconnected = false;
    const noPushApi = {
      restClient: { refreshToken: 'raw-initial-token' },
      onRefreshTokenUpdated: noPushUpdates,
      registerPushReceiver: async () => {
        // Simulates RingApi.registerPushReceiver catching pushReceiver.connect() error internally
        await new Promise((r) => setTimeout(r, 15));
      },
      getCameras: async () => {
        // Ordinary OAuth rotation happens during getAuth() before registerPushReceiver
        noPushApi.restClient.refreshToken = oauthOnlyWrappedToken;
        noPushUpdates.next({ oldRefreshToken: 'raw-initial-token', newRefreshToken: oauthOnlyWrappedToken });
        noPushApi.registerPushReceiver([], []);
        return [{ id: 77, name: 'Front Door', model: 'Doorbell Pro', isDoorbot: true }];
      },
      disconnect: () => {
        noPushDisconnected = true;
      },
    };
    await assert.rejects(
      () =>
        authenticateAndPersistRing({
          stateDir: tmpDir,
          initialRefreshToken: 'raw-initial-token',
          apiFactory: () => noPushApi,
          pushTimeoutMs: 80,
        }),
      /usable push receiver credentials/,
    );
    assert.equal(noPushDisconnected, true);
    assert.equal(loadOrCreateConfig(tmpDir).config.ringRefreshToken, null);

    // 2. Full push registration with valid pnc + PATCH /device succeeds and persists token
    const tokenUpdates = new Subject();
    let disconnected = false;
    let devicePatched = false;
    const mockApi = {
      restClient: {
        refreshToken: 'initial-token',
        _internalOnly_pushNotificationCredentials: null,
        request: async (opts) => {
          if (opts?.method === 'PATCH' && String(opts?.url || '').endsWith('/device')) {
            await new Promise((r) => setTimeout(r, 15));
            devicePatched = true;
          }
          return {};
        },
      },
      onRefreshTokenUpdated: tokenUpdates,
      registerPushReceiver: async () => {
        await new Promise((r) => setTimeout(r, 25));
        mockApi.restClient._internalOnly_pushNotificationCredentials = validPnc;
        mockApi.restClient.refreshToken = pushReadyWrappedToken;
        tokenUpdates.next({ oldRefreshToken: oauthOnlyWrappedToken, newRefreshToken: pushReadyWrappedToken });
        mockApi.restClient.request({
          url: 'https://api.ring.com/clients_api/device',
          method: 'PATCH',
          json: { device: { push_notification_token: validPnc.fcm.token } },
        });
      },
      getCameras: async () => {
        mockApi.restClient.refreshToken = oauthOnlyWrappedToken;
        tokenUpdates.next({ oldRefreshToken: 'initial-token', newRefreshToken: oauthOnlyWrappedToken });
        mockApi.registerPushReceiver([], []);
        return [
          {
            id: 77,
            name: 'Front Door',
            model: 'Doorbell Pro',
            isDoorbot: true,
            batteryLevel: 95,
            subscribeToDingEvents: async () => {},
            subscribeToMotionEvents: async () => {},
          },
        ];
      },
      disconnect: () => {
        disconnected = true;
      },
    };

    const res = await authenticateAndPersistRing({
      stateDir: tmpDir,
      initialRefreshToken: 'initial-token',
      apiFactory: () => mockApi,
    });
    assert.equal(res.authenticated, true);
    assert.equal(res.pushCredentialsPersisted, true);
    assert.equal(devicePatched, true);
    assert.equal(disconnected, true);
    const { config } = loadOrCreateConfig(tmpDir);
    assert.equal(config.ringRefreshToken, pushReadyWrappedToken);

    // 3. Verify timeout in registerPushReceiver rejects explicitly rather than reporting false success
    let slowDisconnected = false;
    const slowApi = {
      restClient: { refreshToken: 'slow-initial' },
      onRefreshTokenUpdated: new Subject(),
      registerPushReceiver: () => new Promise((r) => setTimeout(r, 200)),
      getCameras: async () => {
        slowApi.registerPushReceiver([], []);
        return [{ id: 77, name: 'Front Door', model: 'Doorbell Pro', isDoorbot: true }];
      },
      disconnect: () => {
        slowDisconnected = true;
      },
    };
    await assert.rejects(
      () =>
        authenticateAndPersistRing({
          stateDir: tmpDir,
          initialRefreshToken: 'slow-initial',
          apiFactory: () => slowApi,
          pushTimeoutMs: 25,
        }),
      /timed out/,
    );
    assert.equal(slowDisconnected, true);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('Live View SRTP/SRTCP routing, Opus 48kHz->24kHz RTP + RTCP Sender Report scaling, RECONFIGURE video/audio SRTP continuity, prepareStream cancellation, and talkback gate', async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-srtp-'));
  const hkVideoReceiver = await bindUdpSocket('127.0.0.1', 0);
  const hkAudioReceiver = await bindUdpSocket('127.0.0.1', 0);
  const attackerSocket = await bindUdpSocket('127.0.0.1', 0);
  let doorbell = null;

  try {
    const { config, paths } = loadOrCreateConfig(tmpDir, {
      bindAddress: '127.0.0.1',
      alwaysOnPrebuffer: false,
    });
    const mediaSource = new DeterministicTestMediaSource();
    doorbell = new RingHksvDoorbell({ config, paths, mediaSource });

    // Verify STOP during prepareStream cancels preparation and leaves 0 registered sessions
    const cancelPrepPromise = new Promise((resolve) => {
      doorbell.prepareStream(
        {
          sessionID: 'session-cancel-prep',
          sourceAddress: '127.0.0.1',
          targetAddress: '127.0.0.1',
          addressVersion: 'ipv4',
          video: { port: hkVideoReceiver.port, srtpCryptoSuite: 0, srtp_key: crypto.randomBytes(16), srtp_salt: crypto.randomBytes(14) },
          audio: { port: hkAudioReceiver.port, srtpCryptoSuite: 0, srtp_key: crypto.randomBytes(16), srtp_salt: crypto.randomBytes(14) },
        },
        (err) => resolve(err),
      );
      doorbell.handleStreamRequest({ sessionID: 'session-cancel-prep', type: StreamRequestTypes.STOP }, () => {});
    });
    const cancelErr = await cancelPrepPromise;
    assert.ok(cancelErr);
    assert.equal(doorbell.liveSessions.has('session-cancel-prep'), false);

    const controllerVideoKey = crypto.randomBytes(16);
    const controllerVideoSalt = crypto.randomBytes(14);
    const controllerAudioKey = crypto.randomBytes(16);
    const controllerAudioSalt = crypto.randomBytes(14);

    const prepResp = await new Promise((resolve, reject) => {
      doorbell.prepareStream(
        {
          sessionID: 'session-srtp-1',
          sourceAddress: '127.0.0.1',
          targetAddress: '127.0.0.1',
          addressVersion: 'ipv4',
          video: {
            port: hkVideoReceiver.port,
            srtpCryptoSuite: 0,
            srtp_key: controllerVideoKey,
            srtp_salt: controllerVideoSalt,
          },
          audio: {
            port: hkAudioReceiver.port,
            srtpCryptoSuite: 0,
            srtp_key: controllerAudioKey,
            srtp_salt: controllerAudioSalt,
          },
        },
        (err, res) => (err ? reject(err) : resolve(res)),
      );
    });

    await new Promise((resolve, reject) => {
      doorbell.handleStreamRequest(
        {
          sessionID: 'session-srtp-1',
          type: StreamRequestTypes.START,
          video: { width: 1280, height: 720, fps: 24, max_bit_rate: 1500, pt: 99, mtu: 1378 },
          audio: { codec: 'OPUS', channel: 1, sample_rate: 24, max_bit_rate: 24, pt: 110, packet_time: 20 },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });

    const sessionInfo = doorbell.liveSessions.get('session-srtp-1');
    const hkVideoSrtp = new SrtpSession({
      profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
      keys: {
        localMasterKey: controllerVideoKey,
        localMasterSalt: controllerVideoSalt,
        remoteMasterKey: prepResp.video.srtp_key,
        remoteMasterSalt: prepResp.video.srtp_salt,
      },
    });
    const hkAudioSrtp = new SrtpSession({
      profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
      keys: {
        localMasterKey: controllerAudioKey,
        localMasterSalt: controllerAudioSalt,
        remoteMasterKey: prepResp.audio.srtp_key,
        remoteMasterSalt: prepResp.audio.srtp_salt,
      },
    });

    const receivedVideoRtp = [];
    const receivedAudioRtp = [];
    const receivedAudioSrtcp = [];

    const waitForPackets = (videoTarget, audioTarget, rtcpTarget) =>
      withTimeoutAndAbort(
        new Promise((resolve, reject) => {
          const check = () => {
            if (
              receivedVideoRtp.length >= videoTarget &&
              receivedAudioRtp.length >= audioTarget &&
              receivedAudioSrtcp.length >= rtcpTarget
            ) {
              hkVideoReceiver.socket.removeListener('message', onV);
              hkAudioReceiver.socket.removeListener('message', onA);
              resolve();
            }
          };
          const onV = (msg) => {
            try {
              const dec = verifyAndDecryptSrtp(hkVideoSrtp, msg);
              receivedVideoRtp.push(RtpPacket.deSerialize(dec));
              check();
            } catch (err) {
              reject(err);
            }
          };
          const onA = (msg, rinfo) => {
            try {
              assert.equal(rinfo.port, prepResp.audio.port);
              const pt = msg.readUInt8(1) & 0x7f;
              if (pt >= 64 && pt <= 95) {
                const decRtcp = verifyAndDecryptSrtcp(hkAudioSrtp, msg);
                receivedAudioSrtcp.push(decRtcp);
              } else {
                const dec = verifyAndDecryptSrtp(hkAudioSrtp, msg);
                receivedAudioRtp.push(RtpPacket.deSerialize(dec));
              }
              check();
            } catch (err) {
              reject(err);
            }
          };
          hkVideoReceiver.socket.on('message', onV);
          hkAudioReceiver.socket.on('message', onA);
          check();
        }),
        { timeoutMs: 4000, label: 'Synthetic SRTP/SRTCP receipt' },
      );

    const vPkt1 = new RtpPacket(
      new RtpHeader({ payloadType: 99, sequenceNumber: 500, timestamp: 90000, ssrc: 111 }),
      Buffer.from('h264-nal-epoch1'),
    );
    const aPkt1 = new RtpPacket(
      new RtpHeader({ payloadType: 110, sequenceNumber: 10, timestamp: 10000, ssrc: 999 }),
      Buffer.from('opus-frame-1'),
    );
    const aPkt2 = new RtpPacket(
      new RtpHeader({ payloadType: 110, sequenceNumber: 11, timestamp: 58000, ssrc: 999 }),
      Buffer.from('opus-frame-2'),
    );
    // 28-byte RTCP Sender Report (PT=200) at 48kHz RTP timestamp 58000 (1 second after 10000):
    // must be scaled to 34000 (24kHz clock) at byte offset 16!
    const srBuf = Buffer.alloc(28);
    srBuf.writeUInt8(0x80, 0);
    srBuf.writeUInt8(200, 1);
    srBuf.writeUInt16BE(6, 2);
    srBuf.writeUInt32BE(999, 4);
    srBuf.writeUInt32BE(58000, 16);

    const initialBatchPromise = waitForPackets(1, 2, 1);
    sessionInfo.liveProxy.sendSyntheticRtpForTest({
      videoRtpPacket: vPkt1.serialize(),
      audioRtpPacket: aPkt1.serialize(),
    });
    sessionInfo.liveProxy.sendSyntheticRtpForTest({
      audioRtpPacket: aPkt2.serialize(),
      audioRtcpPacket: srBuf,
    });
    await initialBatchPromise;

    assert.equal(receivedVideoRtp[0].header.sequenceNumber, 500);
    assert.equal(receivedVideoRtp[0].header.timestamp, 90000);
    assert.equal(receivedAudioRtp[0].header.timestamp, 10000);
    assert.equal(receivedAudioRtp[1].header.timestamp, 34000);
    assert.equal(receivedAudioSrtcp[0].readUInt8(1), 200);
    assert.equal(receivedAudioSrtcp[0].readUInt32BE(4), prepResp.audio.ssrc);
    assert.equal(receivedAudioSrtcp[0].readUInt32BE(16), 34000);

    // Verify unauthenticated UDP packet does NOT activate speaker
    const unauthRtp = new RtpPacket(
      new RtpHeader({ payloadType: 110, sequenceNumber: 1, timestamp: 100, ssrc: 555 }),
      Buffer.from('unauthenticated-spoof'),
    ).serialize();
    attackerSocket.socket.send(unauthRtp, prepResp.audio.port, '127.0.0.1');
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(mediaSource.speakerActivations, 0);

    // Test concurrent RECONFIGURE requests where request 1 resolves slower than request 2,
    // and request 2 specifies max_bit_rate: 128
    const origAttach = mediaSource.attachFfmpeg.bind(mediaSource);
    mediaSource.attachFfmpeg = async (cid, opts, meta) => {
      if (cid.endsWith(':r1')) {
        await new Promise((r) => setTimeout(r, 50));
      } else if (cid.endsWith(':r2')) {
        await new Promise((r) => setTimeout(r, 5));
      }
      return origAttach(cid, opts, meta);
    };

    const r1Promise = new Promise((resolve, reject) => {
      doorbell.handleStreamRequest(
        {
          sessionID: 'session-srtp-1',
          type: StreamRequestTypes.RECONFIGURE,
          video: { width: 640, height: 360, fps: 15, max_bit_rate: 900 },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
    const r2Promise = new Promise((resolve, reject) => {
      doorbell.handleStreamRequest(
        {
          sessionID: 'session-srtp-1',
          type: StreamRequestTypes.RECONFIGURE,
          video: { width: 320, height: 180, fps: 15, max_bit_rate: 128 },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
    await Promise.all([r1Promise, r2Promise]);

    assert.equal(mediaSource.attachedConsumers.has('live:session-srtp-1:r1'), false);
    const activeReconf = mediaSource.attachedConsumers.get('live:session-srtp-1:r2');
    assert.ok(activeReconf);
    assert.ok(activeReconf.ffmpegOptions.video.includes('128k'));

    // Now send replacement encoder packets with RESET sequence numbers (500 again!) and RESET random timestamps:
    // proxy must maintain continuous sequence numbers (501, 12) and continuous monotonic timestamps without SRTP replay failure!
    const postReconfPromise = waitForPackets(2, 3, 1);
    const vPktReconf = new RtpPacket(
      new RtpHeader({ payloadType: 99, sequenceNumber: 500, timestamp: 1234, ssrc: 222 }),
      Buffer.from('h264-nal-epoch2'),
    );
    const aPktReconf = new RtpPacket(
      new RtpHeader({ payloadType: 110, sequenceNumber: 10, timestamp: 500, ssrc: 888 }),
      Buffer.from('opus-frame-epoch2'),
    );
    sessionInfo.liveProxy.sendSyntheticRtpForTest({
      videoRtpPacket: vPktReconf.serialize(),
      audioRtpPacket: aPktReconf.serialize(),
    });
    await postReconfPromise;

    assert.equal(receivedVideoRtp[1].header.sequenceNumber, 501);
    assert.ok(receivedVideoRtp[1].header.timestamp > receivedVideoRtp[0].header.timestamp);
    assert.equal(receivedAudioRtp[2].header.sequenceNumber, 12);
    assert.equal(receivedAudioRtp[2].header.timestamp, 34480);
  } finally {
    doorbell?.destroy();
    hkVideoReceiver.close();
    hkAudioReceiver.close();
    attackerSocket.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test('SpawnedFfmpeg safely handles missing binary ENOENT, parseCliArgs parses --state-dir, and attachConsumerToRingSession enforces loopback SDP flags', async () => {
  const exitResult = await new Promise((resolve) => {
    const ff = new SpawnedFfmpeg({
      ffmpegPath: '/nonexistent/path/to/ffmpeg-binary',
      ffmpegArgs: ['-version'],
      exitCallback: (code, signal, err) => {
        resolve({ code, signal, err });
      },
    });
    ff.writeStdin('test');
  });
  assert.ok(exitResult.err);
  assert.equal(exitResult.err.code, 'ENOENT');

  const p1 = parseCliArgs(['--state-dir', '/tmp/custom-state', 'pair-info']);
  assert.equal(p1.stateDir, path.resolve('/tmp/custom-state'));
  assert.equal(p1.command, 'pair-info');
  assert.throws(() => parseCliArgs(['--state-dir']), /Missing value/);

  // Test attachConsumerToRingSession with a mock Ring StreamingSession and process.execPath
  const callAnswered = new ReplaySubject(1);
  const usingOpus = new ReplaySubject(1);
  const callEnded = new Subject();
  const audioRtp = new Subject();
  const videoRtp = new Subject();
  let keyFrameRequested = false;

  callAnswered.next(
    [
      'v=0',
      'o=- 1 1 IN IP4 54.1.2.3',
      's=RingLive',
      'c=IN IP4 54.1.2.3',
      't=0 0',
      'm=audio 10000 RTP/AVP 111',
      'a=rtpmap:111 opus/48000/2',
      'm=video 20000 RTP/AVP 96',
      'a=rtpmap:96 H264/90000',
    ].join('\n'),
  );
  usingOpus.next(true);

  const mockRingSession = {
    hasEnded: false,
    onCallEnded: callEnded,
    onUsingOpus: usingOpus,
    onAudioRtp: audioRtp,
    onVideoRtp: videoRtp,
    connection: {
      onCallAnswered: callAnswered,
      onError: new Subject(),
    },
    requestKeyFrame: () => {
      keyFrameRequested = true;
    },
  };

  let stdinReceived = '';
  const attached = await attachConsumerToRingSession(
    mockRingSession,
    {
      input: ['-e', 'process.stdin.on("data", d => process.stdout.write(d))'],
      audio: [],
      video: [],
      output: [],
      stdoutCallback: (c) => {
        stdinReceived += c.toString('utf8');
      },
    },
    { ffmpegPath: process.execPath, label: 'TestRingAttach' },
  );
  assert.equal(keyFrameRequested, true);
  attached.stop();
  assert.equal(audioRtp.observed, false);
  assert.equal(videoRtp.observed, false);
});

test('selectRingCamera prioritizes configured ID/name and doorbots, and patched push-receiver decrypt handles Ring multi-param crypto-key headers', () => {
  const cams = [
    { id: 10, name: 'Backyard Floodlight', isDoorbot: false },
    { id: 20, name: 'Front Door', isDoorbot: true },
  ];
  assert.equal(selectRingCamera(cams, { targetCameraId: null, targetCameraName: null }).id, 20);
  assert.equal(selectRingCamera(cams, { targetCameraId: 10, targetCameraName: null }).id, 10);
  assert.equal(selectRingCamera(cams, { targetCameraId: null, targetCameraName: 'backyard floodlight' }).id, 10);

  // Verify patched @eneris/push-receiver decrypt handles Ring FCM `dh=...; p256ecdsa=...` and `salt=...==`
  const cjsRequire = createRequire(import.meta.url);
  const httpEce = cjsRequire('http_ece');
  const decryptMod = cjsRequire('@eneris/push-receiver/dist/utils/decrypt.js');

  const receiverCurve = crypto.createECDH('prime256v1');
  receiverCurve.generateKeys();
  const senderCurve = crypto.createECDH('prime256v1');
  senderCurve.generateKeys();
  const vapidCurve = crypto.createECDH('prime256v1');
  vapidCurve.generateKeys();
  const authSecret = crypto.randomBytes(16);
  const salt = crypto.randomBytes(16);

  const payload = {
    version: '2.0.0',
    android_config: { category: 'com.ring.pn.live-event.motion' },
    data: { event: { ding: { id: '123456', subtype: 'human' } } },
  };
  const encrypted = httpEce.encrypt(Buffer.from(JSON.stringify(payload), 'utf8'), {
    version: 'aesgcm',
    privateKey: senderCurve,
    dh: receiverCurve.getPublicKey(),
    authSecret,
    salt,
  });

  const fcmObject = {
    rawData: encrypted,
    appData: [
      {
        key: 'crypto-key',
        value: `dh=${senderCurve.getPublicKey('base64url')}; p256ecdsa=${vapidCurve.getPublicKey('base64url')}`,
      },
      {
        key: 'encryption',
        value: `salt=${salt.toString('base64')}`,
      },
    ],
  };

  const decrypted = decryptMod.default(fcmObject, {
    privateKey: receiverCurve.getPrivateKey('base64url'),
    publicKey: receiverCurve.getPublicKey('base64url'),
    authSecret: authSecret.toString('base64url'),
  });
  assert.deepEqual(decrypted, payload);

  // Verify RingCameraMediaSource.startEventListeners forwards every onNewNotification push event
  const onNewNotification = new Subject();
  const receivedEvents = [];
  const source = new RingCameraMediaSource(
    { name: 'Front Door', model: 'Doorbell 2', onNewNotification },
    { onEvent: (ev) => receivedEvents.push(ev) },
  );
  source.startEventListeners();
  onNewNotification.next(payload);
  onNewNotification.next(payload);
  onNewNotification.next({
    android_config: { category: 'com.ring.pn.live-event.ding' },
    data: { event: { ding: { id: '789', subtype: 'ding' } } },
  });
  assert.equal(receivedEvents.length, 3);
  assert.equal(receivedEvents[0].type, 'motion');
  assert.equal(receivedEvents[1].type, 'motion');
  assert.equal(receivedEvents[2].type, 'ding');
  source.destroy();
});

function verifyMp4DecodesWithFfmpeg(ffmpegPath, mp4BufferOrPath, { timeoutMs = 6000 } = {}) {
  return new Promise((resolve, reject) => {
    const isPath = typeof mp4BufferOrPath === 'string';
    const args = [
      '-hide_banner',
      '-v', 'error',
      ...(isPath ? ['-i', mp4BufferOrPath] : ['-f', 'mp4', '-i', 'pipe:0']),
      '-f', 'null',
      '-',
    ];
    const proc = spawn(ffmpegPath, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    const errChunks = [];
    const timer = setTimeout(() => {
      try {
        proc.kill('SIGKILL');
      } catch {}
      reject(new Error(`verifyMp4DecodesWithFfmpeg timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    proc.stderr.on('data', (c) => errChunks.push(c));
    proc.stdin.on('error', () => {});
    proc.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
    proc.on('close', (code) => {
      clearTimeout(timer);
      const errText = Buffer.concat(errChunks).toString('utf8').trim();
      if (code === 0 && !errText) {
        resolve();
      } else {
        reject(new Error(`FFmpeg decode validation failed (code ${code}): ${errText}`));
      }
    });
    if (!isPath) {
      proc.stdin.end(mp4BufferOrPath);
    }
  });
}

test('Real FFmpeg E2E test (when /usr/bin/ffmpeg is present): SyntheticFfmpegMediaSource -> fMP4 keyframe/duration compliance + clip archive decode + Live View SRTP', async (t) => {
  const ffmpegPath = '/usr/bin/ffmpeg';
  if (!fs.existsSync(ffmpegPath)) {
    t.skip('/usr/bin/ffmpeg not installed on this host');
    return;
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ring-hksv-real-ffmpeg-'));
  const hkVideoReceiver = await bindUdpSocket('127.0.0.1', 0);
  const hkAudioReceiver = await bindUdpSocket('127.0.0.1', 0);
  let doorbell = null;
  let mediaSource = null;

  try {
    const { config, paths } = loadOrCreateConfig(tmpDir, {
      bindAddress: '127.0.0.1',
      ffmpegPath,
      alwaysOnPrebuffer: false,
      localClipDumpEnabled: true,
    });
    mediaSource = new SyntheticFfmpegMediaSource({
      ffmpegPath,
      durationSeconds: 4,
    });
    doorbell = new RingHksvDoorbell({ config, paths, mediaSource });

    doorbell.controller.recordingManagement.handleSelectedCameraRecordingConfigurationWrite(SAMPLE_SELECTED_CONFIG_BASE64);
    doorbell.controller.recordingManagement.recordingManagementService
      .getCharacteristic(Characteristic.Active)
      .setValue(Characteristic.Active.ACTIVE);
    doorbell.controller.recordingManagement.recordingManagementService
      .getCharacteristic(Characteristic.RecordingAudioActive)
      .setValue(1);

    await doorbell.handleRingEvent({ type: 'ding', at: Date.now() });
    const ac = new AbortController();
    const packets = [];
    for await (const pkt of doorbell.handleRecordingStreamRequest(99, ac.signal)) {
      packets.push(pkt);
      if (packets.length >= 2) {
        ac.abort();
        doorbell.closeRecordingStream(99, undefined);
        break;
      }
    }

    assert.equal(packets.length, 2);
    assert.equal(peekBoxHeader(packets[0].data).type, 'ftyp');
    assert.equal(peekBoxHeader(packets[1].data).type, 'moof');

    // Decode the combined HKSV fMP4 stream (ftyp+moov + moof+mdat) with real FFmpeg to prove media validity
    const combinedFmp4 = Buffer.concat([packets[0].data, packets[1].data]);
    await verifyMp4DecodesWithFfmpeg(ffmpegPath, combinedFmp4, { timeoutMs: 6000 });

    // Verify with ffprobe that the fMP4 fragment starts with a video keyframe (flags=K) and contains both H.264 video (~48 frames for 2s) and AAC audio
    if (fs.existsSync('/usr/bin/ffprobe')) {
      const probeOut = execFileSync(
        '/usr/bin/ffprobe',
        ['-hide_banner', '-show_packets', '-of', 'compact', '-'],
        { input: combinedFmp4, timeout: 5000 },
      ).toString('utf8');
      const vPackets = probeOut.split('\n').filter((l) => l.includes('codec_type=video'));
      const aPackets = probeOut.split('\n').filter((l) => l.includes('codec_type=audio'));
      assert.ok(vPackets.length >= 40, `Expected >= 40 video frames in 2s fragment, got ${vPackets.length}`);
      assert.ok(aPackets.length >= 50, `Expected >= 50 audio frames in 2s fragment, got ${aPackets.length}`);
      assert.match(vPackets[0], /flags=K/, 'First video packet in fragment must be an IDR keyframe');
    }

    // Verify Live View real FFmpeg SRTP video + Opus audio encryption and timestamp scaling with a bounded deadline
    const prepResp = await new Promise((resolve, reject) => {
      doorbell.prepareStream(
        {
          sessionID: 'session-real-ffmpeg',
          sourceAddress: '127.0.0.1',
          targetAddress: '127.0.0.1',
          addressVersion: 'ipv4',
          video: {
            port: hkVideoReceiver.port,
            srtpCryptoSuite: 0,
            srtp_key: crypto.randomBytes(16),
            srtp_salt: crypto.randomBytes(14),
          },
          audio: {
            port: hkAudioReceiver.port,
            srtpCryptoSuite: 0,
            srtp_key: crypto.randomBytes(16),
            srtp_salt: crypto.randomBytes(14),
          },
        },
        (err, res) => (err ? reject(err) : resolve(res)),
      );
    });

    const hkVideoDecoder = new SrtpSession({
      profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
      keys: {
        localMasterKey: prepResp.video.srtp_key,
        localMasterSalt: prepResp.video.srtp_salt,
        remoteMasterKey: prepResp.video.srtp_key,
        remoteMasterSalt: prepResp.video.srtp_salt,
      },
    });
    const hkAudioDecoder = new SrtpSession({
      profile: SRTP_PROFILE_AES_CM_128_HMAC_SHA1_80,
      keys: {
        localMasterKey: prepResp.audio.srtp_key,
        localMasterSalt: prepResp.audio.srtp_salt,
        remoteMasterKey: prepResp.audio.srtp_key,
        remoteMasterSalt: prepResp.audio.srtp_salt,
      },
    });

    let decryptedVideoCount = 0;
    const audioTimestamps = [];

    const mediaReceivedPromise = new Promise((resolve) => {
      const checkDone = () => {
        if (decryptedVideoCount >= 1 && audioTimestamps.length >= 2) {
          resolve();
        }
      };
      hkVideoReceiver.socket.on('message', (msg) => {
        try {
          const dec = verifyAndDecryptSrtp(hkVideoDecoder, msg);
          const rtp = RtpPacket.deSerialize(dec);
          if (rtp.header.payloadType === 99) {
            decryptedVideoCount++;
            checkDone();
          }
        } catch {}
      });
      hkAudioReceiver.socket.on('message', (msg) => {
        try {
          const dec = verifyAndDecryptSrtp(hkAudioDecoder, msg);
          const rtp = RtpPacket.deSerialize(dec);
          if (rtp.header.payloadType === 110) {
            audioTimestamps.push(rtp.header.timestamp);
            checkDone();
          }
        } catch {}
      });
    });

    await new Promise((resolve, reject) => {
      doorbell.handleStreamRequest(
        {
          sessionID: 'session-real-ffmpeg',
          type: StreamRequestTypes.START,
          video: { width: 640, height: 360, fps: 15, max_bit_rate: 500, pt: 99, mtu: 1378 },
          audio: { codec: 'OPUS', channel: 1, sample_rate: 24, max_bit_rate: 24, pt: 110, packet_time: 20 },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });

    await withTimeoutAndAbort(mediaReceivedPromise, {
      timeoutMs: 8000,
      label: 'Real FFmpeg Live View SRTP media receipt',
    });
    assert.ok(decryptedVideoCount >= 1);
    assert.ok(audioTimestamps.length >= 2);
    assert.equal((audioTimestamps[1] - audioTimestamps[0]) >>> 0, 480);

    doorbell.destroy();
    doorbell = null;

    // Verify the archived local .mp4 clip in clipsDir also decodes cleanly with real FFmpeg
    const clips = fs.readdirSync(paths.clipsDir).filter((n) => n.endsWith('.mp4'));
    assert.equal(clips.length, 1);
    await verifyMp4DecodesWithFfmpeg(ffmpegPath, path.join(paths.clipsDir, clips[0]), { timeoutMs: 6000 });
  } finally {
    doorbell?.destroy();
    mediaSource?.destroy();
    hkVideoReceiver.close();
    hkAudioReceiver.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
