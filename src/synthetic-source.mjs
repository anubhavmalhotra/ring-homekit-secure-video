import { SpawnedFfmpeg, generateFallbackJpeg } from './media-pipeline.mjs';

/**
 * Local FFmpeg `lavfi` media source used for:
 * 1. Deterministic E2E testing of HomeKit Secure Video (`fMP4` DataStream) and Live View SRTP
 * 2. Pre-Ring-auth HomeKit pairing verification (`standby_awaiting_ring_auth`)
 */
export class SyntheticFfmpegMediaSource {
  constructor({
    ffmpegPath = '/usr/bin/ffmpeg',
    name = 'Ring Doorbell (Standby)',
    model = 'Ring Video Doorbell',
    serialNumber = 'RING-HKSV-STANDBY',
    durationSeconds = 6,
  } = {}) {
    this.ffmpegPath = ffmpegPath;
    this.name = name;
    this.model = model;
    this.serialNumber = serialNumber;
    this.firmwareRevision = '1.0.0';
    this.durationSeconds = durationSeconds;
    this.hasBattery = false;
    this.batteryLevel = null;
    this.hasLowBattery = false;
    this.isCharging = false;
    this.activeProcesses = new Set();
  }

  startEventListeners() {}
  stopEventListeners() {}

  async attachFfmpeg(consumerId, ffmpegOptions, { label = consumerId, onExit } = {}) {
    const transcodeVideo = ffmpegOptions.video !== false;
    const dur = Math.max(1, Number(this.durationSeconds) || 6);
    const ffmpegArgs = [
      '-hide_banner',
      '-loglevel', 'error',
      '-re',
      '-f', 'lavfi',
      '-i', `testsrc=size=1280x720:rate=24:duration=${dur}[out0];sine=frequency=440:sample_rate=32000:duration=${dur}[out1]`,
      ...(ffmpegOptions.audio || ['-acodec', 'aac']),
      ...(transcodeVideo ? ffmpegOptions.video || ['-vcodec', 'libx264'] : []),
      ...(ffmpegOptions.output || []),
    ];

    let stopped = false;
    let ff = null;
    const cleanup = () => {
      if (stopped) return;
      stopped = true;
      if (ff) {
        this.activeProcesses.delete(ff);
        try {
          ff.stop();
        } catch {}
      }
      try {
        onExit?.();
      } catch {}
    };

    ff = new SpawnedFfmpeg({
      ffmpegArgs,
      ffmpegPath: this.ffmpegPath,
      stdoutCallback: ffmpegOptions.stdoutCallback,
      exitCallback: () => cleanup(),
      logLabel: label,
      logger: {
        error: () => {},
        info: () => {},
      },
    });
    this.activeProcesses.add(ff);

    return {
      session: null,
      stop: cleanup,
    };
  }

  async fetchCloudSnapshot() {
    return generateFallbackJpeg({ width: 1280, height: 720, ffmpegPath: this.ffmpegPath });
  }

  destroy() {
    for (const ff of [...this.activeProcesses]) {
      try {
        ff.stop();
      } catch {}
    }
    this.activeProcesses.clear();
  }
}
