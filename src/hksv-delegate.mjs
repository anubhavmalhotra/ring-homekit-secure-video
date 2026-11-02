import {
  Accessory,
  AudioBitrate,
  AudioRecordingCodecType,
  AudioRecordingSamplerate,
  AudioStreamingCodecType,
  AudioStreamingSamplerate,
  CameraController,
  Characteristic,
  DoorbellController,
  EventTriggerOption,
  H264Level,
  H264Profile,
  HDSProtocolError,
  HDSProtocolSpecificErrorReason,
  MediaContainerType,
  SRTPCryptoSuites,
  Service,
  StreamRequestTypes,
  VideoCodecType,
  uuid,
} from '@homebridge/hap-nodejs';
import { generateSrtpOptions } from '@homebridge/camera-utils';
import { Fmp4StreamSegmenter, PrebufferHub } from './fmp4.mjs';
import {
  LocalClipRecorder,
  SafeReturnAudioTranscoder,
  attachConsumerToRingSession,
  bindUdpSocket,
  buildHksvFfmpegOptions,
  buildLiveStreamFfmpegOptions,
  createLiveStreamProxy,
  extractJpegFromFmp4,
  generateFallbackJpeg,
  withTimeoutAndAbort,
} from './media-pipeline.mjs';
import { pruneClips } from './config.mjs';

export const MAX_EVENT_HOLD_MS = 90_000;
export const MAX_SINGLE_CLIP_BYTES = 128 * 1024 * 1024;

/**
 * Adapter wrapping a real `ring-client-api` `RingCamera` with reference-counted
 * WebRTC `StreamingSession` sharing so concurrent HKSV recording and Apple Home
 * Live View never open conflicting calls to the doorbell.
 */
export class RingCameraMediaSource {
  constructor(camera, { ffmpegPath = '/usr/bin/ffmpeg', startupTimeoutMs = 7000, onEvent } = {}) {
    this.camera = camera;
    this.ffmpegPath = ffmpegPath;
    this.startupTimeoutMs = startupTimeoutMs;
    this.onEvent = onEvent;
    this.activeSession = null;
    this.sessionPromise = null;
    this.consumers = new Set();
    this.idleTimer = null;
    this.subscriptions = [];
    this.destroyed = false;
  }

  get name() {
    return this.camera.name || 'Ring Doorbell';
  }

  get model() {
    return this.camera.model || 'Ring Video Doorbell';
  }

  get serialNumber() {
    return String(this.camera.data?.device_id || this.camera.id || 'RING-DOORBELL');
  }

  get firmwareRevision() {
    return String(this.camera.data?.firmware_version || '1.0.0');
  }

  get batteryLevel() {
    return this.camera.batteryLevel;
  }

  get hasBattery() {
    return Boolean(this.camera.hasBattery);
  }

  get hasLowBattery() {
    return Boolean(this.camera.hasLowBattery);
  }

  get isCharging() {
    return Boolean(this.camera.isCharging);
  }

  startEventListeners() {
    this.stopEventListeners();
    if (this.destroyed) return;
    if (this.camera.onDoorbellPressed) {
      this.subscriptions.push(
        this.camera.onDoorbellPressed.subscribe((ding) => {
          this.onEvent?.({ type: 'ding', ding, at: Date.now() });
        }),
      );
    }
    if (this.camera.onMotionDetected) {
      this.subscriptions.push(
        this.camera.onMotionDetected.subscribe((motion) => {
          this.onEvent?.({ type: 'motion', motion: Boolean(motion), at: Date.now() });
        }),
      );
    }
  }

  stopEventListeners() {
    for (const sub of this.subscriptions) {
      try {
        sub.unsubscribe();
      } catch {}
    }
    this.subscriptions = [];
  }

  async acquireSession(consumerId, { signal, deadlineMs } = {}) {
    if (this.destroyed) {
      throw new Error('RingCameraMediaSource is destroyed');
    }
    const deadline = deadlineMs || Date.now() + this.startupTimeoutMs;
    const remainingMs = () => Math.max(50, deadline - Date.now());

    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
    this.consumers.add(consumerId);

    if (this.activeSession && !this.activeSession.hasEnded) {
      return this.activeSession;
    }

    if (!this.sessionPromise) {
      let attemptDisposed = false;
      const attemptPromise = (async () => {
        const rawCallPromise = Promise.resolve().then(() => this.camera.startLiveCall());
        // Ensure a late-resolving startLiveCall() after timeout, abort, replacement, or destroy is always stopped
        rawCallPromise
          .then((lateSession) => {
            if (attemptDisposed || this.destroyed) {
              try {
                lateSession?.stop();
              } catch {}
            }
          })
          .catch(() => {});

        try {
          const session = await withTimeoutAndAbort(rawCallPromise, {
            timeoutMs: remainingMs(),
            label: 'Ring startLiveCall',
          });
          if (
            attemptDisposed ||
            this.destroyed ||
            this.consumers.size === 0 ||
            this.sessionPromise !== attemptPromise
          ) {
            attemptDisposed = true;
            try {
              session?.stop();
            } catch {}
            throw new Error('Ring live call aborted because MediaSource was destroyed or all consumers left');
          }
          this.activeSession = session;
          session.onCallEnded.subscribe(() => {
            if (this.activeSession === session) {
              this.activeSession = null;
            }
          });
          return session;
        } catch (err) {
          attemptDisposed = true;
          throw err;
        }
      })().finally(() => {
        if (this.sessionPromise === attemptPromise) {
          this.sessionPromise = null;
        }
      });
      this.sessionPromise = attemptPromise;
    }

    try {
      const session = await withTimeoutAndAbort(this.sessionPromise, {
        timeoutMs: remainingMs(),
        signal,
        label: 'Ring acquireSession',
      });
      if (this.destroyed || !this.consumers.has(consumerId)) {
        if (this.consumers.size === 0 && this.activeSession === session) {
          this.activeSession = null;
          try {
            session.stop();
          } catch {}
        }
        throw new Error('Consumer was released while acquiring Ring session');
      }
      return session;
    } catch (err) {
      this.consumers.delete(consumerId);
      if (this.consumers.size === 0 && this.activeSession) {
        const s = this.activeSession;
        this.activeSession = null;
        try {
          s.stop();
        } catch {}
      }
      throw err;
    }
  }

  releaseSession(consumerId, graceMs = 1500) {
    this.consumers.delete(consumerId);
    if (this.consumers.size > 0) {
      return;
    }
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
    if (graceMs <= 0 || this.destroyed) {
      if (this.activeSession) {
        const session = this.activeSession;
        this.activeSession = null;
        try {
          session.stop();
        } catch {}
      }
      return;
    }
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (this.consumers.size === 0 && this.activeSession) {
        const session = this.activeSession;
        this.activeSession = null;
        try {
          session.stop();
        } catch {}
      }
    }, graceMs);
    this.idleTimer.unref?.();
  }

  async attachFfmpeg(consumerId, ffmpegOptions, { label, onExit, signal } = {}) {
    const deadlineMs = Date.now() + this.startupTimeoutMs;
    const session = await this.acquireSession(consumerId, { signal, deadlineMs });
    try {
      const consumer = await attachConsumerToRingSession(session, ffmpegOptions, {
        ffmpegPath: this.ffmpegPath,
        label: label || consumerId,
        signal,
        startupTimeoutMs: this.startupTimeoutMs,
        deadlineMs,
        onExit: () => {
          this.releaseSession(consumerId);
          onExit?.();
        },
      });
      if (this.destroyed || !this.consumers.has(consumerId)) {
        consumer.stop();
        this.releaseSession(consumerId, 0);
        throw new Error('Ring consumer stopped during attachFfmpeg');
      }
      return {
        session,
        stop: () => {
          consumer.stop();
          this.releaseSession(consumerId);
        },
      };
    } catch (err) {
      this.releaseSession(consumerId, 0);
      throw err;
    }
  }

  async fetchCloudSnapshot() {
    if (this.destroyed || this.camera.snapshotsAreBlocked) {
      return null;
    }
    try {
      return await this.camera.getSnapshot();
    } catch {
      return null;
    }
  }

  destroy() {
    this.destroyed = true;
    this.stopEventListeners();
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
    this.consumers.clear();
    if (this.activeSession) {
      try {
        this.activeSession.stop();
      } catch {}
      this.activeSession = null;
    }
  }
}

/**
 * Complete HomeKit Doorbell + HomeKit Secure Video (`CameraRecordingDelegate` & `CameraStreamingDelegate`)
 * bridge implementation.
 */
export class RingHksvDoorbell {
  constructor({ config, paths, mediaSource, onStatusChange }) {
    this.config = config;
    this.paths = paths;
    this.mediaSource = mediaSource;
    this.onStatusChange = onStatusChange;

    this.recordingActive = false;
    this.recordingConfiguration = undefined;
    this.pendingRecordingConfiguration = undefined;
    this.activeRecordingStreamId = null;
    this.activeStreamAbortController = null;

    this.captureConsumer = null;
    this.captureHub = null;
    this.captureClipRecorder = null;
    this.captureConfigSig = null;
    this.captureStartupPromise = null;
    this.captureGeneration = 0;
    this.captureHoldTimer = null;
    this.captureMaxTimer = null;
    this.prebufferRetryTimer = null;
    this.prebufferRetryCount = 0;
    this.captureStartedAt = 0;
    this.captureEnding = false;
    this.motionDetected = false;
    this.destroyed = false;

    this.liveSessions = new Map();
    this.cachedSnapshot = null;
    this.cachedSnapshotAt = 0;
    this.fallbackSnapshot = null;

    this.stats = {
      motionEvents: 0,
      dingEvents: 0,
      hksvStreamsServed: 0,
      hksvFragmentsSent: 0,
      localClipsSaved: 0,
      lastEventAt: null,
      lastEventType: null,
      lastClipPath: null,
      lastError: null,
    };

    this.accessory = this.#buildAccessory();
  }

  #buildAccessory() {
    const accessoryUuid = uuid.generate(`ring-hksv:${this.config.username}`);
    const accessory = new Accessory(this.config.accessoryName, accessoryUuid);

    accessory
      .getService(Service.AccessoryInformation)
      .setCharacteristic(Characteristic.Manufacturer, 'Ring HKSV Bridge')
      .setCharacteristic(Characteristic.Model, this.mediaSource.model)
      .setCharacteristic(Characteristic.SerialNumber, this.mediaSource.serialNumber)
      .setCharacteristic(Characteristic.FirmwareRevision, this.mediaSource.firmwareRevision);

    const streamingResolutions = [
      [1920, 1080, 30],
      [1920, 1080, 24],
      [1920, 1080, 15],
      [1280, 720, 30],
      [1280, 720, 24],
      [1280, 720, 15],
      [1024, 768, 30],
      [640, 480, 30],
      [640, 360, 30],
      [480, 360, 30],
      [480, 270, 30],
      [320, 240, 30],
      [320, 240, 15],
      [320, 180, 30],
      [320, 180, 15],
    ];

    const recordingResolutions = [
      [1920, 1080, 30],
      [1920, 1080, 24],
      [1920, 1080, 15],
      [1600, 1200, 30],
      [1600, 1200, 24],
      [1600, 1200, 15],
      [1440, 1080, 30],
      [1440, 1080, 24],
      [1440, 1080, 15],
      [1280, 960, 30],
      [1280, 960, 24],
      [1280, 960, 15],
      [1280, 720, 30],
      [1280, 720, 24],
      [1280, 720, 15],
      [1024, 768, 30],
      [1024, 768, 24],
      [1024, 768, 15],
    ];

    const streamingOptions = {
      supportedCryptoSuites: [SRTPCryptoSuites.AES_CM_128_HMAC_SHA1_80],
      video: {
        codec: {
          profiles: [H264Profile.BASELINE, H264Profile.MAIN, H264Profile.HIGH],
          levels: [H264Level.LEVEL3_1, H264Level.LEVEL3_2, H264Level.LEVEL4_0],
        },
        resolutions: streamingResolutions,
      },
      audio: {
        twoWayAudio: true,
        codecs: [
          {
            type: AudioStreamingCodecType.OPUS,
            samplerate: [AudioStreamingSamplerate.KHZ_16, AudioStreamingSamplerate.KHZ_24],
          },
        ],
      },
    };

    const recordingOptions = {
      prebufferLength: 4000,
      overrideEventTriggerOptions: [EventTriggerOption.MOTION, EventTriggerOption.DOORBELL],
      mediaContainerConfiguration: [
        {
          type: MediaContainerType.FRAGMENTED_MP4,
          fragmentLength: 4000,
        },
      ],
      video: {
        type: VideoCodecType.H264,
        parameters: {
          profiles: [H264Profile.BASELINE, H264Profile.MAIN, H264Profile.HIGH],
          levels: [H264Level.LEVEL3_1, H264Level.LEVEL3_2, H264Level.LEVEL4_0],
        },
        resolutions: recordingResolutions,
      },
      audio: {
        codecs: [
          {
            type: AudioRecordingCodecType.AAC_LC,
            audioChannels: 1,
            bitrateMode: AudioBitrate.VARIABLE,
            samplerate: [
              AudioRecordingSamplerate.KHZ_16,
              AudioRecordingSamplerate.KHZ_24,
              AudioRecordingSamplerate.KHZ_32,
              AudioRecordingSamplerate.KHZ_44_1,
              AudioRecordingSamplerate.KHZ_48,
            ],
          },
        ],
      },
    };

    this.controller = new DoorbellController({
      cameraStreamCount: 2,
      delegate: this,
      streamingOptions,
      recording: {
        options: recordingOptions,
        delegate: this,
      },
      sensors: {
        motion: true,
      },
    });

    accessory.configureController(this.controller);

    // Observe HomeKitCameraActive and RecordingAudioActive changes so capture pipeline
    // immediately enforces privacy and audio-recording toggles, including during active streams.
    this.controller.recordingManagement?.operatingModeService
      ?.getCharacteristic(Characteristic.HomeKitCameraActive)
      ?.on('change', () => {
        if (!this.isHomeKitCameraActive()) {
          this.#clearPrebufferRetryTimer();
          this.#stopCapturePipeline({ planned: false, clearRetry: true });
          for (const sessionID of [...this.liveSessions.keys()]) {
            this.#stopLiveStream(sessionID);
          }
        } else if (this.#shouldMaintainPrebuffer()) {
          this.#ensureCapturePipeline('prebuffer').catch(() => {});
        }
        this.#emitStatus();
      });

    this.controller.recordingManagement?.recordingManagementService
      ?.getCharacteristic(Characteristic.RecordingAudioActive)
      ?.on('change', () => {
        const audioAllowed = this.isRecordingAudioActive();
        if (!audioAllowed && this.captureHub?.includeAudio) {
          // Immediately terminate any audio-bearing pipeline when RecordingAudioActive is disabled
          this.#stopCapturePipeline({ planned: false });
          if (this.#shouldMaintainPrebuffer()) {
            this.#ensureCapturePipeline('prebuffer').catch(() => {});
          }
        } else if (
          this.captureConsumer &&
          this.activeRecordingStreamId === null &&
          this.captureConfigSig !== this.#computeCaptureConfigSig()
        ) {
          this.#stopCapturePipeline({ planned: false });
          if (this.#shouldMaintainPrebuffer()) {
            this.#ensureCapturePipeline('prebuffer').catch(() => {});
          }
        }
        this.#emitStatus();
      });

    if (this.mediaSource.hasBattery) {
      this.batteryService = accessory.addService(Service.Battery, `${this.config.accessoryName} Battery`);
      this.updateBatteryStatus();
    }

    return accessory;
  }

  getActivePartPaths() {
    if (this.captureClipRecorder && !this.captureClipRecorder.closed && this.captureClipRecorder.tmpPath) {
      return new Set([this.captureClipRecorder.tmpPath]);
    }
    return new Set();
  }

  setMediaSource(newMediaSource) {
    if (!newMediaSource || newMediaSource === this.mediaSource) return;
    const prev = this.mediaSource;
    this.mediaSource = newMediaSource;
    try {
      prev?.destroy();
    } catch {}
    try {
      this.accessory
        .getService(Service.AccessoryInformation)
        ?.setCharacteristic(Characteristic.Model, this.mediaSource.model)
        .setCharacteristic(Characteristic.SerialNumber, this.mediaSource.serialNumber)
        .setCharacteristic(Characteristic.FirmwareRevision, this.mediaSource.firmwareRevision);
    } catch {}
    if (this.mediaSource.hasBattery && !this.batteryService) {
      this.batteryService = this.accessory.addService(Service.Battery, `${this.config.accessoryName} Battery`);
    }
    this.updateBatteryStatus();
    this.mediaSource.startEventListeners();
    if (this.#shouldMaintainPrebuffer()) {
      this.#ensureCapturePipeline('prebuffer').catch(() => {});
    }
    this.#emitStatus();
  }

  updateBatteryStatus() {
    if (!this.batteryService) return;
    const level = this.mediaSource.batteryLevel;
    if (typeof level === 'number' && level >= 0 && level <= 100) {
      this.batteryService.updateCharacteristic(Characteristic.BatteryLevel, level);
    }
    this.batteryService.updateCharacteristic(
      Characteristic.StatusLowBattery,
      this.mediaSource.hasLowBattery
        ? Characteristic.StatusLowBattery.BATTERY_LEVEL_LOW
        : Characteristic.StatusLowBattery.BATTERY_LEVEL_NORMAL,
    );
    this.batteryService.updateCharacteristic(
      Characteristic.ChargingState,
      this.mediaSource.isCharging
        ? Characteristic.ChargingState.CHARGING
        : Characteristic.ChargingState.NOT_CHARGING,
    );
  }

  isHomeKitCameraActive() {
    const val = this.controller.recordingManagement?.operatingModeService
      .getCharacteristic(Characteristic.HomeKitCameraActive).value;
    return val === undefined || Boolean(val);
  }

  isRecordingAudioActive() {
    const val = this.controller.recordingManagement?.recordingManagementService
      .getCharacteristic(Characteristic.RecordingAudioActive).value;
    return Boolean(val);
  }

  #shouldMaintainPrebuffer() {
    return Boolean(
      !this.destroyed &&
        this.config.alwaysOnPrebuffer &&
        this.recordingActive &&
        this.recordingConfiguration &&
        this.isHomeKitCameraActive(),
    );
  }

  #computeCaptureConfigSig(configObj = this.recordingConfiguration) {
    const includeAudio = configObj ? this.isRecordingAudioActive() : true;
    return JSON.stringify({
      includeAudio,
      config: configObj || null,
    });
  }

  // ---- CameraRecordingDelegate implementation ----

  updateRecordingActive(active) {
    this.recordingActive = Boolean(active);
    if (!this.recordingActive) {
      this.pendingRecordingConfiguration = undefined;
      this.#clearPrebufferRetryTimer();
      this.#stopCapturePipeline({ planned: false, clearRetry: true });
    } else if (this.#shouldMaintainPrebuffer()) {
      this.#ensureCapturePipeline('prebuffer').catch(() => {});
    }
    this.#emitStatus();
  }

  updateRecordingConfiguration(configuration) {
    if (!configuration) {
      this.recordingConfiguration = undefined;
      this.pendingRecordingConfiguration = undefined;
      this.#clearPrebufferRetryTimer();
      this.#stopCapturePipeline({ planned: false, clearRetry: true });
      this.#emitStatus();
      return;
    }

    if (this.activeRecordingStreamId !== null) {
      // Retain active configuration for the in-progress recording; apply new configuration at stream end
      this.pendingRecordingConfiguration = configuration;
      this.#emitStatus();
      return;
    }

    this.recordingConfiguration = configuration;
    this.pendingRecordingConfiguration = undefined;
    if (this.captureConsumer && this.captureConfigSig !== this.#computeCaptureConfigSig()) {
      this.#stopCapturePipeline({ planned: false });
    }
    if (this.#shouldMaintainPrebuffer()) {
      this.#ensureCapturePipeline('prebuffer').catch(() => {});
    }
    this.#emitStatus();
  }

  #onRecordingStreamFinished(streamId) {
    if (this.activeRecordingStreamId !== streamId) {
      return;
    }
    this.activeRecordingStreamId = null;
    if (this.activeStreamAbortController) {
      try {
        this.activeStreamAbortController.abort();
      } catch {}
      this.activeStreamAbortController = null;
    }

    if (this.pendingRecordingConfiguration !== undefined) {
      this.recordingConfiguration = this.pendingRecordingConfiguration;
      this.pendingRecordingConfiguration = undefined;
      if (this.captureConsumer && this.captureConfigSig !== this.#computeCaptureConfigSig()) {
        this.#stopCapturePipeline({ planned: false });
      }
    }

    if (this.#shouldMaintainPrebuffer() && !this.captureConsumer) {
      this.#ensureCapturePipeline('prebuffer').catch(() => {});
    }
  }

  async *handleRecordingStreamRequest(streamId, signal) {
    this.activeRecordingStreamId = streamId;
    const streamAc = new AbortController();
    this.activeStreamAbortController = streamAc;
    const onExternalAbort = () => streamAc.abort();
    signal?.addEventListener('abort', onExternalAbort, { once: true });
    if (signal?.aborted) {
      streamAc.abort();
    }
    const streamSignal = streamAc.signal;

    this.stats.hksvStreamsServed++;
    this.#emitStatus();

    let sentIsLast = false;

    try {
      const hub = await this.#ensureCapturePipeline('hksv_stream', { signal: streamSignal });
      if (!this.#shouldMaintainPrebuffer() && !this.captureHoldTimer) {
        this.#extendCaptureTimers();
      }

      const initSegment = await hub.waitForInit(streamSignal, 12000);
      if (streamSignal.aborted || this.activeRecordingStreamId !== streamId) {
        return;
      }
      if (hub.includeAudio && !this.isRecordingAudioActive()) {
        throw new HDSProtocolError(HDSProtocolSpecificErrorReason.UNEXPECTED_FAILURE);
      }

      // First packet MUST be MEDIA_INITIALIZATION (ftyp + moov)
      yield { data: initSegment, isLast: false };

      let lastSeq = 0;
      let lastYieldedData = null;
      const initialBuffered = hub.getPrebufferSnapshot();
      for (let i = 0; i < initialBuffered.length; i++) {
        if (streamSignal.aborted || this.activeRecordingStreamId !== streamId) {
          return;
        }
        if (hub.includeAudio && !this.isRecordingAudioActive()) {
          throw new HDSProtocolError(HDSProtocolSpecificErrorReason.UNEXPECTED_FAILURE);
        }
        const item = initialBuffered[i];
        lastSeq = item.seq;
        lastYieldedData = item.data;
        const hasMore = i < initialBuffered.length - 1 || hub.hasFragmentsAfter(lastSeq);
        const isLastBuffered = Boolean(!hasMore && hub.plannedClose && !hub.failedClose);
        this.stats.hksvFragmentsSent++;
        yield { data: item.data, isLast: isLastBuffered };
        if (isLastBuffered) {
          sentIsLast = true;
          return;
        }
      }

      while (!sentIsLast && !streamSignal.aborted && this.activeRecordingStreamId === streamId) {
        const nextItem = await hub.waitForFragmentAfter(lastSeq, streamSignal, 12000);
        if (streamSignal.aborted || this.activeRecordingStreamId !== streamId) {
          return;
        }
        if (!nextItem) {
          if (hub.plannedClose && !hub.failedClose && lastYieldedData) {
            if (hub.includeAudio && !this.isRecordingAudioActive()) {
              throw new HDSProtocolError(HDSProtocolSpecificErrorReason.UNEXPECTED_FAILURE);
            }
            this.stats.hksvFragmentsSent++;
            yield { data: lastYieldedData, isLast: true };
            sentIsLast = true;
            break;
          }
          throw new HDSProtocolError(HDSProtocolSpecificErrorReason.UNEXPECTED_FAILURE);
        }
        if (hub.includeAudio && !this.isRecordingAudioActive()) {
          throw new HDSProtocolError(HDSProtocolSpecificErrorReason.UNEXPECTED_FAILURE);
        }
        lastSeq = nextItem.seq;
        lastYieldedData = nextItem.data;
        const hasMoreQueued = hub.hasFragmentsAfter(lastSeq);
        const isFinal = Boolean(
          !hasMoreQueued && (hub.plannedClose || (this.captureEnding && !hub.failedClose)),
        );
        this.stats.hksvFragmentsSent++;
        yield { data: nextItem.data, isLast: isFinal };
        if (isFinal) {
          sentIsLast = true;
          break;
        }
      }
    } catch (err) {
      this.stats.lastError = err.message;
      this.#emitStatus();
      if (!streamSignal.aborted && this.activeRecordingStreamId === streamId) {
        if (err instanceof HDSProtocolError) {
          throw err;
        }
        throw new HDSProtocolError(HDSProtocolSpecificErrorReason.UNEXPECTED_FAILURE);
      }
    } finally {
      signal?.removeEventListener('abort', onExternalAbort);
      this.#onRecordingStreamFinished(streamId);
    }
  }

  acknowledgeStream(streamId) {
    this.#onRecordingStreamFinished(streamId);
  }

  closeRecordingStream(streamId, _reason) {
    this.#onRecordingStreamFinished(streamId);
  }

  // ---- Event & Capture Pipeline Management ----

  #ensureEventClipRecorder(triggerReason, hub) {
    if (!this.config.localClipDumpEnabled) return;
    if (this.captureClipRecorder && !this.captureClipRecorder.closed) return;
    let recorder = null;
    try {
      recorder = new LocalClipRecorder(this.paths.clipsDir, triggerReason);
      if (hub?.initSegment) {
        recorder.writeInit(hub.initSegment);
        for (const item of hub.getPrebufferSnapshot()) {
          recorder.writeFragment(item.data);
        }
      }
      this.captureClipRecorder = recorder;
    } catch (err) {
      try {
        recorder?.abort();
      } catch {}
      this.stats.lastError = `Clip recorder open failed: ${err.message}`;
    }
  }

  #finalizeEventClipRecorder() {
    const recorder = this.captureClipRecorder;
    this.captureClipRecorder = null;
    if (!recorder) return;

    const result = recorder.close();
    if (!result) return;

    if (!result.ok && result.error) {
      this.stats.lastError = `Clip durability error: ${result.error.message}`;
      return;
    }
    if (result.ok && !result.empty && result.path) {
      this.stats.localClipsSaved++;
      this.stats.lastClipPath = result.path;
      try {
        pruneClips(this.paths.clipsDir, {
          maxAgeMs: this.config.clipRetentionDays * 86400_000,
          maxTotalBytes: this.config.maxClipsTotalBytes,
          activePartPaths: this.getActivePartPaths(),
        });
      } catch {}
    }
  }

  async handleRingEvent(event) {
    const type = event?.type || 'motion';
    if (type === 'motion' && event.motion === false) {
      return;
    }

    this.stats.lastEventAt = new Date().toISOString();
    this.stats.lastEventType = type;

    if (type === 'ding') {
      this.stats.dingEvents++;
      try {
        this.controller.ringDoorbell();
      } catch {}
    } else {
      this.stats.motionEvents++;
    }
    this.#emitStatus();

    if (!this.isHomeKitCameraActive()) {
      return;
    }

    let targetHub = null;
    try {
      targetHub = await this.#ensureCapturePipeline(type);
      if (this.destroyed || !this.isHomeKitCameraActive() || this.captureHub !== targetHub) {
        return;
      }
      this.#ensureEventClipRecorder(type, targetHub);
      this.#extendCaptureTimers();
      await targetHub.waitForInit(undefined, 12000);
      await targetHub.waitForFragmentAfter(0, undefined, 12000);
      if (this.destroyed || !this.isHomeKitCameraActive() || this.captureHub !== targetHub) {
        return;
      }
      this.#setMotionDetected(true);
    } catch (err) {
      this.stats.lastError = `Event capture failed: ${err.message}`;
      this.#emitStatus();
      if (targetHub && this.captureHub === targetHub) {
        this.#stopCapturePipeline({ planned: false, expectedHub: targetHub });
      }
    }
  }

  async #ensureCapturePipeline(triggerReason, { signal } = {}) {
    while (true) {
      if (this.destroyed || !this.isHomeKitCameraActive()) {
        throw new Error('Capture pipeline rejected because camera is disabled or destroyed');
      }
      if (signal?.aborted) {
        throw new Error('Capture pipeline request aborted');
      }
      const desiredSig = this.#computeCaptureConfigSig();
      if (
        this.captureHub &&
        !this.captureHub.closed &&
        this.captureConsumer &&
        !this.captureEnding &&
        this.captureConfigSig === desiredSig
      ) {
        if (triggerReason !== 'prebuffer' && triggerReason !== 'hksv_stream') {
          this.#ensureEventClipRecorder(triggerReason, this.captureHub);
        }
        return this.captureHub;
      }

      if (this.captureStartupPromise) {
        try {
          await withTimeoutAndAbort(this.captureStartupPromise, {
            timeoutMs: 10000,
            signal,
            label: 'captureStartupPromise',
          });
        } catch (err) {
          if (signal?.aborted || this.destroyed || !this.isHomeKitCameraActive()) {
            throw err;
          }
        }
        continue;
      }

      break;
    }

    if (this.destroyed || !this.isHomeKitCameraActive()) {
      throw new Error('Capture pipeline rejected because camera is disabled or destroyed');
    }

    if (this.captureConsumer || this.captureHub) {
      this.#stopCapturePipeline({ planned: false });
    }

    const gen = ++this.captureGeneration;
    let startupPromise;
    startupPromise = (async () => {
      const configSnapshot = this.recordingConfiguration;
      const includeAudio = configSnapshot ? this.isRecordingAudioActive() : true;
      const configSig = this.#computeCaptureConfigSig(configSnapshot);
      const ffmpegOpts = buildHksvFfmpegOptions(configSnapshot, { includeAudio });

      const prebufferMs = configSnapshot?.prebufferLength || 4000;
      const fragLengthMs =
        ffmpegOpts.fragmentDurationMs ||
        configSnapshot?.mediaContainerConfiguration?.fragmentLength ||
        2000;
      const maxPrebufferFragments = Math.max(1, Math.ceil(prebufferMs / fragLengthMs));

      const hub = new PrebufferHub({
        maxPrebufferFragments,
        includeAudio,
        generation: gen,
      });
      const segmenter = new Fmp4StreamSegmenter();

      this.captureHub = hub;
      this.captureConfigSig = configSig;
      this.captureStartedAt = Date.now();
      this.captureEnding = false;

      if (triggerReason !== 'prebuffer') {
        this.#ensureEventClipRecorder(triggerReason, null);
      }

      let snapshotExtracted = false;
      segmenter.on('init', (initBuf) => {
        if (this.captureHub !== hub) return;
        this.prebufferRetryCount = 0;
        this.captureClipRecorder?.writeInit(initBuf);
        hub.onInit(initBuf);
      });

      segmenter.on('fragment', (fragmentBuf) => {
        if (this.captureHub !== hub) return;
        if (this.captureClipRecorder && !this.captureClipRecorder.closed) {
          this.captureClipRecorder.writeFragment(fragmentBuf);
          if (this.captureClipRecorder.bytesWritten >= MAX_SINGLE_CLIP_BYTES) {
            this.#finalizeEventClipRecorder();
          }
        }
        const finishingCooldown = this.captureEnding;
        if (finishingCooldown) {
          hub.plannedClose = true;
        }
        hub.onFragment(fragmentBuf);

        if (!snapshotExtracted && hub.initSegment) {
          snapshotExtracted = true;
          extractJpegFromFmp4(hub.initSegment, fragmentBuf, {
            ffmpegPath: this.config.ffmpegPath,
          })
            .then((jpeg) => {
              this.cachedSnapshot = jpeg;
              this.cachedSnapshotAt = Date.now();
            })
            .catch(() => {});
        }

        if (finishingCooldown) {
          this.#stopCapturePipeline({ planned: true, expectedHub: hub });
        }
      });

      segmenter.on('error', (err) => {
        if (this.captureHub !== hub) return;
        this.stats.lastError = `fMP4 segmenter error: ${err.message}`;
        this.#stopCapturePipeline({ planned: false, expectedHub: hub });
      });

      try {
        const attached = await this.mediaSource.attachFfmpeg(
          `hksv_capture:${gen}`,
          {
            ...ffmpegOpts,
            stdoutCallback: (chunk) => segmenter.push(chunk),
          },
          {
            label: `HKSV-fMP4-${gen}`,
            signal: triggerReason === 'hksv_stream' && !this.#shouldMaintainPrebuffer() ? signal : undefined,
            onExit: () => {
              if (this.captureHub === hub) {
                this.#stopCapturePipeline({ planned: hub.plannedClose, expectedHub: hub });
              }
            },
          },
        );

        if (
          this.destroyed ||
          !this.isHomeKitCameraActive() ||
          this.captureGeneration !== gen ||
          this.captureHub !== hub ||
          (triggerReason === 'prebuffer' && !this.#shouldMaintainPrebuffer()) ||
          (signal?.aborted && !this.#shouldMaintainPrebuffer() && !this.motionDetected)
        ) {
          try {
            attached.stop();
          } catch {}
          if (this.captureHub === hub) {
            this.#stopCapturePipeline({ planned: false, expectedHub: hub });
          } else {
            hub.markFailedEnd();
          }
          throw new Error('Capture pipeline was stopped or superseded during startup');
        }

        this.captureConsumer = attached;
        return hub;
      } catch (err) {
        if (this.captureHub === hub) {
          this.#stopCapturePipeline({ planned: false, expectedHub: hub });
        } else {
          hub.markFailedEnd();
        }
        throw err;
      }
    })();

    this.captureStartupPromise = startupPromise;
    try {
      return await startupPromise;
    } finally {
      if (this.captureStartupPromise === startupPromise) {
        this.captureStartupPromise = null;
      }
    }
  }

  #clearPrebufferRetryTimer() {
    if (this.prebufferRetryTimer) {
      clearTimeout(this.prebufferRetryTimer);
      this.prebufferRetryTimer = null;
    }
  }

  #schedulePrebufferRecovery() {
    if (this.prebufferRetryTimer || this.destroyed || !this.#shouldMaintainPrebuffer()) {
      return;
    }
    const attempt = ++this.prebufferRetryCount;
    const delayMs = Math.min(5000, 100 * 2 ** Math.min(6, attempt - 1));
    this.prebufferRetryTimer = setTimeout(() => {
      this.prebufferRetryTimer = null;
      if (this.destroyed || !this.#shouldMaintainPrebuffer() || this.captureConsumer) {
        return;
      }
      this.#ensureCapturePipeline('prebuffer').catch(() => {
        if (!this.destroyed && this.#shouldMaintainPrebuffer() && !this.captureConsumer) {
          this.#schedulePrebufferRecovery();
        }
      });
    }, delayMs);
    this.prebufferRetryTimer.unref?.();
  }

  #extendCaptureTimers() {
    const holdMs = (this.config.eventDurationSeconds || 25) * 1000;
    if (this.captureHoldTimer) {
      clearTimeout(this.captureHoldTimer);
    }
    this.captureHoldTimer = setTimeout(() => {
      this.captureHoldTimer = null;
      this.#beginCaptureCooldown();
    }, holdMs);
    this.captureHoldTimer.unref?.();

    if (!this.captureMaxTimer) {
      this.captureMaxTimer = setTimeout(() => {
        this.captureMaxTimer = null;
        this.#stopCapturePipeline({ planned: true });
      }, MAX_EVENT_HOLD_MS);
      this.captureMaxTimer.unref?.();
    }
  }

  #beginCaptureCooldown() {
    this.#setMotionDetected(false);
    this.captureEnding = true;
    // Allow up to 4.5s for the final post-roll fragment to flush with isLast=true
    if (this.captureHoldTimer) {
      clearTimeout(this.captureHoldTimer);
    }
    this.captureHoldTimer = setTimeout(() => {
      this.captureHoldTimer = null;
      this.#stopCapturePipeline({ planned: true });
    }, 4500);
    this.captureHoldTimer.unref?.();
  }

  #stopCapturePipeline({ planned = false, expectedHub = null, expectedGen = null, clearRetry = false } = {}) {
    if (expectedHub && this.captureHub && this.captureHub !== expectedHub) {
      return;
    }
    if (expectedGen !== null && this.captureGeneration !== expectedGen) {
      return;
    }
    if (clearRetry || !this.#shouldMaintainPrebuffer()) {
      this.#clearPrebufferRetryTimer();
    }

    this.captureGeneration++;
    if (this.captureHoldTimer) {
      clearTimeout(this.captureHoldTimer);
      this.captureHoldTimer = null;
    }
    if (this.captureMaxTimer) {
      clearTimeout(this.captureMaxTimer);
      this.captureMaxTimer = null;
    }
    this.#setMotionDetected(false);
    this.captureEnding = false;
    this.captureConfigSig = null;

    // Detach hub and consumer BEFORE calling consumer.stop() so synchronous onExit
    // callbacks cannot re-enter #stopCapturePipeline or overwrite hub.plannedClose.
    const consumer = this.captureConsumer;
    this.captureConsumer = null;
    const hub = this.captureHub;
    this.captureHub = null;

    try {
      hub?.markEnded({ planned });
    } catch {}

    this.#finalizeEventClipRecorder();

    try {
      consumer?.stop();
    } catch {}

    if (planned && this.#shouldMaintainPrebuffer() && this.activeRecordingStreamId === null) {
      this.#clearPrebufferRetryTimer();
      this.#ensureCapturePipeline('prebuffer').catch(() => {
        this.#schedulePrebufferRecovery();
      });
    } else if (!planned && !clearRetry && this.#shouldMaintainPrebuffer()) {
      this.#schedulePrebufferRecovery();
    }
    this.#emitStatus();
  }

  #setMotionDetected(detected) {
    const next = Boolean(detected);
    if (this.motionDetected === next) return;
    this.motionDetected = next;
    try {
      this.controller.motionService?.updateCharacteristic(Characteristic.MotionDetected, next);
    } catch {}
  }

  // ---- CameraStreamingDelegate (Snapshots & Live View) ----

  handleSnapshotRequest(request, callback) {
    this.#resolveSnapshot(request)
      .then((buf) => callback(undefined, buf))
      .catch((err) => callback(err));
  }

  async #resolveSnapshot(request) {
    // 1. Fresh frame extracted from a recent live/recording session (< 60s old)
    if (this.cachedSnapshot && Date.now() - this.cachedSnapshotAt < 60_000) {
      return this.cachedSnapshot;
    }
    // 2. Try Ring cloud snapshot if available (fast timeout so HomeKit never hangs)
    let snapTimer = null;
    const cloudSnap = await Promise.race([
      this.mediaSource.fetchCloudSnapshot(),
      new Promise((resolve) => {
        snapTimer = setTimeout(() => resolve(null), 3500);
        snapTimer.unref?.();
      }),
    ]).finally(() => {
      if (snapTimer) clearTimeout(snapTimer);
    });
    if (Buffer.isBuffer(cloudSnap) && cloudSnap.length > 0) {
      this.cachedSnapshot = cloudSnap;
      this.cachedSnapshotAt = Date.now();
      return cloudSnap;
    }
    // 3. Cached snapshot of any age if available
    if (this.cachedSnapshot) {
      return this.cachedSnapshot;
    }
    // 4. Deterministic valid JPEG fallback generated via FFmpeg
    if (!this.fallbackSnapshot) {
      this.fallbackSnapshot = await generateFallbackJpeg({
        width: request?.width || 1280,
        height: request?.height || 720,
        ffmpegPath: this.config.ffmpegPath,
      });
    }
    return this.fallbackSnapshot;
  }

  prepareStream(request, callback) {
    this.#prepareLiveStream(request)
      .then((response) => callback(undefined, response))
      .catch((err) => callback(err));
  }

  async #prepareLiveStream(request) {
    const sessionID = request.sessionID;
    if (this.destroyed || !this.isHomeKitCameraActive()) {
      throw new Error('Cannot prepare Live View stream while camera is disabled or destroyed');
    }
    const placeholder = {
      sessionID,
      preparing: true,
      stopped: false,
      reconfCount: 0,
    };
    this.liveSessions.set(sessionID, placeholder);

    let videoReturnSocket = null;
    let returnAudioSocket = null;
    try {
      videoReturnSocket = await bindUdpSocket('0.0.0.0', 0);
      returnAudioSocket = await bindUdpSocket('0.0.0.0', 0);
    } catch (err) {
      videoReturnSocket?.close();
      returnAudioSocket?.close();
      if (this.liveSessions.get(sessionID) === placeholder) {
        this.liveSessions.delete(sessionID);
      }
      throw err;
    }

    if (
      this.destroyed ||
      !this.isHomeKitCameraActive() ||
      placeholder.stopped ||
      this.liveSessions.get(sessionID) !== placeholder
    ) {
      videoReturnSocket.close();
      returnAudioSocket.close();
      if (this.liveSessions.get(sessionID) === placeholder) {
        this.liveSessions.delete(sessionID);
      }
      throw new Error('Live View stream preparation was cancelled before completion');
    }

    const videoSsrc = CameraController.generateSynchronisationSource();
    const audioSsrc = CameraController.generateSynchronisationSource();
    const videoSrtp = generateSrtpOptions();
    const audioSrtp = generateSrtpOptions();

    this.liveSessions.set(sessionID, {
      sessionID,
      preparing: false,
      stopped: false,
      reconfCount: 0,
      prepareRequest: request,
      startRequest: null,
      targetAddress: request.targetAddress,
      addressVersion: request.addressVersion,
      videoPort: request.video.port,
      videoReturnPort: videoReturnSocket.port,
      videoSrtpKey: videoSrtp.srtpKey,
      videoSrtpSalt: videoSrtp.srtpSalt,
      returnVideoSrtpKey: request.video.srtp_key,
      returnVideoSrtpSalt: request.video.srtp_salt,
      videoSsrc,
      audioPort: request.audio.port,
      audioReturnPort: returnAudioSocket.port,
      audioSrtpKey: audioSrtp.srtpKey,
      audioSrtpSalt: audioSrtp.srtpSalt,
      returnAudioSrtpKey: request.audio.srtp_key,
      returnAudioSrtpSalt: request.audio.srtp_salt,
      audioSsrc,
      videoReturnSocket,
      returnAudioSocket,
      liveProxy: null,
      consumer: null,
      returnAudioTranscoder: null,
    });

    return {
      addressOverride: this.config.bindAddress,
      video: {
        port: videoReturnSocket.port,
        ssrc: videoSsrc,
        srtp_key: videoSrtp.srtpKey,
        srtp_salt: videoSrtp.srtpSalt,
      },
      audio: {
        port: returnAudioSocket.port,
        ssrc: audioSsrc,
        srtp_key: audioSrtp.srtpKey,
        srtp_salt: audioSrtp.srtpSalt,
      },
    };
  }

  handleStreamRequest(request, callback) {
    const sessionID = request.sessionID;
    const sessionInfo = this.liveSessions.get(sessionID);
    if (!sessionInfo) {
      callback();
      return;
    }

    if (request.type === StreamRequestTypes.START) {
      this.#startLiveStream(sessionInfo, request)
        .then(() => callback())
        .catch((err) => {
          this.#stopLiveStream(sessionID);
          callback(err);
        });
      return;
    }

    if (request.type === StreamRequestTypes.RECONFIGURE) {
      this.#reconfigureLiveStream(sessionInfo, request)
        .then(() => callback())
        .catch((err) => callback(err));
      return;
    }

    if (request.type === StreamRequestTypes.STOP) {
      this.#stopLiveStream(sessionID);
      callback();
    }
  }

  async #startLiveStream(sessionInfo, startRequest) {
    sessionInfo.startRequest = startRequest;
    const liveProxy = await createLiveStreamProxy(sessionInfo, startRequest);
    if (sessionInfo.stopped || this.liveSessions.get(sessionInfo.sessionID) !== sessionInfo) {
      liveProxy.close();
      return;
    }
    sessionInfo.liveProxy = liveProxy;

    const consumerId = `live:${sessionInfo.sessionID}`;
    const ffmpegOptions = buildLiveStreamFfmpegOptions(sessionInfo, startRequest, liveProxy.proxyPorts);

    let attached;
    try {
      attached = await this.mediaSource.attachFfmpeg(consumerId, ffmpegOptions, {
        label: `LiveView-${sessionInfo.sessionID.slice(0, 8)}`,
        onExit: () => {
          if (sessionInfo.consumer === attached) {
            this.#stopLiveStream(sessionInfo.sessionID);
          }
        },
      });
    } catch (err) {
      liveProxy.close();
      throw err;
    }

    if (sessionInfo.stopped || this.liveSessions.get(sessionInfo.sessionID) !== sessionInfo) {
      try {
        attached.stop();
      } catch {}
      liveProxy.close();
      return;
    }
    sessionInfo.consumer = attached;

    // Two-way talkback audio from Apple Home to the Ring Doorbell speaker (cryptographically SRTP-authenticated)
    if (attached.session && typeof attached.session.sendAudioPacket === 'function') {
      try {
        const usingOpus = Boolean(await attached.session.isUsingOpus);
        if (sessionInfo.stopped || this.liveSessions.get(sessionInfo.sessionID) !== sessionInfo) {
          return;
        }
        const returnAudioTranscoder = new SafeReturnAudioTranscoder({
          sessionInfo,
          startRequest,
          liveProxy,
          usingOpus,
          ffmpegPath: this.config.ffmpegPath,
          onAuthenticatedTalkback: () => {
            if (!sessionInfo.stopped) {
              attached.session.activateCameraSpeaker?.();
            }
          },
          onTranscodedRtp: (rtp) => {
            if (!sessionInfo.stopped) {
              attached.session.sendAudioPacket(rtp);
            }
          },
        });
        await returnAudioTranscoder.start();
        if (sessionInfo.stopped || this.liveSessions.get(sessionInfo.sessionID) !== sessionInfo) {
          returnAudioTranscoder.stop();
          return;
        }
        sessionInfo.returnAudioTranscoder = returnAudioTranscoder;
      } catch {
        // Non-fatal if return audio fails; keep live video/audio active
      }
    }
  }

  async #reconfigureLiveStream(sessionInfo, reconfRequest) {
    if (sessionInfo.stopped || !sessionInfo.startRequest || !sessionInfo.liveProxy) {
      return;
    }
    const rev = ++sessionInfo.reconfCount;
    const mergedStartRequest = {
      ...sessionInfo.startRequest,
      video: {
        ...(sessionInfo.startRequest.video || {}),
        ...(reconfRequest.video || {}),
      },
    };
    const nextPorts = await sessionInfo.liveProxy.allocateConsumerPorts();
    if (
      sessionInfo.stopped ||
      this.liveSessions.get(sessionInfo.sessionID) !== sessionInfo ||
      sessionInfo.reconfCount !== rev
    ) {
      return;
    }

    const consumerId = `live:${sessionInfo.sessionID}:r${rev}`;
    const ffmpegOptions = buildLiveStreamFfmpegOptions(sessionInfo, mergedStartRequest, nextPorts);

    const nextConsumer = await this.mediaSource.attachFfmpeg(consumerId, ffmpegOptions, {
      label: `LiveView-${sessionInfo.sessionID.slice(0, 8)}-r${rev}`,
      onExit: () => {
        if (sessionInfo.consumer === nextConsumer) {
          this.#stopLiveStream(sessionInfo.sessionID);
        }
      },
    });

    if (
      sessionInfo.stopped ||
      this.liveSessions.get(sessionInfo.sessionID) !== sessionInfo ||
      sessionInfo.reconfCount !== rev
    ) {
      try {
        nextConsumer.stop();
      } catch {}
      return;
    }

    const prevConsumer = sessionInfo.consumer;
    sessionInfo.startRequest = mergedStartRequest;
    sessionInfo.consumer = nextConsumer;
    sessionInfo.liveProxy.activateConsumerPorts(nextPorts, mergedStartRequest);
    try {
      prevConsumer?.stop();
    } catch {}
  }

  #stopLiveStream(sessionID) {
    const sessionInfo = this.liveSessions.get(sessionID);
    if (!sessionInfo) return;
    sessionInfo.stopped = true;
    this.liveSessions.delete(sessionID);
    try {
      sessionInfo.consumer?.stop();
    } catch {}
    try {
      sessionInfo.returnAudioTranscoder?.stop();
    } catch {}
    try {
      sessionInfo.liveProxy?.close();
    } catch {}
    try {
      sessionInfo.videoReturnSocket?.close();
    } catch {}
    try {
      sessionInfo.returnAudioSocket?.close();
    } catch {}
  }

  #emitStatus() {
    this.onStatusChange?.(this.getStatus());
  }

  getStatus() {
    return {
      accessoryName: this.config.accessoryName,
      cameraModel: this.mediaSource.model,
      bindAddress: this.config.bindAddress,
      hapPort: this.config.hapPort,
      homeKitCameraActive: this.isHomeKitCameraActive(),
      hksvRecordingActive: this.recordingActive,
      hksvConfigSelected: Boolean(this.recordingConfiguration),
      recordingAudioActive: this.isRecordingAudioActive(),
      motionDetected: this.motionDetected,
      activeLiveStreams: this.liveSessions.size,
      capturePipelineActive: Boolean(this.captureConsumer),
      stats: { ...this.stats },
    };
  }

  destroy() {
    this.destroyed = true;
    this.#clearPrebufferRetryTimer();
    this.#stopCapturePipeline({ planned: false, clearRetry: true });
    for (const sessionID of [...this.liveSessions.keys()]) {
      this.#stopLiveStream(sessionID);
    }
    this.mediaSource.destroy();
  }
}
