import fs from 'node:fs';
import { Categories, HAPStorage, MDNSAdvertiser } from '@homebridge/hap-nodejs';
import { RingApi } from 'ring-client-api';
import { setFfmpegPath } from 'ring-client-api/ffmpeg';
import {
  acquireStateDirLock,
  defaultStateDir,
  loadOrCreateConfig,
  pruneClips,
  updateRefreshToken,
  writeJsonAtomic,
} from './config.mjs';
import { RingCameraMediaSource, RingHksvDoorbell } from './hksv-delegate.mjs';
import { SyntheticFfmpegMediaSource } from './synthetic-source.mjs';

export function selectRingCamera(cameras, config) {
  if (!Array.isArray(cameras) || cameras.length === 0) {
    return null;
  }
  if (config.targetCameraId !== null) {
    const byId = cameras.find((c) => c.id === config.targetCameraId);
    if (byId) return byId;
  }
  if (config.targetCameraName) {
    const lower = config.targetCameraName.toLowerCase();
    const byName = cameras.find((c) => (c.name || '').toLowerCase() === lower);
    if (byName) return byName;
  }
  const doorbot = cameras.find((c) => c.isDoorbot);
  return doorbot || cameras[0];
}

export async function startService({
  stateDir = defaultStateDir(),
  overrides = {},
  checkOnly = false,
} = {}) {
  if (checkOnly) {
    const { config, paths } = loadOrCreateConfig(stateDir, overrides);
    fs.accessSync(config.ffmpegPath, fs.constants.X_OK);
    return {
      ok: true,
      config: { ...config, ringRefreshToken: Boolean(config.ringRefreshToken), pincode: '***' },
      paths,
    };
  }

  const stateLock = acquireStateDirLock(stateDir);
  let doorbell = null;
  try {
    const { config, paths } = loadOrCreateConfig(stateDir, overrides);
    fs.accessSync(config.ffmpegPath, fs.constants.X_OK);

    setFfmpegPath(config.ffmpegPath);
    HAPStorage.setCustomStoragePath(paths.hapPersistDir);

    try {
      pruneClips(paths.clipsDir, {
        maxAgeMs: config.clipRetentionDays * 86400_000,
        maxTotalBytes: config.maxClipsTotalBytes,
        removeAllParts: true,
      });
    } catch {}

    let ringState = 'awaiting_ring_auth';
    let ringCameraName = null;
    let ringCameraId = null;
    let ringApi = null;
    let activeToken = null;
    let persistedToken = config.ringRefreshToken || null;
    let pendingTokenToPersist = null;
    let connectingRing = false;
    let stopping = false;

    const standbySource = new SyntheticFfmpegMediaSource({
      ffmpegPath: config.ffmpegPath,
      name: `${config.accessoryName} (Standby)`,
    });

    const writeStatus = () => {
      if (!doorbell) return;
      const payload = {
        updatedAt: new Date().toISOString(),
        pid: process.pid,
        ringState,
        ringCameraName,
        ringCameraId,
        ...doorbell.getStatus(),
      };
      try {
        writeJsonAtomic(paths.statusFile, payload, 0o600);
      } catch {}
    };

    doorbell = new RingHksvDoorbell({
      config,
      paths,
      mediaSource: standbySource,
      onStatusChange: () => writeStatus(),
    });

    await doorbell.accessory.publish({
      username: config.username,
      pincode: config.pincode,
      setupID: config.setupId,
      port: config.hapPort,
      bind: config.bindAddress,
      category: Categories.VIDEO_DOORBELL,
      advertiser: MDNSAdvertiser.CIAO,
    });

    const setupUri = doorbell.accessory.setupURI();
    writeJsonAtomic(
      paths.pairingFile,
      {
        accessoryName: config.accessoryName,
        username: config.username,
        pincode: config.pincode,
        setupId: config.setupId,
        setupUri,
        bindAddress: config.bindAddress,
        hapPort: config.hapPort,
        updatedAt: new Date().toISOString(),
      },
      0o600,
    );

    const persistRotatedToken = (newToken) => {
      if (!newToken || typeof newToken !== 'string') {
        return;
      }
      const trimmed = newToken.trim();
      if (!trimmed) return;
      activeToken = trimmed;
      if (trimmed === persistedToken && pendingTokenToPersist === null) {
        return;
      }
      pendingTokenToPersist = trimmed;
      try {
        updateRefreshToken(stateDir, trimmed);
        persistedToken = trimmed;
        pendingTokenToPersist = null;
      } catch (err) {
        if (doorbell) {
          doorbell.stats.lastError = `Failed to persist rotated Ring token: ${err.message}`;
        }
        writeStatus();
      }
    };

    const connectRingIfConfigured = async () => {
      if (stopping || connectingRing) return;
      if (pendingTokenToPersist) {
        persistRotatedToken(pendingTokenToPersist);
      }
      let latestConfig;
      try {
        latestConfig = loadOrCreateConfig(stateDir).config;
      } catch {
        return;
      }
      const token = pendingTokenToPersist || latestConfig.ringRefreshToken;
      if (!token || (token === activeToken && ringState === 'connected')) {
        return;
      }

      connectingRing = true;
      ringState = 'connecting';
      writeStatus();

      let candidateApi = null;
      try {
        if (ringApi) {
          try {
            ringApi.disconnect();
          } catch {}
          ringApi = null;
        }

        let pushRegPromise = null;
        candidateApi = new RingApi({
          refreshToken: token,
          cameraStatusPollingSeconds: 20,
          avoidSnapshotBatteryDrain: true,
          controlCenterDisplayName: process.env.RING_CONTROL_CENTER_NAME || 'ring-hksv-local-bridge',
          ffmpegPath: latestConfig.ffmpegPath,
        });
        ringApi = candidateApi;

        if (typeof candidateApi.registerPushReceiver === 'function') {
          const origRegister = candidateApi.registerPushReceiver.bind(candidateApi);
          candidateApi.registerPushReceiver = (...rpArgs) => {
            pushRegPromise = Promise.resolve(origRegister(...rpArgs)).catch(() => {});
            return pushRegPromise;
          };
        }

        activeToken = token;
        candidateApi.onRefreshTokenUpdated.subscribe(({ newRefreshToken }) => {
          persistRotatedToken(newRefreshToken);
        });

        const cameras = await candidateApi.getCameras();
        if (stopping || ringApi !== candidateApi) {
          try {
            candidateApi.disconnect();
          } catch {}
          if (ringApi === candidateApi) ringApi = null;
          return;
        }

        const chosen = selectRingCamera(cameras, latestConfig);
        if (!chosen) {
          try {
            candidateApi.disconnect();
          } catch {}
          if (ringApi === candidateApi) ringApi = null;
          ringState = 'no_ring_cameras_found';
          activeToken = null;
          writeStatus();
          return;
        }

        if (pushRegPromise) {
          let pushTimer = null;
          await Promise.race([
            pushRegPromise,
            new Promise((r) => {
              pushTimer = setTimeout(r, 4000);
              pushTimer.unref?.();
            }),
          ]).finally(() => {
            if (pushTimer) clearTimeout(pushTimer);
          });
        }

        if (stopping || ringApi !== candidateApi) {
          try {
            candidateApi.disconnect();
          } catch {}
          if (ringApi === candidateApi) ringApi = null;
          return;
        }

        ringCameraName = chosen.name;
        ringCameraId = chosen.id;
        ringState = 'connected';

        const ringSource = new RingCameraMediaSource(chosen, {
          ffmpegPath: latestConfig.ffmpegPath,
          onEvent: (ev) => {
            doorbell.handleRingEvent(ev);
          },
        });
        doorbell.setMediaSource(ringSource);
        await chosen.subscribeToDingEvents().catch(() => {});
        await chosen.subscribeToMotionEvents().catch(() => {});

        if (stopping || ringApi !== candidateApi) {
          try {
            ringSource.destroy();
          } catch {}
          try {
            candidateApi.disconnect();
          } catch {}
          if (ringApi === candidateApi) ringApi = null;
          return;
        }

        if (candidateApi.restClient?.refreshToken) {
          persistRotatedToken(candidateApi.restClient.refreshToken);
        }
        writeStatus();
      } catch (err) {
        if (candidateApi) {
          try {
            candidateApi.disconnect();
          } catch {}
          if (ringApi === candidateApi) {
            ringApi = null;
          }
        }
        ringState = `auth_or_connect_error: ${err.message}`;
        activeToken = null;
        writeStatus();
      } finally {
        connectingRing = false;
      }
    };

    await connectRingIfConfigured();
    writeStatus();

    const configPollTimer = setInterval(() => {
      if (pendingTokenToPersist) {
        persistRotatedToken(pendingTokenToPersist);
      }
      if (ringState !== 'connected') {
        connectRingIfConfigured().catch(() => {});
      }
      try {
        pruneClips(paths.clipsDir, {
          maxAgeMs: config.clipRetentionDays * 86400_000,
          maxTotalBytes: config.maxClipsTotalBytes,
          activePartPaths: doorbell.getActivePartPaths(),
        });
      } catch {}
    }, 5000);

    const onSigUsr1 = () => {
      doorbell.handleRingEvent({ type: 'ding', at: Date.now() });
    };
    const onSigUsr2 = () => {
      doorbell.handleRingEvent({ type: 'motion', motion: true, at: Date.now() });
    };
    process.on('SIGUSR1', onSigUsr1);
    process.on('SIGUSR2', onSigUsr2);

    const stop = async () => {
      if (stopping) return;
      stopping = true;
      clearInterval(configPollTimer);
      process.removeListener('SIGUSR1', onSigUsr1);
      process.removeListener('SIGUSR2', onSigUsr2);
      if (pendingTokenToPersist) {
        persistRotatedToken(pendingTokenToPersist);
      }
      try {
        await doorbell.accessory.unpublish();
      } catch {}
      try {
        doorbell.destroy();
      } catch {}
      try {
        ringApi?.disconnect();
        ringApi = null;
      } catch {}
      stateLock.release();
    };

    return {
      config,
      paths,
      doorbell,
      setupUri,
      stop,
    };
  } catch (err) {
    try {
      doorbell?.destroy();
    } catch {}
    stateLock.release();
    throw err;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const checkOnly = args.includes('--check');
  const stateDirIdx = args.indexOf('--state-dir');
  const stateDir = stateDirIdx !== -1 && args[stateDirIdx + 1] ? args[stateDirIdx + 1] : defaultStateDir();

  startService({ stateDir, checkOnly })
    .then((res) => {
      if (checkOnly) {
        console.log(JSON.stringify(res, null, 2));
        process.exit(0);
      }
      const shutdown = async () => {
        await res.stop();
        process.exit(0);
      };
      process.once('SIGINT', shutdown);
      process.once('SIGTERM', shutdown);
    })
    .catch((err) => {
      console.error(JSON.stringify({ event: 'startup_failed', error: err.message }));
      process.exit(1);
    });
}
