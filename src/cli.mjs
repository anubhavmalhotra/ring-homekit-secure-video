#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import qrcode from 'qrcode-terminal';
import { RingApi } from 'ring-client-api';
import { RingRestClient } from 'ring-client-api/rest-client';
import {
  defaultStateDir,
  loadOrCreateConfig,
  resolvePaths,
  updateRefreshToken,
} from './config.mjs';
import { withTimeoutAndAbort } from './media-pipeline.mjs';
import { selectRingCamera } from './service.mjs';

export function parseCliArgs(args = []) {
  let stateDir = defaultStateDir();
  const positionals = [];
  const flags = new Set();

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--state-dir') {
      if (i + 1 >= args.length || args[i + 1].startsWith('-')) {
        throw new Error('Missing value for --state-dir');
      }
      stateDir = path.resolve(args[++i]);
    } else if (arg.startsWith('--state-dir=')) {
      const val = arg.slice('--state-dir='.length).trim();
      if (!val) {
        throw new Error('Missing value for --state-dir');
      }
      stateDir = path.resolve(val);
    } else if (arg.startsWith('-')) {
      flags.add(arg);
    } else {
      positionals.push(arg);
    }
  }

  return {
    stateDir,
    command: positionals[0] || 'status',
    subArg: positionals[1] || null,
    flags,
  };
}

function promptLine(question, { hidden = false } = {}) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: true,
    });
    if (hidden) {
      const origWrite = rl._writeToOutput;
      rl._writeToOutput = function (str) {
        if (str.includes('\n') || str.includes('\r')) {
          origWrite.call(rl, str);
        }
      };
      process.stdout.write(question);
    }
    rl.question(hidden ? '' : question, (answer) => {
      rl.close();
      if (hidden) {
        process.stdout.write('\n');
      }
      resolve(answer.trim());
    });
  });
}

async function readStdinToken() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString('utf8').trim();
}

export function hasUsablePushCredentials(rawRefreshToken, restClient = null) {
  if (typeof rawRefreshToken !== 'string' || !rawRefreshToken.trim()) {
    return false;
  }
  try {
    const decoded = Buffer.from(rawRefreshToken.trim(), 'base64').toString('utf8');
    const parsed = JSON.parse(decoded);
    if (!parsed || typeof parsed !== 'object') {
      return false;
    }
    if (typeof parsed.rt !== 'string' || !parsed.rt.trim()) {
      return false;
    }
    const pnc = parsed.pnc;
    if (!pnc || typeof pnc !== 'object') {
      return false;
    }
    const fcmToken = pnc.fcm?.token;
    if (typeof fcmToken !== 'string' || !fcmToken.trim()) {
      return false;
    }
    if (!pnc.fcm?.installation || typeof pnc.fcm.installation !== 'object') {
      return false;
    }
    if (!pnc.gcm || typeof pnc.gcm !== 'object') {
      return false;
    }
    if (!pnc.keys || typeof pnc.keys !== 'object') {
      return false;
    }
    if (!pnc.config || typeof pnc.config !== 'object') {
      return false;
    }
    const liveToken = restClient?._internalOnly_pushNotificationCredentials?.fcm?.token;
    if (typeof liveToken === 'string' && liveToken.trim() && liveToken !== fcmToken) {
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

export async function authenticateAndPersistRing({
  stateDir,
  initialRefreshToken,
  apiFactory = (opts) => new RingApi(opts),
  pushTimeoutMs = 15000,
}) {
  let latestRefreshToken = initialRefreshToken;
  let lastPersistError = null;
  const api = apiFactory({
    refreshToken: initialRefreshToken,
    controlCenterDisplayName: process.env.RING_CONTROL_CENTER_NAME || 'ring-hksv-local-bridge',
  });

  let pushRegPromise = null;
  const devicePatchPromises = [];
  let pushTokenUpdated = hasUsablePushCredentials(initialRefreshToken, api.restClient);
  let resolvePushTokenWait = null;
  const pushTokenWaitPromise = new Promise((resolve) => {
    resolvePushTokenWait = resolve;
  });

  if (typeof api.registerPushReceiver === 'function') {
    const origRegisterPush = api.registerPushReceiver.bind(api);
    api.registerPushReceiver = (...rpArgs) => {
      pushRegPromise = Promise.resolve().then(() => origRegisterPush(...rpArgs));
      return pushRegPromise;
    };
  }

  if (api.restClient && typeof api.restClient.request === 'function') {
    const origRequest = api.restClient.request.bind(api.restClient);
    api.restClient.request = (...reqArgs) => {
      const reqOpts = reqArgs[0];
      const p = Promise.resolve().then(() => origRequest(...reqArgs));
      if (
        reqOpts &&
        typeof reqOpts === 'object' &&
        reqOpts.method === 'PATCH' &&
        (String(reqOpts.url || '').endsWith('/device') || reqOpts.json?.device?.push_notification_token)
      ) {
        devicePatchPromises.push(p);
      }
      return p;
    };
  }

  const sub = api.onRefreshTokenUpdated?.subscribe?.(({ newRefreshToken }) => {
    if (newRefreshToken && newRefreshToken.trim()) {
      latestRefreshToken = newRefreshToken.trim();
      if (hasUsablePushCredentials(latestRefreshToken, api.restClient)) {
        pushTokenUpdated = true;
        resolvePushTokenWait?.(latestRefreshToken);
        try {
          updateRefreshToken(stateDir, latestRefreshToken);
          lastPersistError = null;
        } catch (err) {
          lastPersistError = err;
        }
      }
    }
  });

  try {
    const deadline = Date.now() + pushTimeoutMs;
    const cameras = await api.getCameras();
    if (!Array.isArray(cameras) || cameras.length === 0) {
      throw new Error('Authenticated to Ring, but no cameras/doorbells were found on this account');
    }
    if (pushRegPromise) {
      await withTimeoutAndAbort(pushRegPromise, {
        timeoutMs: Math.max(1, deadline - Date.now()),
        label: 'Ring push receiver credential registration',
      });
    }
    const candidateAfterReg = api.restClient?.refreshToken || latestRefreshToken || initialRefreshToken;
    if (
      !hasUsablePushCredentials(candidateAfterReg, api.restClient) &&
      !pushTokenUpdated &&
      api.onRefreshTokenUpdated
    ) {
      await withTimeoutAndAbort(pushTokenWaitPromise, {
        timeoutMs: Math.max(1, Math.min(1000, deadline - Date.now())),
        label: 'Ring push receiver refresh token update',
      }).catch(() => {});
    }
    if (devicePatchPromises.length > 0) {
      await withTimeoutAndAbort(Promise.all(devicePatchPromises), {
        timeoutMs: Math.max(1, deadline - Date.now()),
        label: 'Ring push notification token device PATCH',
      });
    }

    const { config } = loadOrCreateConfig(stateDir);
    const chosen = selectRingCamera(cameras, config);

    await chosen.subscribeToDingEvents?.().catch(() => {});
    await chosen.subscribeToMotionEvents?.().catch(() => {});

    const tokenToPersist = api.restClient?.refreshToken || latestRefreshToken || initialRefreshToken;
    if (!hasUsablePushCredentials(tokenToPersist, api.restClient)) {
      throw new Error(
        'Ring authentication succeeded for REST API, but usable push receiver credentials (pnc.fcm/gcm/keys/config) were not registered in the refresh token',
      );
    }

    const { config: savedConfig } = loadOrCreateConfig(stateDir, {
      ringRefreshToken: tokenToPersist,
      targetCameraId: chosen.id,
      targetCameraName: chosen.name,
    });
    if (lastPersistError && savedConfig.ringRefreshToken !== tokenToPersist) {
      throw lastPersistError;
    }
    if (
      savedConfig.ringRefreshToken !== tokenToPersist ||
      !hasUsablePushCredentials(savedConfig.ringRefreshToken, api.restClient)
    ) {
      throw new Error('Failed to verify persisted Ring refresh token and push credentials on disk');
    }

    return {
      authenticated: true,
      cameraSelected: {
        id: chosen.id,
        name: chosen.name,
        model: chosen.model,
        isDoorbot: chosen.isDoorbot,
        batteryLevel: chosen.batteryLevel,
      },
      totalCamerasFound: cameras.length,
      tokenPersistedSecurely: true,
      pushCredentialsPersisted: true,
    };
  } finally {
    try {
      sub?.unsubscribe?.();
    } catch {}
    try {
      api.disconnect?.();
    } catch {}
  }
}

async function runAuth(stateDir, flags) {
  let refreshToken = null;

  if (flags.has('--refresh-token-stdin')) {
    refreshToken = await readStdinToken();
    if (!refreshToken) {
      throw new Error('No refresh token received on stdin');
    }
  } else {
    console.log('=== Ring Account Authentication (saved locally with 0600 permissions) ===');
    const email = await promptLine('Ring Email: ');
    const password = await promptLine('Ring Password (hidden): ', { hidden: true });
    if (!email || !password) {
      throw new Error('Email and password are required');
    }
    const restClient = new RingRestClient({
      email,
      password,
      controlCenterDisplayName: process.env.RING_CONTROL_CENTER_NAME || 'ring-hksv-local-bridge',
    });

    let authResp;
    try {
      authResp = await restClient.getCurrentAuth();
    } catch (err) {
      if (restClient.promptFor2fa) {
        console.log(restClient.promptFor2fa);
        const code = await promptLine('2FA Verification Code: ');
        authResp = await restClient.getAuth(code);
      } else {
        throw err;
      }
    }
    refreshToken = authResp.refresh_token;
  }

  const summary = await authenticateAndPersistRing({
    stateDir,
    initialRefreshToken: refreshToken,
  });
  console.log(JSON.stringify(summary, null, 2));
}

export function runPairInfo(stateDir) {
  const paths = resolvePaths(stateDir);
  if (!fs.existsSync(paths.pairingFile)) {
    throw new Error(`Pairing file not found at ${paths.pairingFile}. Start the service first.`);
  }
  const pairing = JSON.parse(fs.readFileSync(paths.pairingFile, 'utf8'));
  console.log('=== Apple Home / HomeKit Secure Video Pairing ===');
  console.log(`Accessory Name : ${pairing.accessoryName}`);
  console.log(`Bind Address   : ${pairing.bindAddress}:${pairing.hapPort}`);
  console.log(`Setup Code     : ${pairing.pincode}`);
  console.log(`Setup URI      : ${pairing.setupUri}\n`);
  qrcode.generate(pairing.setupUri, { small: true });
  console.log('\nSteps in Apple Home:');
  console.log('1. Open Apple Home -> tap "+" -> Add Accessory -> scan QR code above (or More Options -> select Ring Doorbell and enter the 8-digit Setup Code).');
  console.log('2. In the Doorbell settings inside Apple Home -> Recording Options -> set "When Home" and "When Away" to "Stream & Allow Recording".');
  console.log('3. Your HomePod / Apple TV Home Hub will now analyze and record motion/doorbell clips to iCloud+.');
}

export function runStatus(stateDir) {
  const paths = resolvePaths(stateDir);
  const status = fs.existsSync(paths.statusFile)
    ? JSON.parse(fs.readFileSync(paths.statusFile, 'utf8'))
    : { status: 'not_started' };

  const clips = [];
  if (fs.existsSync(paths.clipsDir)) {
    for (const name of fs.readdirSync(paths.clipsDir).sort().slice(-10)) {
      if (!name.endsWith('.mp4')) continue;
      const fullPath = path.join(paths.clipsDir, name);
      try {
        const st = fs.lstatSync(fullPath);
        if (st.isFile()) {
          clips.push({ name, bytes: st.size, mtime: st.mtime.toISOString() });
        }
      } catch {}
    }
  }

  console.log(JSON.stringify({ service: status, recentLocalClips: clips }, null, 2));
}

export function runSimulateEvent(stateDir, kind = 'ding') {
  const paths = resolvePaths(stateDir);
  if (!fs.existsSync(paths.statusFile)) {
    throw new Error('Service status file not found; start the service first');
  }
  const status = JSON.parse(fs.readFileSync(paths.statusFile, 'utf8'));
  const pid = Number(status.pid);
  if (!Number.isInteger(pid) || pid <= 1) {
    throw new Error('Invalid PID in status.json');
  }
  const sig = kind === 'motion' ? 'SIGUSR2' : 'SIGUSR1';
  process.kill(pid, sig);
  console.log(JSON.stringify({ triggered: kind, signal: sig, pid }, null, 2));
}

export async function main(argv = process.argv.slice(2)) {
  const { stateDir, command, subArg, flags } = parseCliArgs(argv);

  if (command === 'auth') {
    await runAuth(stateDir, flags);
    return;
  }
  if (command === 'pair-info') {
    runPairInfo(stateDir);
    return;
  }
  if (command === 'status') {
    runStatus(stateDir);
    return;
  }
  if (command === 'simulate-event') {
    runSimulateEvent(stateDir, subArg || 'ding');
    return;
  }
  throw new Error(`Unknown command: ${command}. Expected: auth | pair-info | status | simulate-event`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  });
}
