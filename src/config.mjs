import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const FORBIDDEN_PINCODES = new Set([
  '000-00-000',
  '111-11-111',
  '222-22-222',
  '333-33-333',
  '444-44-444',
  '555-55-555',
  '666-66-666',
  '777-77-777',
  '888-88-888',
  '999-99-999',
  '123-45-678',
  '876-54-321',
]);

export function defaultStateDir() {
  if (process.env.RING_HKSV_STATE_DIR) {
    return path.resolve(process.env.RING_HKSV_STATE_DIR);
  }
  return path.join(os.homedir(), '.local', 'share', 'ring-hksv');
}

export function detectDefaultLanAddress() {
  if (process.env.RING_HKSV_BIND_ADDRESS && process.env.RING_HKSV_BIND_ADDRESS.trim()) {
    return process.env.RING_HKSV_BIND_ADDRESS.trim();
  }
  const nets = os.networkInterfaces();
  for (const entries of Object.values(nets)) {
    for (const entry of entries || []) {
      if (entry && entry.family === 'IPv4' && !entry.internal && entry.address) {
        return entry.address;
      }
    }
  }
  return '0.0.0.0';
}

export function detectDefaultFfmpegPath() {
  if (process.env.FFMPEG_PATH && process.env.FFMPEG_PATH.trim()) {
    return process.env.FFMPEG_PATH.trim();
  }
  for (const candidate of ['/usr/bin/ffmpeg', '/usr/local/bin/ffmpeg', '/opt/homebrew/bin/ffmpeg']) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return '/usr/bin/ffmpeg';
}

export function resolvePaths(stateDir = defaultStateDir()) {
  const root = path.resolve(stateDir);
  return {
    root,
    lockFile: path.join(root, 'service.lock'),
    configFile: path.join(root, 'config.json'),
    statusFile: path.join(root, 'status.json'),
    pairingFile: path.join(root, 'pairing.json'),
    hapPersistDir: path.join(root, 'hap-persist'),
    clipsDir: path.join(root, 'clips'),
  };
}

export function acquireStateDirLock(stateDir = defaultStateDir()) {
  const paths = resolvePaths(stateDir);
  ensureSecureDir(paths.root, 0o700);
  const lockPath = paths.lockFile;

  const tryCreate = () => {
    const fd = fs.openSync(
      lockPath,
      fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW || 0),
      0o600,
    );
    try {
      const payload = JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }) + '\n';
      fs.writeFileSync(fd, payload, 'utf8');
      fs.fchmodSync(fd, 0o600);
      fs.fsyncSync(fd);
      return fd;
    } catch (err) {
      try {
        fs.closeSync(fd);
      } catch {}
      try {
        fs.unlinkSync(lockPath);
      } catch {}
      throw err;
    }
  };

  let fd;
  try {
    fd = tryCreate();
  } catch (err) {
    if (err?.code !== 'EEXIST') {
      throw err;
    }
    let existingPid = null;
    try {
      const raw = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
      if (Number.isInteger(raw?.pid) && raw.pid > 0) {
        existingPid = raw.pid;
      }
    } catch {}
    if (existingPid !== null) {
      let alive = false;
      try {
        process.kill(existingPid, 0);
        alive = true;
      } catch (killErr) {
        alive = killErr?.code === 'EPERM';
      }
      if (alive) {
        throw new Error(`State directory ${paths.root} is already locked by running process PID ${existingPid}`);
      }
    }
    try {
      fs.unlinkSync(lockPath);
    } catch {}
    fd = tryCreate();
  }

  let released = false;
  return {
    lockPath,
    release() {
      if (released) return;
      released = true;
      try {
        fs.closeSync(fd);
      } catch {}
      try {
        fs.unlinkSync(lockPath);
      } catch {}
    },
  };
}

export function ensureSecureDir(dirPath, mode = 0o700) {
  fs.mkdirSync(dirPath, { recursive: true, mode });
  const stat = fs.lstatSync(dirPath);
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    throw new Error(`Refusing insecure directory path: ${dirPath}`);
  }
  fs.chmodSync(dirPath, mode);
}

export function fsyncDir(dirPath) {
  const dirFd = fs.openSync(dirPath, 'r');
  try {
    fs.fsyncSync(dirFd);
  } finally {
    fs.closeSync(dirFd);
  }
}

export function writeJsonAtomic(filePath, data, mode = 0o600) {
  const dir = path.dirname(filePath);
  ensureSecureDir(dir, 0o700);
  if (fs.existsSync(filePath)) {
    const existing = fs.lstatSync(filePath);
    if (existing.isSymbolicLink()) {
      throw new Error(`Refusing to overwrite symlink: ${filePath}`);
    }
  }
  const tmpName = `.${path.basename(filePath)}.${crypto.randomBytes(6).toString('hex')}.tmp`;
  const tmpPath = path.join(dir, tmpName);
  const fd = fs.openSync(tmpPath, 'wx', mode);
  try {
    const payload = JSON.stringify(data, null, 2) + '\n';
    fs.writeFileSync(fd, payload, 'utf8');
    fs.fchmodSync(fd, mode);
    fs.fsyncSync(fd);
  } catch (err) {
    try {
      fs.closeSync(fd);
    } catch {}
    try {
      fs.unlinkSync(tmpPath);
    } catch {}
    throw err;
  }
  fs.closeSync(fd);
  try {
    fs.renameSync(tmpPath, filePath);
    fsyncDir(dir);
  } catch (err) {
    try {
      fs.unlinkSync(tmpPath);
    } catch {}
    throw err;
  }
}

export function generateMacAddress() {
  const bytes = crypto.randomBytes(6);
  // Locally administered, unicast MAC address
  bytes[0] = (bytes[0] & 0xfe) | 0x02;
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0').toUpperCase()).join(':');
}

export function generatePincode() {
  for (let i = 0; i < 64; i++) {
    const n = crypto.randomInt(0, 100_000_000);
    const digits = n.toString().padStart(8, '0');
    const pin = `${digits.slice(0, 3)}-${digits.slice(3, 5)}-${digits.slice(5, 8)}`;
    if (!FORBIDDEN_PINCODES.has(pin)) {
      return pin;
    }
  }
  throw new Error('Failed to generate valid HomeKit pincode');
}

export function generateSetupId() {
  const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  let out = '';
  for (let i = 0; i < 4; i++) {
    out += alphabet[crypto.randomInt(0, alphabet.length)];
  }
  return out;
}

export function validateConfig(raw) {
  if (!raw || typeof raw !== 'object') {
    throw new Error('Configuration must be a JSON object');
  }
  const accessoryName = typeof raw.accessoryName === 'string' && raw.accessoryName.trim()
    ? raw.accessoryName.trim()
    : 'Ring Doorbell';
  const username = typeof raw.username === 'string' && /^([0-9A-F]{2}:){5}[0-9A-F]{2}$/i.test(raw.username)
    ? raw.username.toUpperCase()
    : null;
  if (!username) {
    throw new Error('Invalid or missing HomeKit username (MAC format required)');
  }
  const pincode = typeof raw.pincode === 'string' && /^\d{3}-\d{2}-\d{3}$/.test(raw.pincode)
    ? raw.pincode
    : null;
  if (!pincode || FORBIDDEN_PINCODES.has(pincode)) {
    throw new Error('Invalid or forbidden HomeKit pincode');
  }
  const setupId = typeof raw.setupId === 'string' && /^[0-9A-Z]{4}$/.test(raw.setupId)
    ? raw.setupId
    : null;
  if (!setupId) {
    throw new Error('Invalid or missing HomeKit setupId');
  }
  const bindAddress = typeof raw.bindAddress === 'string' && raw.bindAddress.trim()
    ? raw.bindAddress.trim()
    : detectDefaultLanAddress();
  const hapPort = Number.isInteger(raw.hapPort) && raw.hapPort >= 1024 && raw.hapPort <= 65535
    ? raw.hapPort
    : 21065;
  if (hapPort === 21064) {
    throw new Error('hapPort 21064 is reserved to avoid colliding with default HomeKit bridges');
  }
  const eventDurationSeconds = Number.isInteger(raw.eventDurationSeconds) && raw.eventDurationSeconds >= 8 && raw.eventDurationSeconds <= 180
    ? raw.eventDurationSeconds
    : 25;
  const clipRetentionDays = Number.isInteger(raw.clipRetentionDays) && raw.clipRetentionDays >= 1 && raw.clipRetentionDays <= 90
    ? raw.clipRetentionDays
    : 10;
  const maxClipsTotalBytes = Number.isInteger(raw.maxClipsTotalBytes) && raw.maxClipsTotalBytes >= 64 * 1024 * 1024
    ? raw.maxClipsTotalBytes
    : 5 * 1024 * 1024 * 1024;
  const ffmpegPath = typeof raw.ffmpegPath === 'string' && raw.ffmpegPath.trim()
    ? raw.ffmpegPath.trim()
    : detectDefaultFfmpegPath();
  const ringRefreshToken = typeof raw.ringRefreshToken === 'string' && raw.ringRefreshToken.trim()
    ? raw.ringRefreshToken.trim()
    : null;
  const targetCameraName = typeof raw.targetCameraName === 'string' && raw.targetCameraName.trim()
    ? raw.targetCameraName.trim()
    : null;
  const targetCameraId = Number.isInteger(raw.targetCameraId) ? raw.targetCameraId : null;
  const localClipDumpEnabled = Boolean(raw.localClipDumpEnabled);
  const alwaysOnPrebuffer = raw.alwaysOnPrebuffer !== false;

  return {
    accessoryName,
    username,
    pincode,
    setupId,
    bindAddress,
    hapPort,
    eventDurationSeconds,
    clipRetentionDays,
    maxClipsTotalBytes,
    ffmpegPath,
    ringRefreshToken,
    targetCameraName,
    targetCameraId,
    localClipDumpEnabled,
    alwaysOnPrebuffer,
  };
}

export function loadOrCreateConfig(stateDir = defaultStateDir(), overrides = {}) {
  const paths = resolvePaths(stateDir);
  ensureSecureDir(paths.root, 0o700);
  ensureSecureDir(paths.hapPersistDir, 0o700);
  ensureSecureDir(paths.clipsDir, 0o700);

  let existing = {};
  if (fs.existsSync(paths.configFile)) {
    const stat = fs.lstatSync(paths.configFile);
    if (stat.isSymbolicLink()) {
      throw new Error(`Refusing symlinked config file: ${paths.configFile}`);
    }
    existing = JSON.parse(fs.readFileSync(paths.configFile, 'utf8'));
  }

  const merged = {
    accessoryName: 'Ring Doorbell',
    username: existing.username || generateMacAddress(),
    pincode: existing.pincode || generatePincode(),
    setupId: existing.setupId || generateSetupId(),
    bindAddress: detectDefaultLanAddress(),
    hapPort: 21065,
    eventDurationSeconds: 25,
    clipRetentionDays: 10,
    maxClipsTotalBytes: 5 * 1024 * 1024 * 1024,
    ffmpegPath: detectDefaultFfmpegPath(),
    ringRefreshToken: null,
    targetCameraName: null,
    targetCameraId: null,
    localClipDumpEnabled: false,
    alwaysOnPrebuffer: true,
    ...existing,
    ...overrides,
  };

  const validated = validateConfig(merged);
  writeJsonAtomic(paths.configFile, validated, 0o600);
  return { config: validated, paths };
}

export function updateRefreshToken(stateDir, newRefreshToken) {
  if (typeof newRefreshToken !== 'string' || !newRefreshToken.trim()) {
    throw new Error('Refusing to persist empty Ring refresh token');
  }
  return loadOrCreateConfig(stateDir, { ringRefreshToken: newRefreshToken.trim() });
}

export function pruneClips(
  clipsDir,
  {
    maxAgeMs = 10 * 86400_000,
    maxTotalBytes = 5 * 1024 * 1024 * 1024,
    stalePartAgeMs = 300_000,
    removeAllParts = false,
    activePartPaths = new Set(),
    now = Date.now(),
  } = {},
) {
  if (!fs.existsSync(clipsDir)) {
    return { removedCount: 0, remainingBytes: 0 };
  }
  const protectedPaths = new Set(
    Array.from(activePartPaths || [], (p) => path.resolve(String(p))),
  );
  const evictable = [];
  let activeBytes = 0;
  let removedCount = 0;

  for (const name of fs.readdirSync(clipsDir)) {
    const isMp4 = name.endsWith('.mp4');
    const isPart = name.endsWith('.part');
    if (!isMp4 && !isPart) continue;
    const fullPath = path.resolve(clipsDir, name);
    try {
      const stat = fs.lstatSync(fullPath);
      if (stat.isSymbolicLink() || !stat.isFile()) continue;
      if (protectedPaths.has(fullPath)) {
        activeBytes += stat.size;
        continue;
      }
      if (isPart && (removeAllParts || now - stat.mtimeMs > stalePartAgeMs)) {
        fs.unlinkSync(fullPath);
        removedCount++;
        continue;
      }
      evictable.push({ fullPath, mtimeMs: stat.mtimeMs, size: stat.size, isPart });
    } catch {}
  }

  evictable.sort((a, b) => a.mtimeMs - b.mtimeMs);
  const kept = [];

  for (const entry of evictable) {
    if (now - entry.mtimeMs > maxAgeMs) {
      try {
        fs.unlinkSync(entry.fullPath);
        removedCount++;
      } catch {}
    } else {
      kept.push(entry);
    }
  }

  let totalBytes = activeBytes + kept.reduce((acc, item) => acc + item.size, 0);
  while (totalBytes > maxTotalBytes && kept.length > 0) {
    const oldest = kept.shift();
    try {
      fs.unlinkSync(oldest.fullPath);
      totalBytes -= oldest.size;
      removedCount++;
    } catch {}
  }

  return { removedCount, remainingBytes: totalBytes };
}
