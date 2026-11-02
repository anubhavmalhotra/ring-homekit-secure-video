# ring-hksv-bridge

Self-hosted **Ring Video Doorbell → Apple HomeKit Secure Video (`HKSV` / iCloud+) + Local Clip Archive** bridge.

Replaces a paid Ring Protect cloud recording subscription by bridging Ring doorbell/camera push events and live WebRTC/RTP media directly to:
1. **Apple HomeKit Secure Video (`HKSV` / iCloud+)** — Your Apple Home Hub (HomePod / Apple TV) receives fragmented MP4 (`fMP4`) streams over HomeKit DataStream (`HDS`), performs local on-device AI detection (people, faces, animals, vehicles, packages), and uploads end-to-end encrypted clips to iCloud+ without counting against your iCloud storage quota.
2. **Local MP4 Clip Archive** — Simultaneously saves timestamped, crash-safe `.mp4` clips (`H.264` + `AAC-LC`) to your local disk with automatic age and storage-cap pruning.
3. **Apple Home Live View & Two-Way Audio** — Full RFC 3711 SRTP/SRTCP (`AES_CM_128_HMAC_SHA1_80`) Live View streaming, Opus `48 kHz` → `16/24 kHz` RTP/RTCP timestamp scaling, seamless `RECONFIGURE` across encoder changes, and authenticated two-way talkback.

---

## Features

- **Zero Ring Subscription Required:** Uses real-time Ring push notifications (`FCM`/`GCM` via `ring-client-api`) + live WebRTC sessions when motion or a doorbell press occurs.
- **HomeKit Secure Video (`CameraRecordingDelegate`):**
  - Compliant `ftyp+moov` initialization segment and keyframe-aligned 2-second `moof+mdat` fragments (`Fmp4StreamSegmenter`).
  - Supports both **Event-Driven Mode** (`alwaysOnPrebuffer: false`, recommended for battery-powered doorbells so they stay in low-power sleep until motion/ding) and **Always-On 4-Second Prebuffer Mode** (`alwaysOnPrebuffer: true`, for hardwired cameras).
  - Enforces HomeKit privacy (`HomeKitCameraActive`) and audio mute (`RecordingAudioActive`) toggles immediately, including mid-stream.
- **Rootless & Hardened:**
  - Runs as an unprivileged user process (`systemd --user` on Linux or standalone Node.js `>= 20` on macOS/Linux).
  - Enforces `0700` state directories and `0600` atomic JSON/clip writes with parent-directory `fsync`.
  - Loopback-isolated FFmpeg SDP sockets (`-localaddr 127.0.0.1`) and RFC 3711 SRTP/SRTCP replay protection before any talkback speaker activation.

---

## Prerequisites

- **Node.js `>= 20`** (the included `./install.sh` automatically installs official Node.js `v22.14.0` in `~/.local/` if Node `>= 20` is not already installed)
- **FFmpeg** (`ffmpeg` and `ffprobe` on `PATH`, e.g. `sudo apt install ffmpeg` or `brew install ffmpeg`)
- **Apple Home Hub** (HomePod, HomePod mini, or Apple TV) on the same LAN and an **iCloud+** plan for HomeKit Secure Video recording

---

## Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/anubhavmalhotra/ring-hksv-bridge.git
cd ring-hksv-bridge
chmod +x install.sh
./install.sh
```

On Linux with `systemd`, `./install.sh` installs dependencies, runs the unit + real-FFmpeg E2E test suite, installs and starts the rootless `~/.config/systemd/user/ring-hksv.service` unit, and prints your Apple Home QR code and 8-digit Setup Code.

*(Tip: To keep `systemd --user` services running after logout/reboot on Linux, ensure lingering is enabled once via `loginctl enable-linger $USER`.)*

### 2. Authenticate Your Ring Account (One-Time 2FA)

```bash
npm run auth
```

Enter your Ring email, password, and 2FA verification code (or pass an existing refresh token via `RING_REFRESH_TOKEN` or `--stdin`). The CLI verifies that `FCM`/`GCM` push receiver credentials are registered and durably persisted (`0600`) in `~/.local/share/ring-hksv/config.json`.

If running under `systemd --user`, restart the service after authenticating:

```bash
systemctl --user restart ring-hksv.service
```

### 3. Pair with Apple Home

```bash
npm run pair-info
```

1. Open the **Apple Home** app on iPhone/iPad → tap **+** → **Add Accessory**.
2. Scan the QR code printed by `npm run pair-info` (or tap **More Options…**, select **Ring Doorbell**, and enter the 8-digit Setup Code).
3. In Apple Home → **Ring Doorbell** settings → **Recording Options**, set **When Home** and **When Away** to **Stream & Allow Recording**.

---

## Configuration

Configuration is stored in `~/.local/share/ring-hksv/config.json` (override the directory with `--state-dir /path` or `RING_HKSV_STATE_DIR=/path`):

| Field | Default | Description |
|---|---|---|
| `accessoryName` | `"Ring Doorbell"` | Accessory name shown in Apple Home |
| `bindAddress` | Auto-detected LAN IPv4 | Local interface IPv4 address for HomeKit mDNS & HAP server |
| `hapPort` | `21065` | TCP port for the HomeKit accessory server |
| `alwaysOnPrebuffer` | `true` | Maintain a continuous 4s prebuffer when HKSV is active (`set to false` for battery-powered doorbells) |
| `localClipDumpEnabled` | `true` | Save every motion/doorbell clip to `<stateDir>/clips/*.mp4` |
| `eventDurationSeconds` | `25` | Hold duration (seconds) after motion/ding trigger |
| `clipRetentionDays` | `10` | Maximum age (days) for local `.mp4` clips |
| `maxClipsTotalBytes` | `5368709120` (5 GiB) | Maximum total disk usage for local `.mp4` clips |
| `ffmpegPath` | Auto-detected | Path to `ffmpeg` binary |
| `targetCameraId` | `null` (auto-select) | Optional specific Ring camera/doorbell device ID |
| `targetCameraName` | `null` (auto-select) | Optional specific Ring camera/doorbell name |

---

## CLI Commands

```bash
# Show live daemon status and recent local MP4 clips
npm run status

# Display Apple Home setup code, setup URI, and terminal QR code
npm run pair-info

# Authenticate with Ring (interactive 2FA or token input)
npm run auth

# Trigger a synthetic doorbell/motion event on a running daemon (sends SIGUSR1)
npm run test-event

# Run the unit and real-FFmpeg E2E test suite
npm test
```

---

## License

MIT
