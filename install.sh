#!/usr/bin/env bash
# Generic rootless user-space installer for ring-hksv-bridge (Linux systemd --user).
set -Eeuo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="${RING_HKSV_APP_DIR:-${SCRIPT_DIR}}"
STATE_DIR="${RING_HKSV_STATE_DIR:-${HOME}/.local/share/ring-hksv}"
SERVICE_NAME="${RING_HKSV_SERVICE_NAME:-ring-hksv.service}"
UNIT_DIR="${HOME}/.config/systemd/user"
NODE_VERSION="${RING_HKSV_NODE_VERSION:-v22.14.0}"

command -v ffmpeg >/dev/null 2>&1 || {
  echo "Error: ffmpeg is required on PATH (e.g. sudo apt install ffmpeg)" >&2
  exit 1
}

resolve_node_bin() {
  if command -v node >/dev/null 2>&1; then
    local ver
    ver="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
    if [[ "${ver}" -ge 20 ]]; then
      command -v node
      return 0
    fi
  fi
  return 1
}

NODE_BIN="$(resolve_node_bin || true)"

if [[ -z "${NODE_BIN}" ]]; then
  arch="$(uname -m)"
  case "${arch}" in
    aarch64|arm64) node_arch="linux-arm64" ;;
    x86_64|amd64)  node_arch="linux-x64" ;;
    *)
      echo "Unsupported architecture ${arch} for automatic Node.js install; please install Node.js >= 20 manually." >&2
      exit 1
      ;;
  esac

  NODE_DIST="node-${NODE_VERSION}-${node_arch}"
  NODE_DIR="${HOME}/.local/${NODE_DIST}"
  if [[ ! -x "${NODE_DIR}/bin/node" ]]; then
    echo "Installing official Node.js ${NODE_VERSION} (${node_arch}) into ${NODE_DIR}..."
    mkdir -p "${HOME}/.local"
    tmp_tar="$(mktemp "${HOME}/.local/node.XXXXXX.tar.xz")"
    trap 'rm -f "${tmp_tar}"' EXIT
    curl --fail --silent --show-error --location \
      "https://nodejs.org/dist/${NODE_VERSION}/${NODE_DIST}.tar.xz" \
      --output "${tmp_tar}"
    expected_sha="$(curl --fail --silent --show-error --location "https://nodejs.org/dist/${NODE_VERSION}/SHASUMS256.txt" | awk -v f="${NODE_DIST}.tar.xz" '$2 == f { print $1 }')"
    actual_sha="$(sha256sum "${tmp_tar}" | awk '{ print $1 }')"
    if [[ -z "${expected_sha}" || "${actual_sha}" != "${expected_sha}" ]]; then
      echo "Node.js archive SHA256 mismatch (expected ${expected_sha}, got ${actual_sha})" >&2
      exit 1
    fi
    rm -rf "${NODE_DIR}"
    mkdir -p "${NODE_DIR}"
    tar -xJf "${tmp_tar}" -C "${NODE_DIR}" --strip-components=1
    rm -f "${tmp_tar}"
    trap - EXIT
  fi
  NODE_BIN="${NODE_DIR}/bin/node"
fi

NODE_BIN_DIR="$(dirname "${NODE_BIN}")"
export PATH="${NODE_BIN_DIR}:${PATH}"

cd "${APP_DIR}"
chmod 700 "${APP_DIR}"
mkdir -p "${STATE_DIR}"
chmod 700 "${STATE_DIR}"

echo "Installing npm dependencies..."
npm install --omit=dev --no-audit --no-fund --ignore-scripts

echo "Running unit and E2E tests..."
npm test

echo "Running configuration preflight check..."
"${NODE_BIN}" src/service.mjs --state-dir "${STATE_DIR}" --check >/dev/null

if command -v systemctl >/dev/null 2>&1; then
  mkdir -p "${UNIT_DIR}"
  sed \
    -e "s|__APP_DIR__|${APP_DIR}|g" \
    -e "s|__STATE_DIR__|${STATE_DIR}|g" \
    -e "s|__NODE_BIN__|${NODE_BIN}|g" \
    -e "s|__NODE_BIN_DIR__|${NODE_BIN_DIR}|g" \
    "${APP_DIR}/ring-hksv.service" > "${UNIT_DIR}/${SERVICE_NAME}"
  chmod 600 "${UNIT_DIR}/${SERVICE_NAME}"

  systemctl --user daemon-reload
  systemctl --user enable --now "${SERVICE_NAME}"
  systemctl --user restart "${SERVICE_NAME}"

  HAP_PORT="$("${NODE_BIN}" -e "import fs from 'node:fs'; const c = JSON.parse(fs.readFileSync('${STATE_DIR}/config.json','utf8')); console.log(c.hapPort || 21065);")"
  ready=0
  for _ in {1..30}; do
    if [[ -f "${STATE_DIR}/pairing.json" ]] && ss -tuln | grep -Eq ":${HAP_PORT}\\b"; then
      ready=1
      break
    fi
    sleep 1
  done

  if [[ "${ready}" -ne 1 ]]; then
    echo "${SERVICE_NAME} did not bind port ${HAP_PORT} in time" >&2
    systemctl --user status "${SERVICE_NAME}" --no-pager >&2 || true
    exit 1
  fi
fi

"${NODE_BIN}" src/cli.mjs --state-dir "${STATE_DIR}" pair-info
