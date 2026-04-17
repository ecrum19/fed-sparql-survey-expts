#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ -f /etc/os-release ]]; then
  # shellcheck disable=SC1091
  source /etc/os-release
  if [[ "${ID:-}" != "ubuntu" ]]; then
    echo "This script is intended for Ubuntu. Detected: ${ID:-unknown}"
    exit 1
  fi
else
  echo "Cannot detect OS. /etc/os-release not found."
  exit 1
fi

SUDO=""
if [[ "${EUID}" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    echo "This script needs root privileges. Re-run as root or install sudo."
    exit 1
  fi
fi

echo "[1/6] Installing Ubuntu system packages..."
export DEBIAN_FRONTEND=noninteractive
${SUDO} apt-get install -y \
  ca-certificates \
  curl \
  git \
  gnupg \
  build-essential \
  python3 \
  python3-pip \
  python3-venv

node_major() {
  if ! command -v node >/dev/null 2>&1; then
    echo "0"
    return
  fi
  node -v | sed -E 's/^v([0-9]+).*/\1/'
}

CURRENT_NODE_MAJOR="$(node_major)"
if [[ "${CURRENT_NODE_MAJOR}" -lt 18 ]]; then
  echo "[2/6] Installing Node.js 20.x (NodeSource)..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | ${SUDO} -E bash -
  ${SUDO} apt-get install -y nodejs
else
  echo "[2/6] Node.js is already installed (v${CURRENT_NODE_MAJOR})."
fi

echo "[3/6] Ensuring Yarn is available..."
if command -v yarn >/dev/null 2>&1; then
  echo "Yarn already installed: $(yarn --version)"
else
  if command -v corepack >/dev/null 2>&1; then
    ${SUDO} corepack enable
    corepack prepare yarn@stable --activate
  elif command -v npm >/dev/null 2>&1; then
    ${SUDO} npm install -g yarn
  else
    echo "Error: neither corepack nor npm is available"
    exit 1
  fi
  echo "Yarn installed: $(yarn --version)"
fi

VENV_DIR="${VENV_DIR:-.venv}"
echo "[4/6] Creating/updating Python virtual environment at ${VENV_DIR}..."
python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"

echo "[5/6] Installing Python packages from requirements.txt..."
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt

echo "[6/6] Installing Comunica dependencies..."
if [[ -d "comunica/.git" ]]; then
  (
    cd comunica
    yarn install
  )
elif [[ -d "comunica" ]]; then
  echo "Existing ./comunica directory is not a git checkout; skipping automatic install."
  echo "Remove/rename ./comunica and re-run this script, or install manually."
else
  bash install_comunica.sh
fi

echo "Done. Activate the virtual env with: source ${VENV_DIR}/bin/activate"
