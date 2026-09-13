#!/usr/bin/env bash
# Build and package release assets for all supported platforms.
#
# Usage: scripts/release.sh <version> [commit]
#   version: without leading "v", must match the release tag (e.g. 1.5.1-pevjant.1)
#   commit:  defaults to current HEAD; the tag must point at this commit
#
# Produces dist/cc-connect-v<version>-<os>-<arch>.tar.gz (linux/darwin) and
# .zip (windows) using the upstream npm wrapper's expected asset naming.
# Every binary is verified against its platform's magic bytes before packing.
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="${1:?usage: scripts/release.sh <version-without-v> [commit]}"
COMMIT="${2:-$(git rev-parse --short HEAD)}"
BUILD_TIME="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
OUT=dist/release
rm -rf "$OUT"
mkdir -p "$OUT"

if [ ! -f web/dist/index.html ]; then
  echo "web/dist missing — build the admin UI first: (cd web && npm install && npm run build)" >&2
  exit 1
fi

magic() { od -An -tx1 -N4 "$1" | tr -d ' \n'; }

for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do
  os=${target%/*} arch=${target#*/} ext=""
  [ "$os" = "windows" ] && ext=".exe"
  name="cc-connect-v${VERSION}-${os}-${arch}"

  # env assignments must prefix the go command — plain shell vars are not
  # exported and go build would silently fall back to the host platform.
  GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 \
    go build -trimpath \
    -ldflags "-s -w -X main.version=v${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
    -o "${OUT}/cc-connect${ext}" ./cmd/cc-connect

  got="$(magic "${OUT}/cc-connect${ext}")"
  case "$os" in
    linux)  want="7f454c46" ;;  # ELF
    darwin) want="cffaedfe" ;;  # Mach-O 64-bit little endian
    windows) want="4d5a" ;;     # PE ("MZ")
  esac
  if [ "${got:0:${#want}}" != "$want" ]; then
    echo "FATAL: ${name} has magic ${got}, want ${want} — wrong platform binary" >&2
    exit 1
  fi

  if [ "$os" = "windows" ]; then
    (cd "$OUT" && powershell -NoProfile -Command "Compress-Archive -Force 'cc-connect.exe' '${name}.zip'")
  else
    tar czf "${OUT}/${name}.tar.gz" -C "$OUT" cc-connect
  fi
  rm -f "${OUT}/cc-connect${ext}"
  echo "built ${name}"
done

echo
echo "Assets in ${OUT}/ — upload to the release tag v${VERSION}:"
ls -la "$OUT"
