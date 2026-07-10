#!/bin/sh
set -eu

REPOSITORY="${NBORK_REPOSITORY:-mikesoylu/nbork}"
VERSION="${NBORK_VERSION:-latest}"

fail() {
  printf 'nbork installer: %s\n' "$*" >&2
  exit 1
}

download() {
  url=$1
  destination=$2
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 3 --retry-delay 1 "$url" -o "$destination"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$destination"
  else
    fail "curl or wget is required"
  fi
}

case "$(uname -s)" in
  Linux) os=unknown-linux-musl ;;
  Darwin) os=apple-darwin ;;
  *) fail "unsupported operating system: $(uname -s)" ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) arch=x86_64 ;;
  arm64 | aarch64) arch=aarch64 ;;
  *) fail "unsupported CPU architecture: $(uname -m)" ;;
esac

target="${arch}-${os}"
archive="nbork-${target}.tar.gz"

if [ -n "${NBORK_RELEASE_URL:-}" ]; then
  release_url=${NBORK_RELEASE_URL%/}
else
  case "$VERSION" in
    latest) release_url="https://github.com/${REPOSITORY}/releases/latest/download" ;;
    v*) release_url="https://github.com/${REPOSITORY}/releases/download/${VERSION}" ;;
    *) release_url="https://github.com/${REPOSITORY}/releases/download/v${VERSION}" ;;
  esac
fi

tmp_dir="${TMPDIR:-/tmp}/nbork-install.$$"
mkdir -m 700 "$tmp_dir" || fail "could not create temporary directory"
trap 'rm -rf "$tmp_dir"' 0 HUP INT TERM

printf 'Downloading nbork %s for %s...\n' "$VERSION" "$target"
download "${release_url}/${archive}" "$tmp_dir/$archive"
download "${release_url}/${archive}.sha256" "$tmp_dir/$archive.sha256"

expected=$(awk 'NR == 1 { print $1 }' "$tmp_dir/$archive.sha256")
case "$expected" in
  *[!0-9a-fA-F]* | '') fail "release checksum is invalid" ;;
esac
[ "${#expected}" -eq 64 ] || fail "release checksum is invalid"

if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp_dir/$archive" | awk '{ print $1 }')
elif command -v shasum >/dev/null 2>&1; then
  actual=$(shasum -a 256 "$tmp_dir/$archive" | awk '{ print $1 }')
elif command -v openssl >/dev/null 2>&1; then
  actual=$(openssl dgst -sha256 "$tmp_dir/$archive" | awk '{ print $NF }')
else
  fail "sha256sum, shasum, or openssl is required to verify the download"
fi

[ "$expected" = "$actual" ] || fail "release checksum did not match"
tar -xzf "$tmp_dir/$archive" -C "$tmp_dir"
[ -f "$tmp_dir/nbork" ] || fail "release archive did not contain nbork"

if [ -n "${NBORK_INSTALL_DIR:-}" ]; then
  install_dir=$NBORK_INSTALL_DIR
elif [ -d /usr/local/bin ] && [ -w /usr/local/bin ]; then
  install_dir=/usr/local/bin
else
  [ -n "${HOME:-}" ] || fail "HOME is not set; set NBORK_INSTALL_DIR explicitly"
  install_dir="$HOME/.local/bin"
fi

mkdir -p "$install_dir" || fail "could not create $install_dir"
if command -v install >/dev/null 2>&1; then
  install -m 0755 "$tmp_dir/nbork" "$install_dir/nbork"
else
  cp "$tmp_dir/nbork" "$install_dir/nbork"
  chmod 0755 "$install_dir/nbork"
fi

printf 'Installed nbork to %s/nbork\n' "$install_dir"
case ":${PATH:-}:" in
  *":$install_dir:"*) ;;
  *) printf 'Add %s to PATH, then run: nbork <channel-name>\n' "$install_dir" ;;
esac
