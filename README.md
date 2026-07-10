# nbork

A lightweight reverse tunnel that exposes any local port through an nbork channel.

## Install

Linux and macOS (x86-64 and ARM64):

```sh
curl -fsSL https://nbork.com/setup.sh | sh
```

The installer downloads the appropriate precompiled binary from the latest GitHub release, verifies its SHA-256 checksum, and installs it to `/usr/local/bin` when writable or `~/.local/bin` otherwise. It does not require Node.js.

To install a specific release or directory:

```sh
curl -fsSL https://nbork.com/setup.sh | NBORK_VERSION=v0.2.2 NBORK_INSTALL_DIR="$HOME/bin" sh
```

Windows x86-64 binaries are attached to each GitHub release for manual installation.

## Usage

Start the tunnel with a channel name:

```sh
nbork <channel-name>
```

Then access any port on the machine through the channel:

```sh
curl https://<channel-name>-<port>.nbork.com
```

For example, if an app is listening on port 3000:

```sh
nbork demo
curl https://demo-3000.nbork.com
```

The client proxies streaming HTTP request and response bodies as well as WebSockets, including WebSocket subprotocols used by tools such as Vite.

## Configuration

```text
Usage: nbork [OPTIONS] <CHANNEL>

Options:
  --base-url <URL>  Broker URL [env: TUNNEL_BASE] [default: https://nbork.com]
  --secret <VALUE>  Channel secret [env: TUNNEL_SECRET]
  -h, --help        Print help
  -V, --version     Print version
```

The channel can also be supplied through the `CHANNEL` environment variable.

## Development

Build and test the native client:

```sh
cargo test
cargo build --release
```

The Cloudflare Worker remains TypeScript-based:

```sh
npm ci
npm run build
npm run dev
```

Pushing a tag such as `v0.2.2` builds stripped binaries for Linux, macOS, and Windows and publishes them with checksums to a GitHub release.
