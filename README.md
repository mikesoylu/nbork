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

## SSH shell

Start nbork on the computer you want to reach. SSH is enabled by default. The shell runs as the account that starts nbork and uses its default shell. A local SSH daemon is unnecessary.

```sh
nbork demo
```

With the shared ingress deployed, connect from another computer with stock OpenSSH. The username is the nbork channel name.

```sh
ssh demo@nbork.com
```

Use `nbork demo --no-ssh` for an HTTP and WebSocket tunnel only.

**Without a password, anyone who can reach the channel can open a shell.** On first use, confirm passwordless access in the host terminal. nbork remembers your approval for that channel and broker. An unattended launch without prior approval starts the web tunnel only. Passing `--ssh` explicitly enables shell access for that run.

To require a password, start nbork with `--secret <password>` or set `TUNNEL_SECRET`. OpenSSH prompts for the password and the host verifies it. The ingress terminates SSH and can read the password and shell traffic. On first connection, OpenSSH asks you to verify the gateway's host key. The gateway separately pins the agent's key, which nbork stores in `~/.nbork/ssh_host_ed25519_key`. Set `NBORK_SSH_DIR` to choose another directory.

### SSH limitations

Each connection supports one interactive shell. The public gateway listens on port 22. OpenSSH’s `-p` option changes the port dialed on the gateway. The following SSH features are unsupported.

- One-shot commands such as `ssh demo@nbork.com uptime`. Run `uptime` after connecting.
- File transfers with `scp`, `sftp`, or `rsync` over SSH.
- Port forwarding (`-L`, `-R`, `-D`), agent forwarding, and X11 forwarding.
- Client public-key login through `authorized_keys`.

### Hosting the SSH gateway

One ingress serves `nbork.com` on port 22 for SSH and port 443 for HTTPS and agent WebSockets. It uses the existing Cloudflare Worker and Spectrum endpoint to reach each channel. The operator configures the ingress once. [Deployment and cutover guide](ingress/README.md)

## Configuration

```text
Usage: nbork [OPTIONS] <CHANNEL>

Options:
  --base-url <URL>  Broker URL [env: TUNNEL_BASE] [default: https://nbork.com]
  --no-ssh          Disable the default SSH shell
  --ssh             Explicitly enable shell access for this run
  --secret <VALUE>  Channel secret (SSH login password) [env: TUNNEL_SECRET]
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

Wrangler listens on HTTP port 8787 and TCP port 8788 locally. After `cargo build` and `npm ci`, run `python3 tests/ssh.py` to exercise the Worker's TCP routes. With Go 1.26 or newer installed, run `python3 tests/ingress.py` to exercise the shared ingress with OpenSSH, HTTPS and an agent WebSocket. Both suites start local services on temporary ports.

Pushing a tag such as `v0.3.0` builds stripped binaries for Linux, macOS, and Windows and publishes them with checksums to a GitHub release.
