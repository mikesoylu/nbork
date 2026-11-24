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

Connect from another computer with stock OpenSSH. The username is the nbork channel name.

```sh
ssh demo@ssh.nbork.com
```

Use `nbork demo --no-ssh` for an HTTP and WebSocket tunnel only.

**Without a password, anyone who can reach the channel can open a shell.** On first use, confirm passwordless access in the host terminal. nbork remembers your approval for that channel and broker. An unattended launch without prior approval starts the web tunnel only. Passing `--ssh` explicitly enables shell access for that run.

To require a password, start nbork with `--secret <password>` or set `TUNNEL_SECRET`. OpenSSH prompts for the password and the host verifies it. The Cloudflare gateway terminates SSH and can read the password and shell traffic. On first connection, OpenSSH asks you to verify the gateway's host key. The gateway separately pins the agent's key, which nbork stores in `~/.nbork/ssh_host_ed25519_key`. Set `NBORK_SSH_DIR` to choose another directory.

### SSH limitations

Each connection supports one interactive shell. The public gateway listens on port 22. OpenSSH’s `-p` option changes the port dialed on the gateway. The following SSH features are unsupported.

- One-shot commands such as `ssh demo@ssh.nbork.com uptime`. Run `uptime` after connecting.
- File transfers with `scp`, `sftp`, or `rsync` over SSH.
- Port forwarding (`-L`, `-R`, `-D`), agent forwarding, and X11 forwarding.
- Client public-key login through `authorized_keys`.

### Hosting the SSH gateway

Spectrum sends port 22 on `ssh.nbork.com` to the Worker. A Durable Object runs the Go SSH gateway as WebAssembly and connects to the selected channel through an internal binding. The installer and agent WebSockets use `nbork.com`. [Operator setup](ssh-gateway/README.md)

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

Building the Worker requires Node.js and Go 1.26 or newer. Wrangler compiles the SSH gateway during its build.

```sh
npm ci
npm run build
npm run dev
```

Wrangler listens on HTTP port 8787 and TCP port 8788 locally. After `npm ci`, `npm run build`, and `cargo build`, run `python3 tests/ssh.py` to check the TCP routes and `python3 tests/ssh_gateway.py` to check OpenSSH login, host keys, the installer redirect, and agent WebSockets. Both suites start local services on temporary ports.

Pushing a tag such as `v0.3.0` builds stripped binaries for Linux, macOS, and Windows and publishes them with checksums to a GitHub release.
