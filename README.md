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

From another terminal, connect to the channel's SSH hostname. Any SSH username works.

```sh
ssh whatever@demo.nbork.com
```

Use `nbork demo --no-ssh` for an HTTP and WebSocket tunnel only.

**Without a password, anyone who can reach the channel can open a shell.** On first use, confirm passwordless access in the host terminal. nbork remembers your approval for that channel and broker. An unattended launch without prior approval starts the web tunnel only. Passing `--ssh` explicitly enables shell access for that run.

To require a password, start nbork with `--secret <password>` or set `TUNNEL_SECRET` before starting it. OpenSSH prompts for the password and nbork verifies it on the host. The first connection also has OpenSSH's usual host identity prompt. nbork stores its Ed25519 host key in `~/.nbork/ssh_host_ed25519_key`. Set `NBORK_SSH_DIR` to choose another directory.

### Configure Cloudflare TCP routing

Deploy the Worker, then create a Spectrum TCP application with that Worker as its origin and edge port **22**. Spectrum passes the SSH traffic through with edge TLS termination off. Worker origins are available on Pro and Business zones and on zones with Spectrum Enterprise. Pro and Business zones without Spectrum Enterprise get one Worker-origin application on one TCP port. [Cloudflare setup instructions](https://developers.cloudflare.com/spectrum/how-to/route-tcp-traffic-to-a-worker/)

**For a single machine**, name the Spectrum application `demo.nbork.com` and set the following entry in the Worker's `vars` in `wrangler.jsonc` before deploying. All TCP connections to this Worker then go to `demo`. The SSH command above works with a stock client and its default configuration.

```json
"SSH_ROUTES": { "*": "demo" }
```

For multiple channel hostnames with dedicated Spectrum IPs, map each destination IPv4 and IPv6 address to its channel in `SSH_ROUTES`. Use assigned static IPs for a stable mapping. Spectrum supplies the Worker with the destination IP and port. SSH sends its username after encryption and omits the destination hostname. [Cloudflare connection addresses](https://developers.cloudflare.com/workers/runtime-apis/handlers/connect/#connection-addresses)

**To share one Spectrum address across arbitrary channels**, name the application `ssh.nbork.com`, leave its address unmapped, and add this once to `~/.ssh/config` on each connecting computer. This uses the HTTP CONNECT mode of OpenBSD netcat (`nc`) to pass the channel hostname to the TCP listener.

```sshconfig
Host *.nbork.com
    ProxyCommand nc -X connect -x ssh.nbork.com:22 %h 22
```

With this configuration, `ssh whatever@demo.nbork.com` routes to `demo` even if the client supplies another username or `-p` value. For a direct TCP connection, the client must dial a provisioned Spectrum edge port. The edge port never selects a local service.

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

Wrangler listens on HTTP port 8787 and TCP port 8788 locally. To exercise SSH without a Spectrum application, run the following in two additional terminals.

```sh
cargo run -- demo --base-url http://127.0.0.1:8787
ssh -o 'ProxyCommand=nc -X connect -x 127.0.0.1:8788 %h 22' whatever@demo.nbork.com
```

After `cargo build` and `npm ci`, run `python3 tests/ssh.py` on macOS or Linux to exercise both TCP routing modes and password handling with OpenSSH and OpenBSD netcat.

Pushing a tag such as `v0.2.2` builds stripped binaries for Linux, macOS, and Windows and publishes them with checksums to a GitHub release.
