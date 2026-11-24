# Deploy the Cloudflare SSH gateway

The production endpoint is `ssh <channel>@ssh.nbork.com`. Deploy the Worker with Node.js and Go 1.26 or newer. Wrangler bundles the WebAssembly module built from the Go gateway and the matching Go runtime. Keep one Worker-origin Spectrum application on `ssh.nbork.com:22` with an empty `SSH_ROUTES` map. The public endpoint accepts stock OpenSSH and the existing HTTP CONNECT preface. [Cloudflare Spectrum setup](https://developers.cloudflare.com/spectrum/how-to/route-tcp-traffic-to-a-worker/)

Set the Worker secret `SSH_HOST_KEY` to a dedicated OpenSSH Ed25519 private key using `npx wrangler secret put SSH_HOST_KEY < /secure/path/ssh_host_ed25519_key`. Back up this key. Its public fingerprint identifies the gateway to every client. Each channel's Durable Object pins the first agent key it sees and rejects a replacement before forwarding a password. Preserve the agent key when moving a channel to a different machine.

Run `npm run deploy`, then start a test channel and connect from a second terminal using `ssh <channel>@ssh.nbork.com`. The Go gateway runs inside Workers, with one Durable Object per SSH session. The hostname used by existing agents and the installer stays on the HTTPS Worker route.
