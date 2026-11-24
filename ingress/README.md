# Deploy the shared ingress

The ingress needs a public address that serves port 22 for SSH, port 443 for HTTPS and port 80 for redirects and certificate issuance. The existing nbork binary works with it. Cloudflare Pro permits one Worker-origin Spectrum application and a proxied DNS record cannot share its name with a Spectrum application. The ingress keeps that application on `ssh.nbork.com`. [Cloudflare hostname restrictions](https://developers.cloudflare.com/spectrum/reference/troubleshooting/#cannot-create-spectrum-application--dns-record-already-exists) · [Worker-origin application limits](https://developers.cloudflare.com/spectrum/how-to/route-tcp-traffic-to-a-worker/#prerequisites)

| Connection | Path |
| --- | --- |
| SSH | OpenSSH → apex ingress → `ssh.nbork.com:22` → Worker → channel's nbork agent |
| Apex HTTPS and agent WebSockets | Client → apex ingress → `https://broker.nbork.com` → Worker |
| Channel web traffic | Client → Cloudflare wildcard route → Worker |

The ingress terminates both SSH connections. It can read passwords and shell traffic. The agent still checks its own secret. Clients verify the gateway's SSH key. The gateway pins each agent's public key on first contact in `/var/lib/nbork-ingress/hosts/<channel>`. A changed key closes the connection before a password is sent. To pre-pin a channel or rotate its key, verify the public key with the machine's operator and install the one-line OpenSSH public key in that file. Back up the gateway key and channel pins together.

## Stage the service

1. Deploy the Worker from this revision with a proxied `broker.nbork.com` record covered by its wildcard route. The web origin must resolve directly to Cloudflare throughout cutover. Verify that `curl -sSI https://broker.nbork.com/setup.sh` returns the installer redirect. Keep the existing port-22 Spectrum application and its empty `SSH_ROUTES` map.
2. Prepare a Linux host with a stable public address and an administrative access path independent of its public port 22. Build with Go 1.26 or newer and install the binary and the systemd unit.

   ```sh
   cd ingress
   go build -o nbork-ingress .
   sudo install -m 0755 nbork-ingress /usr/local/bin/
   sudo install -m 0644 nbork-ingress.service /etc/systemd/system/
   sudo install -d -m 0700 /etc/nbork-ingress
   sudo ssh-keygen -t ed25519 -N '' -f /etc/nbork-ingress/ssh_host_ed25519_key
   ```

   Generate the gateway key once. Restore that key on replacement instances. The unit uses systemd credentials and persists state in `/var/lib/nbork-ingress`.

3. For pre-cutover testing, provision a publicly trusted certificate for `nbork.com` using DNS validation. Add a systemd drop-in (`sudo systemctl edit nbork-ingress`) pointing to the certificate and key.

   ```ini
   [Service]
   LoadCredential=cert:/etc/nbork-ingress/fullchain.pem
   LoadCredential=tls-key:/etc/nbork-ingress/privkey.pem
   ExecStart=
   ExecStart=/usr/local/bin/nbork-ingress -host-key %d/ssh-key -tls-cert %d/cert -tls-key %d/tls-key
   ```

   Run `sudo systemctl daemon-reload` and `sudo systemctl enable --now nbork-ingress`. For file-supplied certificates, restart the service after renewal to load the new pair. The base unit also supports automatic ACME issuance and renewal once public DNS and ports 80/443 reach the ingress.

## Verify and cut over

On a staging client, map `nbork.com` to the new IP temporarily using its hosts file. Leave `broker.nbork.com`, `ssh.nbork.com` and the wildcard on their existing Cloudflare addresses. From this client, check:

- `curl -sSI https://nbork.com/setup.sh` returns the GitHub redirect.
- Start `nbork ingress-check --secret <test-password>`. Run `ssh ingress-check@nbork.com` from a second terminal on that client. Verify the gateway fingerprint against the staged host key and open a shell with the test password. This exercises the agent's control WebSocket through the staged HTTPS ingress.
- Reach a known local web service through `https://ingress-check-<port>.nbork.com`.

Record the original apex DNS records and proxy settings. After review, change only the apex A/AAAA records to DNS-only addresses served by the new ingress. Publish an AAAA record only if the ingress accepts IPv6 on all three ports. Remove the staging override and repeat the checks through public DNS. Keep both backends available through DNS convergence. An interrupted agent reconnects automatically.

To roll back, restore the original apex records and Cloudflare proxy settings. Allow cached ingress addresses to expire before stopping the ingress. Existing clients can use the original Spectrum CONNECT route during rollback.
