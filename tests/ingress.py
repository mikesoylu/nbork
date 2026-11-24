"""Exercise the shared ingress with OpenSSH, TLS and a real local Worker."""

import base64
import http.client
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import tempfile
from contextlib import ExitStack

from ssh import ROOT, available_port, service


def check_web(port, cert):
    tls = ssl.create_default_context(cafile=str(cert))
    conn = http.client.HTTPSConnection("localhost", port, context=tls, timeout=5)
    conn.request("GET", "/setup.sh", headers={"Host": "nbork.test"})
    response = conn.getresponse()
    assert response.status == 302 and response.getheader("location").endswith("/main/setup.sh")
    conn.close()

    # Exercise an agent heartbeat through the TLS reverse proxy and the real DO.
    with socket.create_connection(("localhost", port), timeout=5) as sock:
        with tls.wrap_socket(sock, server_hostname="localhost") as wire, wire.makefile("rb") as reader:
            key = base64.b64encode(os.urandom(16)).decode()
            wire.sendall((f"GET /_connect/web-check HTTP/1.1\r\nHost: nbork.test\r\n"
                          f"Connection: Upgrade\r\nUpgrade: websocket\r\n"
                          f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
            assert reader.readline().startswith(b"HTTP/1.1 101 ")
            while reader.readline() not in (b"\r\n", b""):
                pass
            payload = b'{"type":"ping"}'
            mask = os.urandom(4)
            wire.sendall(bytes([0x81, 0x80 | len(payload)]) + mask +
                         bytes(ch ^ mask[i % 4] for i, ch in enumerate(payload)))
            opcode, size = struct.unpack("BB", reader.read(2))
            assert opcode == 0x81 and json.loads(reader.read(size))["type"] == "pong"


def exercise(tmp):
    http, tcp, web, clear, ssh = [available_port() for _ in range(5)]
    domain = "nbork.test"
    key, cert, tls_key = (tmp / k for k in ("host", "cert.pem", "tls.key"))
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key], check=True)
    config = tmp / "wrangler.json"
    config.write_text(json.dumps({
        "name": "nbork-ingress-test", "main": str(ROOT / "src/worker.ts"),
        "compatibility_date": "2025-10-11",
        "vars": {"ROOT_DOMAIN": domain, "WEB_ORIGIN_HOST": "127.0.0.1", "SSH_HOST_KEY": key.read_text()},
        "connect": [{"protocol": "tcp", "port": tcp}],
        "durable_objects": {"bindings": [{"name": "TUNNEL", "class_name": "TunnelDO"},
                                        {"name": "SSH_GATEWAY", "class_name": "SshGateway"}]},
        "migrations": [{"tag": "v1", "new_sqlite_classes": ["TunnelDO", "SshGateway"]}],
    }))
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                    "-keyout", tls_key, "-out", cert], check=True, capture_output=True)
    binary = tmp / "ingress"
    subprocess.run(["go", "build", "-o", binary, "."], cwd=ROOT / "ingress", check=True)
    known = tmp / "known_hosts"
    known.write_text(f"[127.0.0.1]:{ssh},[127.0.0.1]:{tcp} " + (tmp / "host.pub").read_text())
    askpass = tmp / "askpass"
    askpass.write_text('#!/bin/sh\necho asked >> "$PROMPTS"\nprintf "%s\\n" "$REPLY"\n')
    askpass.chmod(0o700)

    def login(name, code, password=None, marker=None, port=ssh, options=()):
        prompts = tmp / "prompts"
        prompts.write_text("")
        result = subprocess.run([
            "ssh", "-F", "/dev/null", "-tt", "-p", str(port), "-o", "HostName=127.0.0.1",
            "-o", f"UserKnownHostsFile={known}", "-o", "StrictHostKeyChecking=yes",
            "-o", "GlobalKnownHostsFile=/dev/null", "-o", "IdentityAgent=none",
            "-o", "NumberOfPasswordPrompts=1", *options, f"{name}@{domain}",
        ], input=b'printf "agent-%s\\n" "$NBORK_TEST_MARKER"\nexit 7\n', capture_output=True, timeout=15,
           env={**os.environ, "SSH_ASKPASS_REQUIRE": "force", "SSH_ASKPASS": str(askpass),
                "REPLY": password or "", "PROMPTS": str(prompts)})
        assert result.returncode == code, (name, result.stdout, result.stderr)
        assert bool(prompts.read_text()) == (password is not None), (name, result.stderr)
        if marker:
            assert f"agent-{marker}".encode() in result.stdout, result.stdout

    local = "127.0.0.1:"
    def agent(name, state, *args):
        return service([ROOT / "target/debug/nbork", name, "--base-url", "http://" + local + str(http), *args],
                       tmp / f"{name}.log", f"connected: {name}", NBORK_SSH_DIR=str(tmp / state),
                       NBORK_TEST_MARKER=state, SHELL="/bin/sh")

    with ExitStack() as stack:
        stack.enter_context(service([ROOT / "node_modules/.bin/wrangler", "dev", "--config", config, "--port", str(http)],
                                    tmp / "worker.log", "Ready on http", WRANGLER_SEND_METRICS="false"))
        stack.enter_context(service([binary, "-domain", domain, "-host-key", key, "-state", tmp / "state",
                                     "-ssh-listen", local + str(ssh), "-ssh-origin", local + str(tcp),
                                     "-web-origin", "http://" + local + str(http), "-https-listen", local + str(web),
                                     "-http-listen", local + str(clear), "-tls-cert", cert, "-tls-key", tls_key],
                                    tmp / "ingress.log", "serving SSH"))
        check_web(web, cert)
        stack.enter_context(agent("open", "first", "--ssh"))
        stack.enter_context(agent("web-only", "web-only", "--no-ssh"))
        with agent("locked", "original", "--secret", "correct"):
            for port in (ssh, tcp):
                login("open", 7, marker="first", port=port)
                login("locked", 255, "incorrect", port=port)
                login("locked", 7, "correct", "original", port)
                login("offline", 255, port=port)
                login("web-only", 255, port=port)
            login("locked", 7, "correct", "original", tcp, options=[
                "-o", f"ProxyCommand=nc -X connect -x 127.0.0.1:{tcp} ssh.{domain} 22"])
        with agent("locked", "replacement", "--secret", "correct"):
            login("locked", 255, port=tcp)
            login("locked", 255)  # Reject a changed host before offering it a password.


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        exercise(Path(directory))
    print("Worker and standalone SSH gateways, host keys, authentication, HTTPS and WebSockets passed")
