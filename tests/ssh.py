"""Local OpenSSH integration test. Requires cargo build, npm ci, and OpenBSD nc."""

import contextlib
import json
import os
import pty
import select
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def available_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def service(args, log, ready, confirm=False, **env):
    with contextlib.ExitStack() as stack:
        output = stack.enter_context(log.open("wb"))
        stdin, stderr = subprocess.DEVNULL, output
        if confirm:
            master, slave = pty.openpty()
            stack.callback(os.close, master)
            stack.callback(os.close, slave)
            stdin = stderr = slave
        proc = subprocess.Popen(args, stdin=stdin, stdout=output, stderr=stderr,
                                env={**{k: v for k, v in os.environ.items() if k != "TUNNEL_SECRET"}, **env},
                                start_new_session=True)
        try:
            if confirm:
                os.write(master, b"yes\n")
            deadline = time.monotonic() + 25
            while ready not in log.read_text():
                if proc.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError(log.read_text())
                if confirm and select.select([master], [], [], 0.1)[0]:
                    output.write(os.read(master, 8192))
                    output.flush()
                else:
                    time.sleep(0.1)
            yield
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=10)


def exercise(directory, dedicated):
    http_port, tcp_port = available_port(), available_port()
    config = directory / "wrangler.json"
    config.write_text(json.dumps({
        "name": "nbork-test", "main": str(ROOT / "src/worker.ts"),
        "compatibility_date": "2025-10-11",
        "vars": {"ROOT_DOMAIN": "nbork.test", "SSH_ROUTES": {"127.0.0.1": "demo"} if dedicated else {}},
        "connect": [{"protocol": "tcp", "port": tcp_port}],
        "durable_objects": {"bindings": [{"name": "TUNNEL", "class_name": "TunnelDO"}]},
        "migrations": [{"tag": "v1", "new_sqlite_classes": ["TunnelDO"]}],
    }))
    ssh_config = directory / "ssh_config"
    route = (f"HostName 127.0.0.1\n    Port {tcp_port}\n    HostKeyAlias demo.nbork.test"
             if dedicated else f"ProxyCommand nc -X connect -x 127.0.0.1:{tcp_port} %h 22")
    ssh_config.write_text(f"""Host *.nbork.test
    {route}
    StrictHostKeyChecking accept-new
    UserKnownHostsFile "{directory}/known_hosts"
    GlobalKnownHostsFile /dev/null
    PubkeyAuthentication no
    NumberOfPasswordPrompts 1
    ConnectTimeout 5
""")
    askpass = directory / "askpass"
    askpass.write_text('#!/bin/sh\necho asked >> "$PROMPTS"\nprintf "%s\\n" "$REPLY"\n')
    askpass.chmod(0o700)
    prompts = directory / "prompts"

    def login(host, expected, password=None, options=()):
        prompts.write_text("")
        result = subprocess.run(
            ["ssh", "-F", ssh_config, "-tt", *options, host],
            input=b'printf "remote-%d\\n" 42\nexit 7\n', capture_output=True, timeout=15,
            env={**os.environ, "SSH_ASKPASS_REQUIRE": "force", "SSH_ASKPASS": str(askpass),
                 "REPLY": password or "", "PROMPTS": str(prompts)})
        assert result.returncode == expected, (result.stdout, result.stderr)
        assert bool(prompts.read_text()) == (password is not None), result.stderr
        if expected == 7:
            assert b"remote-42" in result.stdout, result.stdout

    with contextlib.ExitStack() as stack:
        stack.enter_context(service(
            [ROOT / "node_modules/.bin/wrangler", "dev", "--config", config, "--port", str(http_port)],
            directory / "worker.log", "Ready on http", WRANGLER_SEND_METRICS="false", NO_COLOR="1"))

        def host(channel, *options, confirm=False):
            return service([ROOT / "target/debug/nbork", channel, "--base-url",
                            f"http://127.0.0.1:{http_port}", *options], directory / f"{channel}.log",
                           f"connected: {channel}", confirm=confirm, NBORK_SSH_DIR=str(directory / channel), SHELL="/bin/sh")

        with host("demo", confirm=True):
            login("whatever@demo.nbork.test", 7)
        with host("demo", "--no-ssh"):
            login("whatever@demo.nbork.test", 255)
        stack.enter_context(host("demo"))
        login("whatever@demo.nbork.test", 7)
        if not dedicated:
            stack.enter_context(host("locked", "--secret", "test-password"))
            login("other@locked.nbork.test", 255, options=["-o", "BatchMode=yes"])
            login("other@locked.nbork.test", 255, "incorrect")
            login("anyone@locked.nbork.test", 7, "test-password", ["-p", "45678"])
            stack.enter_context(host("web-only", "--no-ssh"))
            login("guest@web-only.nbork.test", 255)
            stack.enter_context(host("unapproved"))
            login("guest@unapproved.nbork.test", 255)
            login("guest@offline.nbork.test", 255)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        exercise(Path(tmp), dedicated=False)
        exercise(Path(tmp), dedicated=True)
    print("SSH shell, password authentication, and both TCP routing modes passed")
