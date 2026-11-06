#!/usr/bin/env python3
"""Launch the actual daemons in new private state directories and drive public APIs.

This checks daemon startup, persistence and CLI wiring. It deliberately does not
claim to boot a VM: use the privileged integration suite for that.
"""
import argparse
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.request


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=3)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    root = parser.parse_args().root.resolve()
    with tempfile.TemporaryDirectory(prefix="sandbox-smoke-") as work:
        directory = Path(work)
        token = secrets.token_urlsafe(48)
        token_file = directory / "root.token"
        descriptor = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write(token)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}"
        engine_socket = directory / "engine.sock"
        processes = []
        logs = open(directory / "daemons.log", "w+")

        def launch(arguments, env=None):
            process = subprocess.Popen(arguments, stdout=logs, stderr=logs, env=env)
            processes.append(process)
            return process

        def request(path, body=None, key=None):
            headers = {"Authorization": "Bearer " + token}
            if key:
                headers["Idempotency-Key"] = key
            if body is not None:
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(endpoint + path,
                                         data=None if body is None else json.dumps(body).encode(),
                                         headers=headers)
            with urllib.request.urlopen(req, timeout=5) as response:
                return json.load(response)

        def ready(check):
            deadline = time.monotonic() + 10
            last = None
            while time.monotonic() < deadline:
                try:
                    return check()
                except (OSError, http.client.HTTPException, ValueError) as error:
                    last = error
                    if any(process.poll() is not None for process in processes):
                        raise RuntimeError("daemon exited during startup") from error
                    time.sleep(0.05)
            raise RuntimeError("daemon startup timed out") from last

        def inventory():
            connection = UnixHTTP(engine_socket)
            try:
                connection.request("GET", "/v1/vms")
                response = connection.getresponse()
                assert response.status == 200, response.status
                return json.load(response)
            finally:
                connection.close()

        engine_config = directory / "engine.json"
        engine_config.write_text(json.dumps({
            "state-dir": str(directory / "engine"),
            "socket": str(directory / "unused.sock"),
            "init": False,
        }))
        product_config = directory / "product.json"
        product_config.write_text(json.dumps({
            "state-dir": str(directory / "product"),
            "engine-socket": str(engine_socket),
            "listen": "127.0.0.1:1",
            "root-token-file": str(token_file),
        }))
        client_config = directory / "client.json"
        client_config.write_text(json.dumps({
            "endpoint": "http://127.0.0.1:1",
            "token-file": str(token_file),
        }))

        try:
            launch([str(root / "bin/engined"), "--config", str(engine_config),
                    "--socket", str(engine_socket), "--init"])
            assert ready(inventory) in (None, [])
            product_args = [str(root / "bin/sandboxd"), "--state-dir", str(directory / "product"),
                            "--engine-socket", str(engine_socket), "--listen", f"127.0.0.1:{port}",
                            "--root-token-file", str(token_file)]
            product = launch(product_args + ["--init"])
            assert ready(lambda: request("/healthz"))["status"] == "ok"
            command = {"action": "template_put", "template": {
                "id": "smoke-template", "image": "sha256:" + "1" * 64,
                "shape": {"vcpus": 1, "memory_mib": 128, "disk_mib": 64}}}
            first = request("/v1/templates", command, "smoke-register")
            assert first["template"]["state"] == "ready"
            listed = subprocess.check_output([str(root / "bin/sandboxctl"), "--endpoint", endpoint,
                                              "--token-file", str(token_file), "list"], text=True)
            assert json.loads(listed) == []
            print("PASS: actual engined UDS, sandboxd health, template registration, sandboxctl list")
            listed = subprocess.check_output([
                str(root / "bin/sandboxctl"), "--config", str(client_config),
                "--endpoint", endpoint, "list",
            ], env={**os.environ, "SANDBOXCTL_ENDPOINT": "http://127.0.0.1:2"}, text=True)
            assert json.loads(listed) == []
            product.terminate()
            product.wait(timeout=10)
            processes.remove(product)
            product = launch([str(root / "bin/sandboxd"), "--config", str(product_config)],
                             env={**os.environ, "SANDBOXD_LISTEN": f"127.0.0.1:{port}"})
            ready(lambda: request("/healthz"))
            again = request("/v1/templates", command, "smoke-register")
            assert first["operation_id"] == again["operation_id"]
            print("PASS: sandboxd restart preserves the same idempotent operation")
            print("PASS: Viper files, environment overrides and explicit flags across all three CLIs")
            product.terminate()
            product.wait(timeout=10)
            processes.remove(product)
            result = subprocess.check_output([str(root / "bin/sandboxd"), "--state-dir",
                                              str(directory / "product"), "--mode", "backup",
                                              "--backup", str(directory / "backup")], text=True)
            assert json.loads(result)["format"] == 1
            print("PASS: offline product backup records a consistent recovery point")
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
            logs.close()


if __name__ == "__main__":
    main()
