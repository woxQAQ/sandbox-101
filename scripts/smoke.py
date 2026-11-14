#!/usr/bin/env python3
"""Exercise one real Haven daemon and both protocol CLIs in private state.

This proves entrypoint, writer, persistence and offline metadata wiring. It does
not boot a VM or establish guest, kernel isolation or daemon lifecycle proof.
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    root = parser.parse_args().root.resolve()
    with tempfile.TemporaryDirectory(prefix="h24e.", dir="/dev/shm") as work:
        directory = Path(work)
        state = directory / "state"
        management = directory / "management.sock"
        token = secrets.token_urlsafe(48)
        token_file = directory / "root.token"
        descriptor = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write(token)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}"
        config = directory / "haven.json"
        config.write_text(json.dumps({
            "state-dir": str(state), "socket": str(directory / "unused.sock"),
            "listen": "127.0.0.1:1", "root-token-file": str(token_file),
            "gc-interval": "0s", "max-memory-mib": 8192, "max-vcpus": 64,
            "log-format": "json", "log-level": "warn",
        }))
        client_config = directory / "havenctl.json"
        client_config.write_text(json.dumps({
            "endpoint": "http://127.0.0.1:1", "token-file": str(token_file),
        }))
        loom_config = directory / "loomctl.json"
        loom_config.write_text(json.dumps({"socket": str(directory / "unused.sock")}))
        processes = []
        logs = open(directory / "haven.log", "w+")
        daemon = None

        def request(path, body=None, bearer=token):
            headers = {"Authorization": "Bearer " + bearer}
            if body is not None:
                headers["Content-Type"] = "application/json"
            query = urllib.request.Request(
                endpoint + path, headers=headers,
                data=None if body is None else json.dumps(body).encode(),
            )
            with urllib.request.urlopen(query, timeout=5) as response:
                return json.load(response)

        def command(binary, arguments, env=None, body=None, success=True):
            result = subprocess.run(
                [str(root / "bin" / binary), *arguments],
                input=None if body is None else json.dumps(body),
                text=True, capture_output=True, timeout=15,
                env={**os.environ, **(env or {})},
            )
            if success and result.returncode:
                raise RuntimeError(f"{binary} failed: {result.stderr}")
            if not success:
                assert result.returncode != 0, f"{binary} unexpectedly succeeded"
                return result
            return json.loads(result.stdout)

        def ready():
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if daemon.poll() is not None:
                    raise RuntimeError("Haven exited during startup")
                try:
                    assert request("/healthz")["status"] == "ok"
                    return
                except (OSError, http.client.HTTPException, ValueError):
                    time.sleep(0.05)
            raise RuntimeError("Haven startup timed out")

        def start(initialize):
            arguments = [str(root / "bin/haven"), "--config", str(config)]
            if initialize:
                arguments += ["--listen", f"127.0.0.1:{port}",
                              "--socket", str(management), "--init"]
            environment = {
                **os.environ, "HAVEN_LISTEN": f"127.0.0.1:{port}",
                "HAVEN_LOG_LEVEL": "info",
                "HAVEN_SOCKET": str(management),
                "ENGINE_STATE_DIR": str(directory / "legacy-engine-must-not-exist"),
                "SANDBOXD_STATE_DIR": str(directory / "legacy-sandboxd-must-not-exist"),
            }
            process = subprocess.Popen(arguments, stdout=logs, stderr=logs, env=environment)
            processes.append(process)
            return process

        def stop(process):
            process.terminate()
            process.wait(timeout=10)
            assert process.returncode == 0, "Haven did not shut down cleanly"
            processes.remove(process)

        haven_args = ["--config", str(client_config), "--endpoint", endpoint]
        loom_args = ["--config", str(loom_config), "--socket", str(management)]
        try:
            invalid = directory / "invalid.json"
            for settings, reason in [
                ({"engine-socket": "unused"}, "unknown key"),
                ({"max-vcpus": 1.5}, "decode config"),
                ({"socket": None}, "must not be null"),
            ]:
                invalid.write_text(json.dumps(settings))
                rejected = command("haven", ["--config", str(invalid)], success=False)
                assert reason in rejected.stderr
            rejected = command("haven", ["--engine-socket", "unused"], success=False)
            assert "unknown flag" in rejected.stderr
            rejected = command("haven", ["--tls-cert", "unused"], success=False)
            assert "provided together" in rejected.stderr
            rejected = command("haven", ["--log-level", "invalid"], success=False)
            assert "unsupported log level" in rejected.stderr
            rejected = command("haven", ["--log-format", "invalid"], success=False)
            assert "unsupported log format" in rejected.stderr
            print("PASS: configuration rejects legacy flags, unknown/null keys, fractional budgets and invalid logging")
            daemon = start(True)
            ready()
            business = command("havenctl", [*haven_args, "status"],
                               {"HAVENCTL_ENDPOINT": "http://127.0.0.1:2"})
            local = command("loomctl", [*loom_args, "status"],
                            {"LOOMCTL_SOCKET": str(directory / "wrong.sock")})
            assert business == local
            assert local["format"] == "haven-single-control" and local["version"] >= 9
            assert command("havenctl", [*haven_args, "list"]) == []
            tenant_file = directory / "tenant.json"
            tenant_file.write_text(json.dumps({
                "id": "smoke", "max_sandboxes": 2, "max_storage_mib": 256,
            }))
            tenant_file.chmod(0o600)
            tenant = command("havenctl", [*haven_args, "--request-file", str(tenant_file), "tenant"])
            assert tenant["id"] == "smoke" and tenant["token"]
            assert request("/v1/sandboxes", bearer=tenant["token"]) == []
            print("PASS: one Haven, both protocol CLIs, same store identity and tenant request file")
            malformed = directory / "malformed.json"
            for payload in ('{"unknown":true}', '{} {}', 'x' * ((1 << 20) + 1)):
                malformed.write_text(payload)
                command("havenctl", [*haven_args, "--request-file", str(malformed), "tenant"], success=False)
            rejected = command("havenctl", [*haven_args, "tenant"], body={"id": "ignored"}, success=False)
            assert "--request-file" in rejected.stderr
            rejected = command("havenctl", [*haven_args, "--request-file", "-", "tenant"], success=False)
            assert "--request-file" in rejected.stderr
            command("havenctl", [*haven_args, "--request-file", str(directory), "tenant"], success=False)
            fifo = directory / "intent.fifo"
            os.mkfifo(fifo, 0o600)
            rejected = command("havenctl", [*haven_args, "--request-file", str(fifo), "tenant"], success=False)
            assert "regular file" in rejected.stderr
            command("havenctl", [*haven_args, "--request-file", str(tenant_file), "list"], success=False)
            assert len(command("havenctl", [*haven_args, "list"])) == 0
            print("PASS: explicit request files reject unknown fields, multiple values, oversized input and stdin")

            rejected = command("haven", ["--config", str(config), "--listen", "127.0.0.1:0",
                                          "--socket", str(directory / "second.sock")], success=False)
            assert "another writer" in rejected.stderr
            blocked_backup = directory / "blocked-backup"
            rejected = command("haven", ["--state-dir", str(state), "--mode", "backup",
                                          "--backup", str(blocked_backup)], success=False)
            assert "another writer" in rejected.stderr and not blocked_backup.exists()
            print("PASS: second daemon and live offline-backup attempt respect writer exclusion")

            before = command("loomctl", [*loom_args, "status"])
            dry_run = command("loomctl", [*loom_args, "--dry-run", "gc"])["gc"]
            request_file = directory / "gc.json"
            request_file.write_text(json.dumps({"action": "gc", "gc": {"dry_run": True}}))
            from_file = command("loomctl", [*loom_args, "--request-file", str(request_file), "request"])["gc"]
            assert from_file["dry_run"] and not from_file.get("operation_id")
            request_file.write_text(json.dumps({"action": "gc", "key": "different", "gc": {}}))
            rejected = command("loomctl", [*loom_args, "--key", "expected", "--request-file", str(request_file), "request"], success=False)
            assert "key differs" in rejected.stderr
            command("loomctl", [*loom_args, "request"], success=False)
            after = command("loomctl", [*loom_args, "status"])
            assert dry_run["dry_run"] and not dry_run.get("operation_id")
            assert before["epoch"] == after["epoch"]
            collection = command("loomctl", [*loom_args, "--key", "smoke-collection", "gc"])
            operation_id = collection["gc"]["operation_id"]
            operation = command("loomctl", [*loom_args, "operation", operation_id])
            assert operation["status"] == "succeeded"
            print("PASS: read-only GC preserves epoch; management mutation has a durable operation")

            stop(daemon)
            daemon = start(False)
            ready()
            replay = command("loomctl", [*loom_args, "--key", "smoke-collection", "gc"])
            assert replay == collection
            status = command("havenctl", ["--config", str(client_config), "status"], {
                "HAVENCTL_ENDPOINT": endpoint, "SANDBOXCTL_ENDPOINT": "http://127.0.0.1:2",
            })
            assert status["store_id"] == local["store_id"]
            assert request("/v1/sandboxes", bearer=tenant["token"]) == []
            assert command("loomctl", ["--config", str(loom_config), "status"],
                           {"LOOMCTL_SOCKET": str(management)})["store_id"] == status["store_id"]
            assert not (directory / "legacy-engine-must-not-exist").exists()
            assert not (directory / "legacy-sandboxd-must-not-exist").exists()
            print("PASS: restart preserves operation identity; HAVEN/HAVENCTL/LOOMCTL precedence")
            stop(daemon)
            daemon = None

            backup = directory / "backup"
            point = command("haven", ["--state-dir", str(state), "--mode", "backup",
                                      "--backup", str(backup)])
            assert point["store"]["identity"] == status["store_id"]
            assert set(point["files"]) == {"haven.db", "node.key", "store.json", "HEAD.json"}
            for name, expected in point["files"].items():
                actual = "sha256:" + hashlib.sha256((backup / name).read_bytes()).hexdigest()
                assert actual == expected, name
            with sqlite3.connect(f"file:{backup / 'haven.db'}?mode=ro&immutable=1", uri=True) as database:
                assert database.execute("SELECT id FROM tenants").fetchall() == [("smoke",)]
                assert database.execute("SELECT scope_kind,status FROM operations WHERE id=?",
                                        (operation_id,)).fetchone() == ("node", "succeeded")
            assert json.loads((backup / "HEAD.json").read_text())["epoch"] == point["epoch"]
            logs.flush()
            logs.seek(0)
            entries = [json.loads(line) for line in logs.read().splitlines()]
            assert any(row["msg"] == "Haven listening" and row["level"] == "INFO" for row in entries)
            assert all("time" in row and "level" in row and "msg" in row for row in entries)
            print("PASS: JSON logging on stderr honors environment over file configuration")
            print("PASS: one offline backup contains business and management state plus matching key/HEAD")
        except BaseException:
            logs.flush()
            logs.seek(0)
            print(logs.read())
            raise
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
