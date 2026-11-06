#!/usr/bin/env bash
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [[ ${1:-} == --inside ]]; then
  run=$2
  [[ $(id -u) == 0 && $(id -g) == 0 &&
     $(readlink /proc/self/ns/user) != "$FS_PARENT_USER" &&
     $(readlink /proc/self/ns/mnt) != "$FS_PARENT_MNT" &&
     $(readlink /proc/self/ns/pid) != "$FS_PARENT_PID" ]] || {
    echo 'BLOCKED: fresh root user/mount/PID namespaces required' >&2
    exit 1
  }
  # All fixture files live on a new mount, including when a test is killed.
  mount -t tmpfs -o mode=1777,nosuid,nodev tmpfs "$run/fixtures"
  unset FS_IDENTITY_CHILD FS_WORKLOAD_PROBE FS_PROBE_ALLOWED FS_PROBE_SENTINEL FS_PROBE_SENTINEL_DEV FS_PROBE_SENTINEL_INO
  export TMPDIR="$run/fixtures" FS_IDENTITY_REQUIRED=1
  "$run/fs_identity" --list >"$run/test-list"
  "$run/fs_identity" --test-threads=1 --show-output | tee "$run/results"
  python3 - "$run/results" "$run/test-list" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
required = {
    "identity_fixture_proves_distinct_authorities",
    "tree_pull_authority_stays_on_created_descriptor",
    "tree_pull_descriptor_cleanup_and_cancellation",
    "tree_push_mixed_owner_rejected_before_commit",
    "tree_push_postcommit_cleanup_outcome_is_explicit",
    "tree_push_postcommit_sync_failure_remains_error",
    "tree_push_cleanup_success_is_reported",
}
listed = re.findall(r"^(\S+): test$", pathlib.Path(sys.argv[2]).read_text(), re.MULTILINE)
expected = set(listed)
if not expected or len(expected) != len(listed) or not required <= expected:
    sys.exit("BLOCKED: compiled test list must contain every required scenario")
passed = re.findall(r"^test (\S+) \.\.\. ok$", text, re.MULTILINE)
if set(passed) != expected or len(passed) != len(expected):
    sys.exit("BLOCKED: every compiled scenario must actually PASS exactly once")
if re.search(r"skip", text, re.IGNORECASE) or not re.search(
        rf"test result: ok\. {len(expected)} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;", text):
    sys.exit("BLOCKED: incomplete identity test run")
print(f"required dual-UID scenarios: {len(expected)} actual PASS; no skips")
PY
  exit 0
fi

for tool in cargo python3 unshare newuidmap newgidmap timeout mount; do
  command -v "$tool" >/dev/null || { echo "BLOCKED: missing $tool" >&2; exit 1; }
done
run=$(mktemp -d /tmp/guest-fs-isolation.XXXXXXXX)
trap 'rm -rf -- "$run"' EXIT
chmod 755 "$run"
mkdir "$run/fixtures"
cd "$root"
cargo test --locked -p sandbox-agent --features filesystem-fixtures --test fs_identity --no-run --message-format=json >"$run/compiler-artifacts.json"
python3 - "$run/compiler-artifacts.json" "$run/fs_identity" <<'PY'
import json
import pathlib
import shutil
import sys

artifacts = []
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    record = json.loads(line)
    if (record.get("reason") == "compiler-artifact"
            and record["target"]["name"] == "fs_identity"
            and "test" in record["target"]["kind"]
            and record.get("executable")):
        artifacts.append(record["executable"])
if len(artifacts) != 1:
    sys.exit("BLOCKED: expected one freshly compiled fs_identity executable")
shutil.copyfile(artifacts[0], sys.argv[2])
pathlib.Path(sys.argv[2]).chmod(0o755)
PY
export FS_PARENT_USER FS_PARENT_MNT FS_PARENT_PID
FS_PARENT_USER=$(readlink /proc/self/ns/user)
FS_PARENT_MNT=$(readlink /proc/self/ns/mnt)
FS_PARENT_PID=$(readlink /proc/self/ns/pid)
timeout --kill-after=5s 90s unshare --user --map-auto --map-root-user --mount --pid --fork --kill-child=KILL --mount-proc --propagation private -- bash "$root/scripts/run-guest-fs-isolation.sh" --inside "$run"
