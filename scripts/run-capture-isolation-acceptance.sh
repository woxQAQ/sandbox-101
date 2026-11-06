#!/usr/bin/env bash
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [[ ${1:-} == --inside ]]; then
  [[ $(id -u) == 0 && $(readlink /proc/self/ns/user) != "$CAPTURE_PARENT_USER" && $(readlink /proc/self/ns/mnt) != "$CAPTURE_PARENT_MNT" && $(readlink /proc/self/ns/pid) != "$CAPTURE_PARENT_PID" ]] || { echo 'fresh root user/mount/PID namespaces required' >&2; exit 1; }
  export CAPTURE_ISOLATION_INSIDE=1
  exec "$2" -test.run='^TestCaptureIsolation' -test.v -test.count=1 -test.timeout=90s
fi
run=$(mktemp -d)
trap 'rm -rf -- "$run"' EXIT
GOWORK=off go -C "$root/engine" test -c -tags=capture_isolation -o "$run/capture.test" .
export CAPTURE_PARENT_USER CAPTURE_PARENT_MNT CAPTURE_PARENT_PID
CAPTURE_PARENT_USER=$(readlink /proc/self/ns/user)
CAPTURE_PARENT_MNT=$(readlink /proc/self/ns/mnt)
CAPTURE_PARENT_PID=$(readlink /proc/self/ns/pid)
unshare --user --map-auto --map-root-user --mount --pid --fork --kill-child=KILL --mount-proc --propagation private -- bash "$root/scripts/run-capture-isolation-acceptance.sh" --inside "$run/capture.test"
