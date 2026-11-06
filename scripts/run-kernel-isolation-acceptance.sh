#!/usr/bin/env bash
set -euo pipefail
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
artifacts_dir=${1:-"$project_root/build/kernel-isolation-acceptance"}
mkdir -p -- "$artifacts_dir"
artifacts_dir=$(realpath -- "$artifacts_dir")
export SANDBOX_KERNEL_ISOLATION_ARTIFACTS="$artifacts_dir"
test_binary="$artifacts_dir/kernel-isolation.test"
test_pattern=${SANDBOX_KERNEL_ISOLATION_TEST_PATTERN:-'^TestKernelIsolationRootless(Network|Cgroup)$'}
GOWORK=off go -C "$project_root/engine" test -c -tags=kernel_isolation -o "$test_binary" .

# DIRECT is for an already delegated test process. Missing namespaces or
# controllers fail acceptance; this runner never turns missing proof into a skip.
if [[ ${SANDBOX_KERNEL_ISOLATION_DIRECT:-0} == 1 ]]; then
  "$test_binary" -test.run="$test_pattern" \
    -test.v -test.count=1 -test.timeout=3m 2>&1 | tee "$artifacts_dir/test.log"
  exit
fi

# Request one fresh transient USER service. The user manager supplies delegation;
# no existing cgroup, unit definition, permission, or system policy is edited.
# DelegateSubgroup keeps the delegated root empty so tests can create their own
# sibling subtree and enable controllers only within that newly owned subtree.
unit="sandbox-kernel-acceptance-$(date +%s)-$$.service"
printf '%s\n' "$unit" > "$artifacts_dir/systemd-unit.txt"
cleanup() {
  local result=$?
  trap - EXIT
  timeout 10s systemctl --user stop "$unit" >/dev/null 2>&1 || true
  exit "$result"
}
trap cleanup EXIT
systemd-run --user --wait --pipe --collect --service-type=exec --unit="$unit" \
  --property='Delegate=cpu memory io pids' \
  --property=DelegateSubgroup=runner \
  --property=CPUAccounting=yes --property=MemoryAccounting=yes \
  --property=TasksAccounting=yes --property=IOAccounting=yes \
  --property=TimeoutStopSec=5s --property=RuntimeMaxSec=180s \
  --setenv="PATH=$PATH" --setenv="HOME=$HOME" \
  --setenv="SANDBOX_KERNEL_ISOLATION_ARTIFACTS=$artifacts_dir" \
  --setenv="SANDBOX_KERNEL_ISOLATION_USER_UNIT=$unit" \
  "$test_binary" -test.run="$test_pattern" \
    -test.v -test.count=1 -test.timeout=3m 2>&1 | tee "$artifacts_dir/test.log"
