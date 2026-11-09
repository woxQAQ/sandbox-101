#!/usr/bin/env bash
set -euo pipefail
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

# Only the transient service created below enters this branch. The fresh mount
# and network namespaces prevent host /run/netns and firewall modifications.
if [[ ${1:-} == --delegated ]]; then
  shift
  IFS= read -r membership < /proc/self/cgroup
  [[ $membership == 0::* ]] || { printf '%s\n' 'cgroup v2 required' >&2; exit 1; }
  service_root="/sys/fs/cgroup$(dirname -- "${membership#0::}")"
  [[ ${service_root##*/} == "$SANDBOX_PRODUCTION_USER_UNIT" ]] || {
    printf '%s\n' 'refusing changes outside the newly created transient service' >&2; exit 1;
  }
  [[ ! -s "$service_root/cgroup.procs" ]] || {
    # cgroupfs reports size zero even when populated; check the contents too.
    printf '%s\n' 'transient delegation root must be empty' >&2; exit 1;
  }
  if IFS= read -r occupied < "$service_root/cgroup.procs"; then
    printf 'transient delegation root is populated: %s\n' "$occupied" >&2; exit 1
  fi
  printf '%s' '+cpu +memory +io +pids' > "$service_root/cgroup.subtree_control"
  export SANDBOX_PRODUCTION_CGROUP_ROOT="$service_root/vm-scopes"
  mkdir -- "$SANDBOX_PRODUCTION_CGROUP_ROOT"
  printf '%s' '+cpu +memory +io +pids' > "$SANDBOX_PRODUCTION_CGROUP_ROOT/cgroup.subtree_control"
  export SANDBOX_PRODUCTION_PARENT_USER
  export SANDBOX_PRODUCTION_PARENT_NET
  export SANDBOX_PRODUCTION_PARENT_MNT
  SANDBOX_PRODUCTION_PARENT_USER=$(readlink /proc/self/ns/user)
  SANDBOX_PRODUCTION_PARENT_NET=$(readlink /proc/self/ns/net)
  SANDBOX_PRODUCTION_PARENT_MNT=$(readlink /proc/self/ns/mnt)
  exec unshare --user --map-auto --map-root-user --net --mount --pid --fork \
    --kill-child=KILL --mount-proc --propagation private -- \
    bash "$project_root/scripts/run-production-acceptance.sh" --inside "$@"
fi

if [[ ${1:-} == --inside ]]; then
  shift
  mount -t tmpfs -o mode=0755,nosuid,nodev,size=256m tmpfs /run
  mkdir /run/netns
  ip link set lo up
  sysctl -qw net.ipv4.ip_forward=1
  # This isolated network has no physical/external interface. Bound admission
  # sets and an unconditional forwarding drop impose a real zero-egress quota.
  nft -f - <<'NFT'
table inet sandbox_engine {
  set active_ifaces { type ifname; size 1024; }
  set active_ips { type ipv4_addr; size 1024; }
  chain forward { type filter hook forward priority -100; policy drop; }
}
NFT
  exec "$1" -test.run='^TestProductionMicroVMAcceptance$' -test.v -test.count=1 -test.timeout=20m
fi

materials_dir=${1:?usage: run-production-acceptance.sh MATERIALS_DIR OCI_LAYOUT [ARTIFACTS_DIR]}
oci_layout=${2:?a real bootable OCI layout is required}
artifacts_dir=${3:-"$project_root/build/production-acceptance"}
: "${SANDBOX_PRODUCTION_IO_MAX:?set an explicit real block-device io.max budget; tmpfs does not prove block-I/O enforcement}"
materials_dir=$(realpath -- "$materials_dir")
oci_layout=$(realpath -- "$oci_layout")
mkdir -p -- "$artifacts_dir"
artifacts_dir=$(realpath -- "$artifacts_dir")
run_dir=$(mktemp -d "$artifacts_dir/run.XXXXXXXX")
printf 'Production acceptance artifacts: %s\n' "$run_dir"
unit=
cleanup() {
  local result=$?
  trap - EXIT
  if [[ -n $unit ]]; then
    timeout 10s systemctl --user stop "$unit" >/dev/null 2>&1 || true
  fi
  printf '%s\n' "$result" > "$run_dir/exit-status"
  exit "$result"
}
trap cleanup EXIT

# The caller supplies the toolchain. Resolve search directories before the
# delegated service masks /run; no environment provider is invoked here.
resolved_path=
IFS=: read -r -a tool_paths <<< "$PATH"
for directory in "${tool_paths[@]}"; do
  if [[ -d $directory ]]; then
    resolved=$(realpath -- "$directory")
    resolved_path+="${resolved_path:+:}$resolved"
  fi
done
export PATH="$resolved_path"
export CGO_ENABLED=0
GOWORK=off go -C "$project_root/engine" build -o "$run_dir/engined" ./cmd/engined 2>&1 | tee "$run_dir/build.log"
GOWORK=off go -C "$project_root/sandboxd" build -o "$run_dir/sandboxd" ./cmd/sandboxd 2>&1 | tee -a "$run_dir/build.log"
GOWORK=off go -C "$project_root/sandboxd" test -c -tags=privileged -o "$run_dir/production.test" ./integration 2>&1 | tee -a "$run_dir/build.log"
bash "$project_root/scripts/check-kernel.sh" "$materials_dir/kernel.config"

unit="sandbox-production-acceptance-$(date +%s)-$$.service"
printf '%s\n' "$unit" > "$run_dir/systemd-unit.txt"
systemd-run --user --wait --pipe --collect --service-type=exec --unit="$unit" \
  --property='Delegate=cpu memory io pids' --property=DelegateSubgroup=runner \
  --property=MemoryMax=8G --property=MemorySwapMax=0 --property=TasksMax=2048 \
  --property=TimeoutStopSec=10s --property=RuntimeMaxSec=1300s \
  --setenv="PATH=$PATH" --setenv="HOME=$HOME" --setenv=GOMAXPROCS=2 \
  --setenv="SANDBOX_IMAGE_TOOL_ROOTS=${SANDBOX_IMAGE_TOOL_ROOTS:-}" \
  --setenv="SANDBOX_PRODUCTION_USER_UNIT=$unit" \
  --setenv="SANDBOX_PRODUCTION_ARTIFACTS=$run_dir" \
  --setenv="SANDBOX_PRODUCTION_MATERIALS=$materials_dir" \
  --setenv="SANDBOX_PRODUCTION_OCI_LAYOUT=$oci_layout" \
  --setenv="SANDBOX_PRODUCTION_IO_MAX=$SANDBOX_PRODUCTION_IO_MAX" \
  --setenv="SANDBOX_E2E_MEMORY_MIB=${SANDBOX_E2E_MEMORY_MIB:-512}" \
  bash "$project_root/scripts/run-production-acceptance.sh" --delegated "$run_dir/production.test" 2>&1 | tee "$run_dir/test.log"
