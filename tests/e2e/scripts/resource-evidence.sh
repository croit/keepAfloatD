#!/usr/bin/env bash
set -euo pipefail

compose_file=${1:?compose file required}
project=${2:?project required}
shift 2

printf 'capture=artifact_time_snapshot counters_do_not_establish_latency_cause=true\n'
# Numeric allowlist only: never collect environments, argv, or full inspect output.
snapshot='
for file in /proc/uptime /proc/loadavg /proc/pressure/cpu /proc/pressure/memory /proc/pressure/io /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu.pressure /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.events /sys/fs/cgroup/memory.pressure /sys/fs/cgroup/io.pressure /sys/fs/cgroup/pids.current /sys/fs/cgroup/pids.max /sys/fs/cgroup/cpu/cpu.stat /sys/fs/cgroup/cpu/cpu.cfs_quota_us /sys/fs/cgroup/cpu/cpu.cfs_period_us; do
  printf "metric=%s\n" "$file"
  if test -r "$file"; then
    head -c 1024 "$file" || printf "\nunavailable status=%s\n" "$?"
  else
    printf "unavailable\n"
  fi
  printf "\n"
done'

probe() {
  local section=$1 status=0
  shift
  printf 'utc=%s section=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${section}"
  timeout -k 1s 2s "$@" 2>/dev/null | head -c 16384 || status=$?
  printf '\nprobe_status=%s\n' "${status}"
}

probe job-visible-resources sh -c "${snapshot}"
ids=()
for service in "${@:1:8}"; do
  status=0
  id=$(timeout -k 1s 2s docker compose -f "${compose_file}" -p "${project}" \
    ps -q --all "${service}" 2>/dev/null) || status=$?
  if [[ ${status} != 0 || ! ${id} =~ ^[a-f0-9]{12,64}$ ]]; then
    printf 'container_unavailable service=%s status=%s\n' "${service}" "${status}"
    continue
  fi
  ids+=("${id}")
  printf 'container service=%s id=%s\n' "${service}" "${id}"
  probe "container-${id}-limits" docker inspect --format \
    'pid={{.State.Pid}} oom_killed={{.State.OOMKilled}} nano_cpus={{.HostConfig.NanoCpus}} cpu_quota={{.HostConfig.CpuQuota}} cpu_period={{.HostConfig.CpuPeriod}} memory_limit={{.HostConfig.Memory}} pids_limit={{.HostConfig.PidsLimit}}' "${id}"
  probe "container-${id}-resources" docker exec "${id}" sh -c "${snapshot}"
done
if (( ${#ids[@]} )); then
  probe docker-stats docker stats --no-stream --format \
    '{{.ID}} cpu={{.CPUPerc}} memory={{.MemUsage}} memory_percent={{.MemPerc}} pids={{.PIDs}} block_io={{.BlockIO}} net_io={{.NetIO}}' "${ids[@]}"
fi
