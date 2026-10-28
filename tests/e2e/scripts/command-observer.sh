#!/usr/bin/env bash
# Sourced after lib.sh; observers own no node resources beyond a read-only PID view.
COMMAND_OBSERVER_WORKERS=()
COMMAND_OBSERVER_DIR=
COMMAND_OBSERVER_RESULT=0

_command_observer_docker() { timeout -k 1s 2s docker "$@"; }

_command_observer_child_running() { jobs -pr | grep -Fxq -- "$1"; }

_command_observer_event() (
  umask 077
  printf '{"utc":"%s","event":"%s","status":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "${2:-0}" >>"${observer_events}"
)

_command_observer_target() {
  local selected project service image
  selected=$(_command_observer_docker compose -f "$COMPOSE_FILE" \
    -p "$COMPOSE_PROJECT_NAME" ps -q --all "$observer_node") || return 1
  [[ "$selected" =~ ^[a-f0-9]{64}$ ]] || return 1
  [[ -z "${observer_target:-}" || "$selected" == "$observer_target" ]] || return 1
  local snapshot
  snapshot=$(_command_observer_docker inspect --format \
    '{{.Id}}|{{index .Config.Labels "com.docker.compose.project"}}|{{index .Config.Labels "com.docker.compose.service"}}|{{.State.Running}}|{{.State.StartedAt}}|{{.Image}}' "$selected") || return 1
  IFS='|' read -r observer_target project service observer_running observer_started image <<<"$snapshot"
  [[ "$observer_target" == "$selected" && "$project" == "$COMPOSE_PROJECT_NAME" &&
    "$service" == "$observer_node" && "$image" =~ ^sha256:[a-f0-9]{64}$ &&
    "$observer_running" =~ ^(true|false)$ &&
    "$observer_started" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z$ ]] || return 1
  observer_target_image=$image
}

_command_observer_owned() {
  local snapshot owner target started image mode
  snapshot=$(_command_observer_docker inspect --format \
    '{{.Id}}|{{index .Config.Labels "com.croit.keepafloatd.observer.owner"}}|{{index .Config.Labels "com.croit.keepafloatd.observer.target"}}|{{index .Config.Labels "com.croit.keepafloatd.observer.started"}}|{{.Image}}|{{.HostConfig.PidMode}}|{{.State.Running}}|{{.State.ExitCode}}|{{.State.Status}}' "$observer_name") || return 1
  IFS='|' read -r observer_id owner target started image mode observer_alive observer_exit observer_state <<<"$snapshot"
  [[ "$observer_id" =~ ^[a-f0-9]{64}$ && "$owner" == "$COMMAND_OBSERVER_OWNER" &&
    "$target" == "$observer_target" && "$started" == "$observer_boot_started" &&
    "$image" == "$COMMAND_OBSERVER_IMAGE" && "$mode" == "container:$observer_target" &&
    "$observer_alive" =~ ^(true|false)$ && "$observer_exit" =~ ^[0-9]+$ ]]
}

_command_observer_remove() {
  # #41: a loaded runner needed more than 3 s for an observer's last sample and exit.
  local failed=0 remaining until_stop=$((SECONDS + 10))
  [[ -n "${observer_name:-}" ]] || return 0
  remaining=$(_command_observer_docker ps -aq --no-trunc --filter "name=^/$observer_name$") || return 1
  [[ -n "$remaining" ]] || return 0
  if ! _command_observer_owned; then
    _command_observer_event cleanup_identity_unverified 1
    return 1
  fi
  if [[ "$observer_alive" == true ]]; then
    _command_observer_docker kill --signal TERM "$observer_id" >/dev/null || failed=1
    while (( SECONDS < until_stop )); do
      _command_observer_owned || { failed=1; break; }
      [[ "$observer_alive" == true ]] || break
      sleep 0.1
    done
    if [[ "$observer_alive" == true ]]; then
      _command_observer_owned &&
        _command_observer_docker kill --signal KILL "$observer_id" >/dev/null || failed=1
    fi
  fi
  if _command_observer_owned && [[ "$observer_alive" == false ]]; then
    _command_observer_event observer_exit "$observer_exit"
    _command_observer_docker rm "$observer_id" >/dev/null || failed=1
  else
    _command_observer_event cleanup_still_running 1
    failed=1
  fi
  return "$failed"
}

_command_observer_drain() {
  local until_drain=$((SECONDS + 3)) status=0
  [[ -n "${observer_cli:-}" ]] || return 0
  while _command_observer_child_running "$observer_cli" && (( SECONDS < until_drain )); do sleep 0.1; done
  if _command_observer_child_running "$observer_cli"; then
    kill -TERM "$observer_cli" || status=1
    _command_observer_event client_drain_timeout 1
    until_drain=$((SECONDS + 2))
    while _command_observer_child_running "$observer_cli" && (( SECONDS < until_drain )); do sleep 0.1; done
    if _command_observer_child_running "$observer_cli"; then kill -KILL "$observer_cli" || status=1; fi
    wait "$observer_cli" || status=$?
    observer_cli=
    _command_observer_event client_forced_exit "$status"
    return 1
  fi
  wait "$observer_cli" || status=$?
  observer_cli=
  _command_observer_event client_exit "$status"
  [[ "$status" == 0 || ( "$status" == 137 && "${observer_gap:-false}" == true ) ]]
}

_command_observer_finish() {
  local status=$1
  trap - EXIT INT TERM
  _command_observer_remove || status=1
  _command_observer_drain || status=1
  _command_observer_remove || status=1
  if [[ -n "${observer_output:-}" && "${observer_gap:-false}" != true ]] &&
    ! grep -Eq '"event"[[:space:]]*:[[:space:]]*"final"' "$observer_output"; then
    _command_observer_event final_missing 1
    status=1
  fi
  _command_observer_event worker_finished "$status"
  exit "$status"
}

_command_observer_worker() {
  local observer_node=$1 observer_events="$COMMAND_OBSERVER_DIR/$1.events.jsonl"
  local observer_target= observer_name= observer_cli= observer_output= observer_boot_started=
  local observer_gap=false boot=0 ready_until=0 status=0
  trap '_command_observer_finish "$?"' EXIT
  trap 'exit 0' TERM INT
  while (( SECONDS < COMMAND_OBSERVER_DEADLINE )); do
    if ! _command_observer_target; then _command_observer_event target_unverified 1; exit 1; fi
    if [[ -n "$observer_name" && ( "$observer_running" == false || "$observer_started" != "$observer_boot_started" ) ]]; then
      observer_gap=true
      _command_observer_event target_exit_or_restart
      _command_observer_remove && _command_observer_drain || exit 1
      observer_name= observer_output=
    fi
    if [[ "$observer_running" == false ]]; then sleep 1; continue; fi
    if [[ -z "$observer_name" ]]; then
      ((boot += 1))
      ((boot <= 8)) || { _command_observer_event boot_limit 1; exit 1; }
      observer_boot_started=$observer_started observer_gap=false
      observer_name="kafd-observer-$COMMAND_OBSERVER_OWNER-$observer_node-$boot"
      observer_output="$COMMAND_OBSERVER_DIR/$observer_node.$boot.jsonl"
      printf '%s|%s|%s\n' "$observer_name" "$observer_target" "$observer_boot_started" \
        >"$COMMAND_OBSERVER_DIR/$observer_node.identity"
      printf '{"event":"attach","target":"%s","started":"%s","boot":%d}\n' \
        "$observer_target" "$observer_boot_started" "$boot" >>"$observer_events"
      # The worker deadline owns this attached client; the file limit also bounds CLI stderr.
      (ulimit -f 2048; exec docker run \
        --name "$observer_name" \
        --label "com.croit.keepafloatd.observer.owner=$COMMAND_OBSERVER_OWNER" \
        --label "com.croit.keepafloatd.observer.target=$observer_target" \
        --label "com.croit.keepafloatd.observer.started=$observer_boot_started" \
        --pid "container:$observer_target" --network none --cap-drop ALL --read-only \
        --security-opt no-new-privileges --memory 64m --memory-swap 64m \
        --cpus 0.25 --pids-limit 16 --pull never --log-driver none -i \
        --entrypoint python3 "$COMMAND_OBSERVER_IMAGE" -u - \
        --duration "$((COMMAND_OBSERVER_DEADLINE - SECONDS))") \
        <"$COMMAND_OBSERVER_SCRIPT" >"$observer_output" \
        2>"$COMMAND_OBSERVER_DIR/$observer_node.$boot.stderr" &
      observer_cli=$!
      ready_until=$((SECONDS + 15))
      sleep 0.2
      continue
    fi
    if ! _command_observer_owned; then
      if ((ready_until > SECONDS)) && _command_observer_child_running "$observer_cli"; then
        _command_observer_event observer_pending
        sleep 0.2
        continue
      fi
      _command_observer_event observer_unverified 1
      exit 1
    fi
    if [[ "$observer_alive" != true ]]; then
      # Re-read the target before treating PID-namespace death as a sampler failure.
      _command_observer_target || exit 1
      if [[ "$observer_running" == false || "$observer_started" != "$observer_boot_started" ]]; then continue; fi
      if [[ "$observer_state" == created ]] && ((ready_until > SECONDS)) &&
        _command_observer_child_running "$observer_cli"; then
        _command_observer_event observer_created_pending
        sleep 0.2
        continue
      fi
      _command_observer_event unexpected_observer_exit "$observer_exit"
      exit 1
    fi
    if grep -Eq '"event"[[:space:]]*:[[:space:]]*"ready"' "$observer_output"; then
      if ((ready_until)); then
        _command_observer_event ready
        : >"$COMMAND_OBSERVER_DIR/$observer_node.ready"
        ready_until=0
      fi
    elif (( SECONDS >= ready_until )); then
      _command_observer_event readiness_timeout 1
      exit 1
    fi
    sleep 1
  done
  _command_observer_event duration_limit 1
  exit 1
}

command_observer_start() {
  [[ "${E2E_OBSERVE_COMMANDS:-0}" == 1 && "${2:-}" == 27_follower_rejoin_election ]] || return 0
  [[ -z "$COMMAND_OBSERVER_DIR" && "${NODES[*]}" == 'node-a node-b node-c' &&
    "$COMPOSE_PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || return 1
  local observer_node=e2e-runner observer_target= observer_events ready_count pid node
  local ready_deadline=$((SECONDS + 15))
  COMMAND_OBSERVER_DEADLINE=$((SECONDS + 1800))
  COMMAND_OBSERVER_SCRIPT="${ROOT_DIR}/tests/e2e/scripts/command-observer.py"
  [[ -f "$COMMAND_OBSERVER_SCRIPT" && -d "${1:?artifact directory required}" ]] || return 1
  COMMAND_OBSERVER_DIR=$(umask 077; mktemp -d "$1/command-observer.XXXXXXXX") || return 1
  COMMAND_OBSERVER_OWNER=${COMMAND_OBSERVER_DIR##*.}
  observer_events="$COMMAND_OBSERVER_DIR/controller.events.jsonl"
  _command_observer_target && [[ "$observer_running" == true ]] || {
    _command_observer_event runner_unverified 1; COMMAND_OBSERVER_RESULT=1; return 1;
  }
  COMMAND_OBSERVER_IMAGE=$observer_target_image
  for node in "${NODES[@]}"; do
    [[ "$node" =~ ^node-[abc]$ ]] || return 1
    (umask 077; _command_observer_worker "$node") &
    COMMAND_OBSERVER_WORKERS+=("$!")
  done
  while (( SECONDS < ready_deadline )); do
    ready_count=0
    for pid in "${COMMAND_OBSERVER_WORKERS[@]}"; do
      _command_observer_child_running "$pid" || { _command_observer_event worker_exited_before_ready 1; return 1; }
    done
    for node in "${NODES[@]}"; do [[ ! -f "$COMMAND_OBSERVER_DIR/$node.ready" ]] || ((ready_count += 1)); done
    ((ready_count == 3)) && return 0
    sleep 0.1
  done
  _command_observer_event readiness_timeout 1
  return 1
}

command_observer_stop() {
  local pid node status=$COMMAND_OBSERVER_RESULT deadline=$((SECONDS + 20))
  [[ -n "$COMMAND_OBSERVER_DIR" ]] || return "$status"
  local observer_events="$COMMAND_OBSERVER_DIR/controller.events.jsonl"
  for pid in "${COMMAND_OBSERVER_WORKERS[@]}"; do
    if _command_observer_child_running "$pid"; then kill -TERM "$pid" || status=1; fi
  done
  for pid in "${COMMAND_OBSERVER_WORKERS[@]}"; do
    while _command_observer_child_running "$pid" && ((SECONDS < deadline)); do sleep 0.1; done
    if _command_observer_child_running "$pid"; then
      _command_observer_event worker_stop_timeout 1
      kill -KILL "$pid" || status=1
      wait "$pid" || status=1
      for node in "${NODES[@]}"; do
        if [[ -f "$COMMAND_OBSERVER_DIR/$node.identity" ]]; then
          local observer_name observer_target observer_boot_started
          IFS='|' read -r observer_name observer_target observer_boot_started <"$COMMAND_OBSERVER_DIR/$node.identity"
          _command_observer_remove || status=1
        fi
      done
      status=1
    else
      wait "$pid" || status=1
    fi
  done
  COMMAND_OBSERVER_WORKERS=()
  COMMAND_OBSERVER_RESULT=$status
  _command_observer_event stopped "$status"
  COMMAND_OBSERVER_DIR=
  return "$status"
}
