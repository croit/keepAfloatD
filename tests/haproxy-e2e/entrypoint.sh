#!/bin/sh
set -eu

/usr/local/bin/haproxy 0.0.0.0:80 10.50.0.30:8080 &
haproxy_pid=$!

cleanup() {
    kill "${keepafloatd_pid:-0}" 2>/dev/null || true
    kill "${haproxy_pid}" 2>/dev/null || true
    wait "${keepafloatd_pid:-0}" 2>/dev/null || true
    wait "${haproxy_pid}" 2>/dev/null || true
}

trap cleanup INT TERM

/usr/local/bin/keepafloatd "$@" &
keepafloatd_pid=$!

wait "${keepafloatd_pid}"
status=$?
kill "${haproxy_pid}" 2>/dev/null || true
wait "${haproxy_pid}" 2>/dev/null || true
exit "${status}"
