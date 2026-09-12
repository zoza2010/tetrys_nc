#!/usr/bin/env bash
# Measure Spain←Russia C on UDP 7494 before any tetrys WAN run.
# Same direction as tetrys: Russia sends, Spain receives (-R), payload 1350.
set -euo pipefail

RU_HOST="${RU_HOST:-sysops@185.41.43.122}"
ES_HOST="${ES_HOST:-sysops@213.96.39.104}"
SSH_PORT="${SSH_PORT:-49999}"
IPERF_PORT="${IPERF_PORT:-7494}"
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o IPQoS=none -o ServerAliveInterval=5 -p "$SSH_PORT")

remote_ru() { "${SSH[@]}" "$RU_HOST" "$@"; }
remote_es() { "${SSH[@]}" "$ES_HOST" "$@"; }

if remote_ru "ss -ulnp | grep -q ':${IPERF_PORT}'" 2>/dev/null; then
  echo "ERROR: UDP ${IPERF_PORT} is busy (tetrys still running?). Free it first." >&2
  exit 2
fi

IPERF_PID=$(remote_ru "nohup iperf3 -s -p ${IPERF_PORT} > /tmp/iperf3_s.log 2>&1 & echo \$!")
sleep 0.5
trap 'remote_ru "kill ${IPERF_PID} 2>/dev/null || true"' EXIT

echo "======== UDP 200M ========"
remote_es "iperf3 -c 185.41.43.122 -p ${IPERF_PORT} -u -R -b 200M -l 1350 -t 6 --get-server-output"
echo
echo "======== UDP 800M ========"
remote_es "iperf3 -c 185.41.43.122 -p ${IPERF_PORT} -u -R -b 800M -l 1350 -t 6 --get-server-output"
echo
echo "======== UDP 1000M ========"
remote_es "iperf3 -c 185.41.43.122 -p ${IPERF_PORT} -u -R -b 1000M -l 1350 -t 6 --get-server-output"
echo
echo "======== TCP -R 8s (Russia sender CC) ========"
remote_ru "sysctl -n net.ipv4.tcp_congestion_control"
remote_es "iperf3 -c 185.41.43.122 -p ${IPERF_PORT} -R -t 8 --get-server-output"
echo
echo "PATH_PROBE_DONE"
