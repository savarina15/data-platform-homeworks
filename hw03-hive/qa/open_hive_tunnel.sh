#!/usr/bin/env bash
# Local-only SSH tunnel to HiveServer2
set -euo pipefail
usage() {
  echo 'Usage: open_hive_tunnel.sh --edge user@host --identity-file FILE [--local-port 11000] [--remote-host 127.0.0.1] [--remote-port 10000] [--dry-run]'
}
edge='' key='' local_port=11000 remote_port=10000 remote_host=127.0.0.1 dry_run=0
while (($#)); do
  case "$1" in
    --edge|--identity-file|--local-port|--remote-host|--remote-port)
      (($# >= 2)) || { echo "Missing value: $1" >&2; exit 2; }
      case "$1" in
        --edge) edge=$2 ;;
        --identity-file) key=$2 ;;
        --local-port) local_port=$2 ;;
        --remote-host) remote_host=$2 ;;
        --remote-port) remote_port=$2 ;;
      esac
      shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ "$edge" =~ ^[[:alnum:]_.-]+@[[:alnum:]_.-]+$ ]] || { echo 'Invalid --edge' >&2; exit 2; }
[[ "$remote_host" =~ ^[[:alnum:]_.-]+$ ]] || { echo 'Invalid --remote-host' >&2; exit 2; }
for port in "$local_port" "$remote_port"; do
  [[ "$port" =~ ^[0-9]{1,5}$ ]] && ((10#$port >= 1 && 10#$port <= 65535)) || {
    echo "Invalid port: $port" >&2; exit 2;
  }
done
if [[ "$key" == '~/'* ]]; then key="$HOME/${key#\~/}"; fi
[[ -r "$key" ]] || { echo "Unreadable SSH key: $key" >&2; exit 2; }

cmd=(ssh -F /dev/null -N -T -i "$key" -o IdentitiesOnly=yes
  -o StrictHostKeyChecking=yes -o ExitOnForwardFailure=yes
  -o ConnectTimeout=10 -o ServerAliveInterval=30
  -L "127.0.0.1:${local_port}:${remote_host}:${remote_port}" "$edge")
printf 'HiveServer2 tunnel: jdbc:hive2://127.0.0.1:%s/default\n' "$local_port"
if ((dry_run)); then
  printf 'DRY RUN: '; printf '%q ' "${cmd[@]}"; printf '\n'; exit 0
fi
exec "${cmd[@]}"
