#!/usr/bin/env bash
set -euo pipefail
if ! command -v python3 >/dev/null 2>&1; then
    echo 'NOT TESTED: требуется Python 3 (стандартная библиотека)' >&2
    exit 2
fi
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 "$script_dir/../../shared/qa/check_cluster.py" tunnel "$@"
