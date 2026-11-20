#!/usr/bin/env bash
set -euo pipefail

submit_to_yarn() {
  local spark_exit

  "$spark_command" \
    --master yarn --deploy-mode cluster --name "$imya_zapuska" \
    --conf "spark.hadoop.yarn.resourcemanager.hostname=$manager_host" \
    --conf spark.hadoop.hive.metastore.uris=thrift://team-37-en:9083 \
    --conf spark.yarn.submit.waitAppCompletion=true \
    --conf "spark.yarn.tags=$run_labels" \
    --num-executors 2 --executor-memory 512m --driver-memory 512m \
    "$put_zadachi" "$@" >"$submit_log" 2>&1 || {
      spark_exit=$?
      printf 'spark-submit завершился с кодом %s\n' "$spark_exit" >&2
      tail -n 30 "$submit_log" >&2
      return "$spark_exit"
    }
}

launcher_path=$(realpath -- "${BASH_SOURCE[0]}")
put_zadachi="${launcher_path%/*}/scripts/spark_task.py"
spark_command="${SPARK_HOME:-/home/team/spark-3.5.1-bin-hadoop3}/bin/spark-submit"
hadoop_root=${HADOOP_HOME:-/home/team/hadoop-3.4.0}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"$hadoop_root/etc/hadoop"}
manager_host=${YARN_RM_HOST:-team-37-nn}
manager_endpoint=${YARN_RM_URL:-"http://$manager_host:8088"}

submission_key=$(python3 -c 'from uuid import uuid4; print(uuid4())')
imya_zapuska="hw5-books-$submission_key"
run_labels="hw5-submit-id=$submission_key"
if [[ -n ${HW5_FLOW_RUN_ID:-} ]]; then
  run_labels+=",prefect-flow-run-id=$HW5_FLOW_RUN_ID"
fi

submit_log=$(mktemp "${TMPDIR:-/tmp}/$imya_zapuska.XXXXXX.log")
printf 'Spark: %s\nЛог: %s\n' "$imya_zapuska" "$submit_log" >&2
submit_to_yarn "$@"

exec python3 - "$manager_endpoint" "$imya_zapuska" "$run_labels" <<'PY'
import json
import re
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def confirmed_application_id(manager_endpoint, imya_zapuska, required_labels):
    filters = urlencode({"name": imya_zapuska, "applicationTypes": "SPARK"})
    report_request = Request(
        f"{manager_endpoint.rstrip('/')}/ws/v1/cluster/apps?{filters}",
        headers={"Accept": "application/json"},
    )
    with urlopen(report_request, timeout=15) as report_stream:
        reports = (json.load(report_stream).get("apps") or {}).get("app", [])

    named_reports = [record for record in reports if record["name"] == imya_zapuska]
    if len(named_reports) != 1:
        raise ValueError(f"Найдено приложений с именем {imya_zapuska}: {len(named_reports)}")

    report = named_reports[0]
    application_id = report["id"]
    observed_labels = {label.strip() for label in report["applicationTags"].split(",")}
    if (
        report["applicationType"].upper() != "SPARK"
        or not set(required_labels.split(",")) <= observed_labels
        or not re.fullmatch(r"application_[0-9]+_[0-9]+", application_id)
    ):
        raise ValueError("Ответ YARN не соответствует текущему запуску Spark")

    outcome = (report["state"], report["finalStatus"])
    print(f"YARN {application_id}: {' / '.join(outcome)}", file=sys.stderr)
    if outcome != ("FINISHED", "SUCCEEDED"):
        raise ValueError(report.get("diagnostics") or "Spark не завершился успешно")

    return application_id


try:
    print(confirmed_application_id(*sys.argv[1:]))
except (OSError, AttributeError, KeyError, TypeError, ValueError) as error:
    sys.exit(f"Ошибка проверки YARN: {error}")
PY
