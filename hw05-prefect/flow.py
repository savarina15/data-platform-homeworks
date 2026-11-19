import os
import subprocess
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run


def execute_launcher(flow_run_id: str) -> subprocess.CompletedProcess[str]:
    put_k_skriptu = Path(__file__).resolve().with_name("run_on_yarn.sh")
    child_env = os.environ.copy()
    child_env["HW5_FLOW_RUN_ID"] = flow_run_id

    try:
        return subprocess.run(
            ["bash", str(put_k_skriptu)],
            env=child_env,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as failure:
        raise RuntimeError(
            f"Запуск Spark завершился с кодом {failure.returncode}\n"
            f"{failure.stderr.strip()}"
        ) from failure


@task(name="Run Spark Job on YARN")
def run_spark_job(flow_run_id: str) -> str:
    journal = get_run_logger()
    journal.info("Запускаем Spark на YARN для flow_run_id=%s", flow_run_id)

    execution = execute_launcher(flow_run_id)
    yarn_reference = execution.stdout.strip()
    journal.info("%s", execution.stderr.strip())
    journal.info("flow_run_id=%s application_id=%s", flow_run_id, yarn_reference)
    return yarn_reference


@flow(name="hw5-spark-pipeline")
def main_flow() -> str:
    return run_spark_job(flow_run.id)


if __name__ == "__main__":
    main_flow()
