from prefect import flow, task
import subprocess

@task(name="Run Spark Job on YARN")
def run_spark_job():
    script_path = "/home/team/data-platform-homeworks/hw05-prefect/run_on_yarn.sh"
    
    print(f"Запускаем {script_path}...")
    result = subprocess.run(["bash", script_path], capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Spark job failed!\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
    
    print("Spark job finished successfully!")
    return "SUCCESS"

@flow(name="hw5-spark-pipeline")
def main_flow():
    run_spark_job()

if __name__ == "__main__":
    main_flow()
