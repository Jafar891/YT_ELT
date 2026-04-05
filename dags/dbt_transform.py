from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/dbt/yt_elt"
DBT_PROFILES_DIR = "/opt/airflow/dbt/yt_elt"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_transform",
    default_args=default_args,
    description="Run dbt models after update_db completes",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transform"],
) as dag:

    wait_for_update_db = ExternalTaskSensor(
        task_id="wait_for_update_db",
        external_dag_id="update_db",
        external_task_id=None,  # waits for the whole DAG to complete
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        timeout=3600,
        poke_interval=30,
        mode="poke",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    wait_for_update_db >> dbt_deps >> dbt_run >> dbt_test
