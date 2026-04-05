from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
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
    schedule=None,                  # fix 1: schedule_interval → schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transform"],
) as dag:

    wait_for_update_db = ExternalTaskSensor(
        task_id="wait_for_update_db",
        external_dag_id="update_db",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],       # fix 2: use DagRunState enum
        failed_states=[DagRunState.FAILED],
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