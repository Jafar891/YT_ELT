from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json



from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality


#define the local timezone
local_tz = pendulum.timezone("Europe/Warsaw")

#default arguments for the DAG
default_args = {
    "owner": "Jafar",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "emails": ["gahramanov.jafar@gmail.com"],
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
    
}

staging_schema = "staging"
core_schema = "core"

#1st DAG to extract video statistics and save as JSON
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="A DAG to extract video statistics from a YouTube channel and save it as a JSON file",
    schedule='0 14 * * *',  # Run daily at 14:00 (2 PM) local time
    catchup=False,
) as dag_produce:
    
    #define tasks
    playlist_id=get_playlist_id()
    video_ids=get_video_ids(playlist_id)
    extracted_data=extract_video_data(video_ids)
    save_to_json=save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",  # ID of the DAG to trigger
        wait_for_completion=False,  # Wait for the triggered DAG to complete before proceeding
    )

    #define task dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json >> trigger_update_db


#2nd DAG to process the JSON file and update the staging and core tables in the data warehouse
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="A DAG to process the JSON file and update the staging and core tables in the data warehouse",
    schedule=None,  # This DAG will be triggered by the first DAG, so we set schedule to None
    catchup=False,
) as dag_update:
    
    #define tasks
    update_staging_table = staging_table()
    update_core_table = core_table()

    trigger_data_quality_checks = TriggerDagRunOperator(
        task_id="trigger_data_quality_checks",
        trigger_dag_id="data_quality_checks",  # ID of the DAG to trigger
        wait_for_completion=False,  # Wait for the triggered DAG to complete before proceeding
    )

    #define task dependencies
    update_staging_table >> update_core_table >> trigger_data_quality_checks


#3rd DAG to run data quality checks on the processed data in the staging and core tables
with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="DAG to run data quality checks on the processed data",
    schedule=None,  # This DAG will be triggered by the second DAG, so we set schedule to None
    catchup=False,
) as dag_quality:
    
    #define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    #define task dependencies
    soda_validate_staging >> soda_validate_core