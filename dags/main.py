from airflow import DAG
import pendulum
from api.video_stats import get_channel_id, get_video_id, extract_video_stats, save_to_json
from datetime import datetime, timedelta

local_tz = pendulum.timezone("Africa/Lagos")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": 'kingsleyejiofor128@gmail.com',
    'start_date': datetime(2024, 6, 1, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "dagrun_timeout": timedelta(minutes=60),
    "max_active_runs": 1
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG for getting json file from raw data',
    schedule_interval= "0 8 * * *",
    catchup=False
) as dag:
    
    playlist_id = get_channel_id()
    video_ids = get_video_id(playlist_id)
    extracted_stats = extract_video_stats(video_ids)
    save_to_json_var= save_to_json(extracted_stats)

    playlist_id >> video_ids >> extracted_stats >> save_to_json_var