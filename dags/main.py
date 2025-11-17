from airflow import DAG  # FIXED: Added missing import
from airflow.decorators import dag
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum

# Import tasks from video_stats
from video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

# Import tasks from dwh (FIXED: removed 'dags.' prefix)
from datawarehouse.dwh import staging_table, core_table

local_tz = pendulum.timezone("Asia/Yerevan")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
}

@dag(
    dag_id="produce_json",
    default_args=default_args,
    description='DAG to produce JSON file with YouTube video data',
    schedule='0 14 * * *',  # Daily at 2 PM Yerevan time
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    end_date=datetime(2030, 12, 31, tzinfo=local_tz),
    catchup=False,
    tags=['youtube', 'etl']
)
def youtube_etl_pipeline():
    """
    YouTube Data ETL Pipeline
    
    This DAG:
    1. Gets the channel's uploads playlist ID
    2. Fetches all video IDs from the playlist
    3. Extracts detailed video statistics
    4. Saves the data to a JSON file
    """
    
    # Get channel handle from Airflow Variables
    channel_handle = Variable.get("CHANNEL_HANDLE")
    
    # Define task dependencies using TaskFlow API
    playlist_id = get_playlist_id(channel_handle=channel_handle)
    video_ids = get_video_ids(playlistId=playlist_id)
    extracted_data = extract_video_data(video_ids=video_ids)
    save_json = save_to_json(extracted_data=extracted_data)
    
    # TaskFlow API automatically sets dependencies
    playlist_id >> video_ids >> extracted_data >> save_json


# Instantiate the DAG
youtube_dag = youtube_etl_pipeline()


# Second DAG for updating database
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    schedule="0 15 * * *",  # Daily at 3 PM Yerevan time (1 hour after produce_json)
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    end_date=datetime(2030, 12, 31, tzinfo=local_tz),
    catchup=False,
    tags=['youtube', 'database']
) as dag:
    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies
    # This means update_staging must complete successfully before update_core starts
    update_staging >> update_core