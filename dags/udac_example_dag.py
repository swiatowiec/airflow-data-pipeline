from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('')
# AWS_SECRET = os.environ.get('')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
     default_args = default_args,
     description = 'Load and transform data in Redshift with Airflow',
     schedule_interval = '0 * * * *',
     catchup = False
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    provide_context = False,
    dag = dag,
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    redshift_conn_id ="redshift",
    aws_conn_id = "aws_credentials",
    region = "us-west-2",
    data_format = "JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id ='Stage_songs',
    dag = dag,
    provide_context = False,
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    redshift_conn_id ="redshift",
    aws_conn_id ="aws_credentials",
    region ="us-west-2",
    data_format ="JSON",
)

load_songplays_table = LoadFactOperator(
    task_id ='Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "songplay",
    sql = "songplay_table_insert",
    append_only = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql = "user_table_insert",
    append_only = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "song",
    sql = "song_table_insert",
    append_only = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "artist",
    sql = "artist_table_insert",
    append_only = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "time",
    sql = "time_table_insert",
    append_only = False
)

checks=[{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0, comparison: '>'},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0, comparison: '>'}]

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = "redshift",
    tables = ["songplay", "users", "song", "artist", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

