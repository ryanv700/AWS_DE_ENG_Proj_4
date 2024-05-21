from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
#from final_project_sql_statements import SqlQueries


#References: The code throughout this proejct uses Udacity Course Material and UdacityGPT as references

S3_BUCKET = 'airflow-project2'
S3_SONG_KEY = 'song-data'
S3_LOG_KEY = 'log-data/2018/11'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-east-1'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow -RV',
    schedule_interval='0 * * * *'
)
def ryans_final_project():

    start_execution = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table='staging_events',
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOG_KEY,
        json_path=f's3://{S3_BUCKET}/log_json_path.json',
        region=REGION
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table='staging_songs',
        s3_bucket=S3_BUCKET,
        s3_key=S3_SONG_KEY,
        json_path='auto',
        region=REGION
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='songplays',
        sql=SqlQueries.songplay_table_insert,
        mode='delete-load',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='users',
        sql=SqlQueries.user_table_insert,
        mode='append',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='songs',
        sql=SqlQueries.song_table_insert,
        mode='append',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='artists',
        sql=SqlQueries.artist_table_insert,
        mode='append',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='time',
        sql=SqlQueries.time_table_insert,
        mode='append',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=REDSHIFT_CONN_ID,
        tests=[
        {'sql': "SELECT COUNT(*) FROM users", 'expected_result': 312},
        {'sql': "SELECT COUNT(*) FROM songs", 'expected_result': 51060},
    ],
    )
    
    end_execution = DummyOperator(
    task_id='end_execution',
    )

    # need to set up the order of operations below

    start_execution >> [stage_events_to_redshift, stage_songs_to_redshift]
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                            load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table,
    load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_execution
    
    
final_project_dag = ryans_final_project()