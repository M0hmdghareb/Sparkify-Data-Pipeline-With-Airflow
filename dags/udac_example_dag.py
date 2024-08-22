from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator ,   LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Gharib',
    'reties' : 3 ,
    'retrie_delay' : timedelta(minutes=5) ,
    'depends_on_past' : False ,
    'provide_context' : True ,
    'email' : False
}

dag = DAG('udacity__dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval ='0 * * * *' ,
          start_date = datetime(2024, 8 , 21) ,
          catchup= False ,
          tags=['']
        )

with open(os.path.join(conf.get("core", "dags_folder"), "create_tables.sql")) as f:
    create_tables_sql = f.read()


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag ,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag ,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data",
    json_path="auto",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag ,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    table="songplays",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag ,
    redshift_conn_id="redshift",
    sql=SqlQueries.user_table_insert,
    table="users",
    truncate=True,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag ,
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert ,
    table="songs",
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag ,
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    table="artists",
    truncate=True,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag ,
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    table="time",
    truncate=True,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag ,
    redshift_conn_id="redshift",
    tables=["songplays", "songs", "artists", "time", "users"],
    schema="public",
    dq_checks=[
        {
            "check_sql": "select count(*) from {}.{} where title is null",
            "expected_result": 0,
        },
    ],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> [stage_events_to_redshift  , stage_songs_to_redshift] >>  load_songplays_table >> [load_song_dimension_table ,load_user_dimension_table ,load_artist_dimension_table , load_time_dimension_table] >> run_quality_checks >> end_operator