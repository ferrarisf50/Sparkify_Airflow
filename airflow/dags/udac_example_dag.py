from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ferrarisf50',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=300),
    #'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

##### drop tables  #######

'''
drop_staging_events_table = PostgresOperator(
    task_id='Drop_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_table_drop
)

drop_staging_songs_table = PostgresOperator(
    task_id='Drop_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_songs_table_drop
)

drop_songplays_table = PostgresOperator(
    task_id='Drop_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songplays_table_drop
)

drop_songs_table = PostgresOperator(
    task_id='Drop_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songs_table_drop
)

drop_users_table = PostgresOperator(
    task_id='Drop_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.users_table_drop
)

drop_artists_table = PostgresOperator(
    task_id='Drop_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.artists_table_drop
)

drop_time_table = PostgresOperator(
    task_id='Drop_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_drop
)

next_operator1 = DummyOperator(task_id='Next_steps1',  dag=dag)
'''
#### Create tables  ####

create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_table_create
)

create_staging_songs_table = PostgresOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_songs_table_create
)

create_songplays_table = PostgresOperator(
    task_id='Create_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songplays_table_create
)

create_artists_table = PostgresOperator(
    task_id='Create_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.artists_table_create
)

create_songs_table = PostgresOperator(
    task_id='Create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songs_table_create
)

create_users_table = PostgresOperator(
    task_id='Create_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.users_table_create
)

create_time_table = PostgresOperator(
    task_id='Create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_create
)

next_operator2 = DummyOperator(task_id='Next_steps2',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    json_path = "s3://udacity-dend/log_json_path.json",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    json_path = "auto",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    delete_existing_data = True,
    sql_query=SqlQueries.songplays_table_insert
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    delete_existing_data = True,
    sql_query=SqlQueries.users_table_insert
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    table='songs',
    delete_existing_data = True,
    sql_query=SqlQueries.songs_table_insert
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table='artists',
    delete_existing_data = True,
    sql_query=SqlQueries.artists_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    delete_existing_data = True,
    sql_query=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=["songplays", "users", "songs", "artists", "time"],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAGs
'''
start_operator >> drop_staging_songs_table
start_operator >> drop_staging_events_table
start_operator >> drop_songplays_table
start_operator >> drop_artists_table
start_operator >> drop_songs_table
start_operator >> drop_users_table
start_operator >> drop_time_table

drop_staging_songs_table  >>  next_operator1
drop_staging_events_table  >> next_operator1
drop_songplays_table >>       next_operator1
drop_artists_table >>         next_operator1
drop_songs_table >>           next_operator1
drop_users_table >>           next_operator1
drop_time_table >>            next_operator1

next_operator1 >> create_staging_songs_table
next_operator1 >> create_staging_events_table
next_operator1 >> create_songplays_table
next_operator1 >> create_artists_table
next_operator1 >> create_songs_table
next_operator1 >> create_users_table
next_operator1 >> create_time_table
'''
start_operator >> create_staging_songs_table
start_operator >> create_staging_events_table
start_operator >> create_songplays_table
start_operator >> create_artists_table
start_operator >> create_songs_table
start_operator >> create_users_table
start_operator >> create_time_table


create_staging_events_table >> next_operator2
create_staging_songs_table >> next_operator2
create_songplays_table >> next_operator2
create_artists_table >> next_operator2
create_songs_table >> next_operator2
create_users_table >> next_operator2
create_time_table >> next_operator2

next_operator2 >> stage_events_to_redshift
next_operator2 >> stage_songs_to_redshift
'''
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
'''
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_users_dimension_table
load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_time_dimension_table

load_users_dimension_table >> run_quality_checks
load_songs_dimension_table >> run_quality_checks
load_artists_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
