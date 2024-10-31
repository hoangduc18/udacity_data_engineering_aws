from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task_group
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from udacity.common import final_project_sql_statements
from airflow.models import Variable

default_args = {
    'owner': 'Duc Hoang Can',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    
    @task_group(group_id='Copy_Data_from_S3')
    def S3_to_Redshift():
        stage_events_to_redshift = StageToRedshiftOperator(
            task_id="Stage_events",
            aws_credentials_id="aws_credentials",
            table="staging_events",
            redshift_conn_id="redshift",
            s3_bucket=Variable.get('s3_bucket_final'),
            s3_prefix=Variable.get('LOG_DATA'),
            json_format=Variable.get('LOG_JSONPATH')
        )

        stage_songs_to_redshift = StageToRedshiftOperator(
            task_id='Stage_songs',
            aws_credentials_id="aws_credentials",
            table='staging_songs',
            redshift_conn_id='redshift',
            s3_bucket=Variable.get('s3_bucket_final'),
            s3_prefix=Variable.get('SONG_DATA')
        )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        query_get_data_from_stage = final_project_sql_statements.SqlQueries.songplay_table_insert,
        target_table="songplays"
    )

    @task_group(group_id='Load_Dimension')
    def Load_dimensions():
        load_user_dimension_table = LoadDimensionOperator(
            task_id='Load_user_dim_table',
            redshift_conn_id="redshift",
            query_get_data_from_stage = final_project_sql_statements.SqlQueries.user_table_insert,
            target_table="users",
            insert_mode="truncate"
        )

        load_song_dimension_table = LoadDimensionOperator(
            task_id='Load_song_dim_table',
            redshift_conn_id="redshift",
            query_get_data_from_stage=final_project_sql_statements.SqlQueries.song_table_insert,
            target_table="songs",
            insert_mode="truncate"
        )

        load_artist_dimension_table = LoadDimensionOperator(
            task_id='Load_artist_dim_table',
            redshift_conn_id="redshift",
            query_get_data_from_stage=final_project_sql_statements.SqlQueries.artist_table_insert,
            target_table="artists",
            insert_mode="truncate"
        )

        load_time_dimension_table = LoadDimensionOperator(
            task_id='Load_time_dim_table',
            redshift_conn_id="redshift",
            query_get_data_from_stage=final_project_sql_statements.SqlQueries.time_table_insert,
            target_table="times",
            insert_mode="truncate"
        )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables_list=["songplays", "users", "songs", "artists", "times"]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> S3_to_Redshift() >> load_songplays_table
    load_songplays_table >> Load_dimensions() >> run_quality_checks
    run_quality_checks >> end_operator



final_project_dag = final_project()
