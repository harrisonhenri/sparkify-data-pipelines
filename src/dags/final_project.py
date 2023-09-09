from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from src.operators.data_quality import DataQualityOperator
from src.operators.load_dimension import LoadDimensionalOperator
from src.operators.load_fact import LoadFactOperator
from src.operators.stage_redshift import StageToRedshiftOperator
from src.sql.insert_queries import InsertQueries

default_args = {
    "owner": "sparkify",
    "start_date": pendulum.now(),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)
def final_project():
    start_operator = DummyOperator(task_id="Begin_execution")

    create_tables = PostgresOperator(
        task_id="Create_tables",
        postgres_conn_id="redshift",
        sql="../sql/create_tables.sql",
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        s3_key="log_data",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        s3_key="song_data/A/A",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        sql_query=InsertQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionalOperator(
        task_id="Load_user_dim_table",
        sql_query=InsertQueries.user_table_insert,
        table_name="users",
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionalOperator(
        task_id="Load_song_dim_table",
        sql_query=InsertQueries.song_table_insert,
        table_name="songs",
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionalOperator(
        task_id="Load_artist_dim_table",
        sql_query=InsertQueries.artist_table_insert,
        table_name="artists",
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionalOperator(
        task_id="Load_time_dim_table",
        sql_query=InsertQueries.time_table_insert,
        table_name="times",
        truncate=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
    )

    (
        start_operator
        >> create_tables
        >> [stage_events_to_redshift, stage_songs_to_redshift]
        >> load_songplays_table
        >> [
            load_user_dimension_table,
            load_song_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ]
        >> run_quality_checks
    )


final_project_dag = final_project()
