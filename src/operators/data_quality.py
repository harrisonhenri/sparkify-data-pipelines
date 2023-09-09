from typing import Any

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        sql_query: str,
        expected_result: Any,
        redshift_conn_id="redshift",
        *args,
        **kwargs,
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.expected_result = sql_query
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info("Starting data quality check")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        response = redshift.run(self.sql_query)

        if response != self.expected_result:
            raise ValueError(
                f"Data quality check failed. {self.table} contained 0 rows"
            )

        self.log.info("Data quality check succeed")
