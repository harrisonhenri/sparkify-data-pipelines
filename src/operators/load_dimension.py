from typing import Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionalOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        sql_query="",
        redshift_conn_id="redshift",
        table_name: Optional[str] = None,
        truncate = False
        *args,
        **kwargs,
    ):
        super(LoadDimensionalOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate = truncate
        self.table_name = table_name

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.table_name and self.truncate:
            self.log.info(f"Truncating table {self.table_name}")
            redshift.run("TRUNCATE TABLE {}".format(self.table_name))

        self.log.info("Starting dimensional table insert")
        redshift.run(self.sql_query)
        self.log.info("Data loaded into dimensional table")
