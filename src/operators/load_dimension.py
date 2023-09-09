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
        delete_table: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super(LoadDimensionalOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.delete_table = delete_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_table:
            self.log.info(f"Cleaning table {self.delete_table}")
            redshift.run("DELETE FROM {}".format(self.delete_table))
            return

        self.log.info("Starting dimensional table insert")
        redshift.run(self.sql_query)
        self.log.info("Data loaded into dimensional table")
