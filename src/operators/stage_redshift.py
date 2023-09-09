from airflow.hooks import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="",
        prefix="",
        json_file=None,
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.template_sql = """COPY {} 
                            FROM '{}' 
                            ACCESS_KEY_ID '{}'
                            SECRET_ACCESS_KEY '{}'
                            FORMAT AS json '{}';
                        """
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = Variable.get("s3_bucket")
        self.prefix = prefix
        self.json_file = json_file if json_file else "auto"

    def execute(self, context):
        self.log.info("Starting S3 to Redshift copy")
        metastore = MetastoreBackend()
        aws_connection = metastore.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run("DELETE FROM {}".format(self.table))
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.prefix)

        sql = self.template_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_file,
        )
        redshift.run(sql)
        self.log.info(f"Data loaded into Redshift table {self.table} successfully")
