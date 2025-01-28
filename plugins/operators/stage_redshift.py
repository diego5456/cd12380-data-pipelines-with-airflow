from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 region="us-west-2",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.region=region
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift  = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info(f"Creating staging table: {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"Copying date from S3: {s3_path}")
        self.log.info("*****************************")
        copy_sql = StageToRedshiftOperator.sql_template.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.region,
            self.json_format
        )
        redshift.run(copy_sql)








