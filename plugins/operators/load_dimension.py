from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 insert_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_only = insert_only
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        redshift  = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if not self.insert_only:
            self.log.info(f"Emptying table: {self.table}")
            delete_sql = f"DELETE FROM {self.table}"
            redshift.run(delete_sql)

        self.log.info(f"Loading '{self.table}' table")
        insert_query = f"""
            INSERT INTO {self.table}
            {self.select_query}
        """
        redshift.run(insert_query)
