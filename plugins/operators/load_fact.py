from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_only=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_only = insert_only


    def execute(self, context):
        redshift  = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if not self.insert_only:
            self.log.info("Emptying table: songplays")
            delete_sql = f"DELETE FROM songplays"
            redshift.run(delete_sql)

        self.log.info("Loading songplays table")
        insert_query = f"""
            INSERT INTO songplays
            {SqlQueries.songplay_table_insert}
        """
        redshift.run(insert_query)
