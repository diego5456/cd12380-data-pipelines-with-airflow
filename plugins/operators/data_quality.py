from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    sql_row_check = """
        SELECT COUNT(*) AS count
        FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_check_dict = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_check_dict = tables_check_dict

    def tables_has_rows(self, table, conn):
        records  = conn.get_records(DataQualityOperator.sql_row_check.format(table))
        self.log.info(f"Number of records in table '{table}': {records[0][0]}")
        assert len(records) > 1 or records[0][0] > 1, f"Data quality check failed. {self.table} returned no results"

    def null_check(self, table, query, conn):
        records = conn.get_records(query)
        self.log.info(f"Number of nulls in table '{table}': {records[0][0]}")
        assert records[0][0] < 1, f"Data quality check failed. {self.table} returned null records for non null column"

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table, query in self.tables_check_dict.items():
            self.tables_has_rows(table, redshift)
            self.null_check(table, query, redshift)



