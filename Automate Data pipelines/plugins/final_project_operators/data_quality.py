from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables_list:
            self.log.info(f"Checking data quality for table {table}")
            result = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            number_of_records = result[0][0]
            if number_of_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows")
            self.log.info(
                f"Data quality on table {table} check passed with {result[0][0]} records")
