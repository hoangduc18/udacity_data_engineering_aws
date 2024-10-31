from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    template_fields = ("query_get_data_from_stage", "target_table")

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_get_data_from_stage="",
                 target_table="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_get_data_from_stage = query_get_data_from_stage
        self.target_table = target_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading data into fact table from staging tables")

        sql = f"""
            INSERT INTO {self.target_table}
            {self.query_get_data_from_stage}
        """

        self.log.info(f"Executing SQL: {sql}")
        redshift_hook.run(sql)

        self.log.info(f"Task Completed")

