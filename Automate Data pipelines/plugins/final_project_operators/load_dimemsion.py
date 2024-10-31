from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    template_fields = ("query_get_data_from_stage", "target_table")

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_get_data_from_stage="",
                 target_table="",
                 insert_mode="append",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_get_data_from_stage = query_get_data_from_stage
        self.target_table = target_table
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        sql = f"""
            INSERT INTO {self.target_table}
            {self.query_get_data_from_stage}
        """
        self.log.info(f"Executing insert statement")
        redshift_hook.run(sql)

        self.log.info(f"Task Completed")

