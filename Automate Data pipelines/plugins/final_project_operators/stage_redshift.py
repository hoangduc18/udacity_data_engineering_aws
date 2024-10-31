from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("table", "json_format")
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_prefix="",
                 json_format="auto",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_credentials_id = aws_credentials_id 
        self.json_format = json_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        s3_path = f"s3://{self.s3_bucket}/{self.s3_prefix}"
        self.log.info(f"Loading data from {s3_path} to Redshift table {self.table}")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Checking if data exist in {self.table}")
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        num_records = records[0][0]
        if num_records < 1: 
            copy_sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                FORMAT AS JSON '{self.json_format}'
                REGION 'us-west-2';
            """

            self.log.info(f"Executing COPY command: {copy_sql}")
            redshift_hook.run(copy_sql)

        self.log.info(f"Task Completed")

