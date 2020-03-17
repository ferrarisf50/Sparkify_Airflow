from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",     
                 ignore_headers=1,
                 *args, **kwargs):
        
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        
        self.log.info(f"Copying {self.table} from S3 to Redshift")
        
        copy_query = """
                    COPY {table}
                    FROM '{s3_path}'
                    ACCESS_KEY_ID '{access_key}'
                    SECRET_ACCESS_KEY '{secret_key}'
                    FORMAT AS JSON
                    REGION 'us-west-2';
                """.format(table=self.table,
                           s3_path=self.s3_path,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key)
        
        redshift_hook.run(copy_query)
        self.log.info(f"Copy {self.table} complete.")


       
        