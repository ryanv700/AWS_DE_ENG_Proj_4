from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

#References: The code throughout this proejct uses Udacity Course Material and UdacityGPT as references

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 region='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info('StageToRedshiftOperator execution started.')

        # Get AWS and Redshift connections
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data from the table
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Copy data from S3 to Redshift
        self.log.info(f"Copying data from S3 to Redshift table {self.table}")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            FORMAT AS JSON '{self.json_path}'
            REGION '{self.region}'
        """
        redshift.run(copy_sql)

        self.log.info('StageToRedshiftOperator execution completed.')





