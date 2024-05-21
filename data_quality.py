from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#References: The code throughout this proejct uses Udacity Course Material and UdacityGPT as references

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tests=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Running data quality checks")

        for test in self.tests:
            sql = test['sql']
            expected_result = test['expected_result']
            self.log.info(f"The SQL query being tested is: {sql}")
            records = redshift.run(sql)
            self.log.info(f"The count of records is: {len(records)}")
            self.log.info(f"The comparison for expected results is: {records[0][0]}")
            if len(records) < 1 or records[0][0] != expected_result:
                raise ValueError(f"Data quality check failed. SQL: {sql}, Expected Result: {expected_result}")

        self.log.info("All data quality checks passed")
