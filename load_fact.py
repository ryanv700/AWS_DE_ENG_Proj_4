from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#References: The code throughout this proejct uses Udacity Course Material and UdacityGPT as references

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 mode='append',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data into {self.table} fact table")

        if self.mode == 'append':
            insert_sql = f"INSERT INTO {self.table} {self.sql}"
        elif self.mode == 'delete-load':
            truncate_sql = f"TRUNCATE TABLE {self.table}"
            redshift.run(truncate_sql)
            insert_sql = f"INSERT INTO {self.table} {self.sql}"
        else:
            raise ValueError("Invalid mode. Choose 'append' or 'delete-load'.")


        redshift.run(insert_sql)
        self.log.info(f"Data successfully loaded into {self.table} fact table")

