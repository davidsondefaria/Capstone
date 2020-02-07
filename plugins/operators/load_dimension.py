from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        TRUNCATE {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    
    @apply_defaults
    def __init__( self
                , redshift_conn_id=""
                , table=""
                , sql_query=""
                , *args, **kwargs
                ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        
        # Reading aws credentials to connect to redshift database
#         aws_hook = AwsHook(self.aws_credentials_id)
#         credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Inserting data into dimensional table
        self.log.info(f'LoadDimensionOperator inserting data into dimension table: {self.table}')
        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.table, self.sql_query)
        
        redshift.run(formatted_sql)