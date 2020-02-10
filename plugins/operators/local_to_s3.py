from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LocalToS3(BaseOperator):
    ui_color='#231341'
    template_fields = ("s3_keys",)
    
    @apply_defaults
    def __init__( self
                , schema=""
                , table=""
                , s3_bucket=""
                , etl_process=None
                , spark=None
                , input_path=""
                , output_path=""
                , input_format=""
                , *args, **kwargs
                ):
        super(LocalToS3, self).__init__(*args, **kwargs)
        self.schema=schema
        self.table=table
        self.s3_bucket=s3_bucket
        self.etl_process=etl_process
        self.spark=spark
        self.dataframe=dataframe
        self.output_path=output_path
        self.input_format=input_format
    
    def execute(self, context):
        etl_process(spark, input_path, output_data, input_format, **kwargs)