import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import ()
from sql import SqlQueries

from operators import (LocalToS3, S3ToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket="udacity-dend"
s3_key=""
schema="public"
song_data="song_data"
redshift_conn_id="redshift-cluster-1"
aws_credentials_id="aws_credentials"
copy_options=["FORMAT AS PARQUET"]

default_args= {
    'owner': 'davidson',
    'start_date': datetime(2020, 7, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email_on_retry': False
}

dag = DAG('capstone_dag'
         , defaults_args=defaults_args
         , description='Transfer data from S3 to Redshift with Airflow'
         , schedule_interval='0 * * * *'
         , catchup=False)

# Starting Operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Copying data from local to S3
brazil_to_s3 = LocalToS3( spark=spark
                        , etl_process=
                        , input_data=input_data
                        , output_data=output_data
                        , input_format="csv"
                        , **kwargs
                        )

enem_to_s3 = LocalToS3( spark=spark
                      , etl_process=
                      , input_data=input_data
                      , output_data=output_data
                      , input_format="csv"
                      , **kwargs
                      )

# Copying data from S3 to Redshift
brazil_cities_to_redshift = S3ToRedshiftOperator(table="brazil_cities_stage"
                                                , s3_key="brazil_cities.parquet"
                                                , schema=schema
                                                , s3_bucket=s3_bucket
                                                , redshift_conn_id=redshift_conn_id
                                                , aws_credentials_id=aws_credentials_id
                                                , verify=None
                                                , copy_options=copy_options
                                                , dag=dag
                                                , task_id='Brazil_Cities_Stage_Table'
                                                )

enem_to_redshift = S3ToRedshiftOperator(table="enem_stage"
                                       , s3_key="enem.parquet"
                                       , schema=schema
                                       , s3_bucket=s3_bucket
                                       , redshift_conn_id=redshift_conn_id
                                       , aws_credentials_id=aws_credentials_id
                                       , verify=None
                                       , copy_options=copy_options
                                       , dag=dag
                                       , task_id='Enem_Stage_Table'
                                       )

# Load from stage to fact
candidate_fact_table = LoadFactOperator(table='candidate_fact'
                                       , sql_query=SqlQueries.candidate_fact_insert
                                       , redshift_conn_id=redshift_conn_id
                                       , dag=dag
                                       , task_id='Load_Candidate_Fact_Table'
                                       )

city_fact_table = LoadFactOperator(table='city_fact'
                                  , sql_query=SqlQueries.city_fact_insert
                                  , redshift_conn_id=redshift_conn_id
                                  , dag=dag
                                  , task_id='Load_City_Fact_Table'
                                  )

# Load from stage to dimension
candidate_dim_table = LoadDimensionOperator(table='candidate_dim'
                                           , sql_query=SqlQueries.candidate_dim_insert
                                           , redshift_conn_id=redshift_conn_id
                                           , dag=dag
                                           , task_id='Load_Candidate_Dimension_Table'
                                           )
student_dim_table = LoadDimensionOperator(table='student_dim'
                                         , sql_query=SqlQueries.student_dim_insert
                                         , redshift_conn_id=redshift_conn_id
                                         , dag=dag
                                         , task_id='Load_Student_Dimension_Table'
                                         )
special_dim_table = LoadDimensionOperator(table='special_dim'
                                         , sql_query=SqlQueries.special_dim_insert
                                         , redshift_conn_id=redshift_conn_id
                                         , dag=dag
                                         , task_id='Load_Special_Dimension_Table'
                                         )
city_dim_table = LoadDimensionOperator(table='city_dim'
                                      , sql_query=SqlQueries.city_dim_insert
                                      , redshift_conn_id=redshift_conn_id
                                      , dag=dag
                                      , task_id='Load_City_Dimension_Table'
                                      )

# Check Data Quality
run_quality_checks = DataQualityOperator(tables=['candidate_fact'
                                                , 'city_fact'
                                                , 'candidate_dim'
                                                , 'student_dim'
                                                , 'special_dim'
                                                , 'city_dim']
                                        , redshift_conn_id=redshift_conn_id
                                        , dag=dag
                                        , task_id='Checking_Data_Quality'
                                        )
# Ending Operator
end_operator = DummyOperator(task_id='Stop_Execution')

#################################

# DAG: Start loading data into S3
start_operator >> brazil_to_s3
start_operator >> enem_to_s3

# DAG: Load data from S3 into Redshift stage tables

brazil_to_s3 >> brazil_cities_to_redshift
enem_to_s3 >> enem_to_redshift

# DAG: Load dimension tables with data in fact and stage tables
brazil_cities_to_redshift >> candidate_dim_table
brazil_cities_to_redshift >> student_dim_table
brazil_cities_to_redshift >> special_dim_table
brazil_cities_to_redshift >> city_dim_table

enem_to_redshift >> candidate_dim_table
enem_to_redshift >> student_dim_table
enem_to_redshift >> special_dim_table
enem_to_redshift >> city_dim_table

# DAG: Load fact tables with data in stage tables
candidate_dim_table >> candidate_fact_table
student_dim_table >> candidate_fact_table
special_dim_table >> candidate_fact_table
city_dim_table >> candidate_fact_table

candidate_dim_table >> city_fact_table
student_dim_table >> city_fact_table
special_dim_table >> city_fact_table
city_dim_table >> city_fact_table

# DAG: Check quality
candidate_dim_table >> run_quality_checks
student_dim_table >> run_quality_checks
special_dim_table >> run_quality_checks
city_dim_table >> run_quality_checks

# DAG: End operation
run_quality_checks >> end_operator