import os
import re
from pyspark.sql import SparkSession

def read_data( spark
              , input_path
              , input_format = "csv"
              , columns = '*'
              , debug_size = None
              , **options
             ):
    if debug_size is None:
        df = spark.read.load(input_path
                             , format=input_format
                             , **options
                            ).select(columns)
    else:
        df = spark.read.load(input_path
                             , format=input_format
                             , **options
                            ).select(columns) \
                            .limit(debug_size)
    return df


def etl_process( spark
                , dataset_name
                , input_path
                , output_path
                , input_format='csv'
                , columns='*'
                , load_size=None
                , partition_by=None
                , header=True
                , **options
               ):
    dataset = read_data(  spark
                        , input_path
                        , input_format = "csv"
                        , columns = '*'
                        , debug_size = None
                        , **options
                       )
    dataset.select(columns)\
           .write \
           .save(output_path
                 , mode=mode
                 , format='parquet'
                 , partition=partition_by
                 **options
                )
    return dataset