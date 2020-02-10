import os
import re
from pyspark.sql import SparkSession

def create_spark_session():
    """
        Create a Spark session and set the necessary settings.
        
        Parameters:
            None
        
        Returns:
            Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def read_file( spark
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

def read_dataframe( spark
                   , dataframe
                   , **options
                  ):
    df = spark.createDataFrame(dataframe, **options)
    return df


def etl_process( spark
                , dataframe
                , dataset_name
                , input_path=''
                , output_path=''
                , input_format='csv'
                , columns='*'
                , load_size=None
                , partition_by=None
                , header=True
                , **options
               ):
    dataset = read_dataframe(spark, dataframe)
    
    dataset.select(columns)\
           .write \
           .save(output_path
                 , mode=mode
                 , format='parquet'
                 , partition=partition_by
                 **options
                )
    return dataset

def process_cities_data(spark, brazil_cities_path, output_data, input_format='csv', **options):
    if input_format == 'csv':
        brazil_df = pd.read_csv(brazil_cities_path, **options)
    elif input_format == 'json':
        brazil_df = pd.read_json(brazil_cities_path, **options)
    
    brazil_df.fillna(0, inplace=True)
    
    brazil_df.drop_duplicates(['CITY', 'STATE'], keep='first')
    
    brazil_df = brazil_df.filter([ 'CITY', 'STATE', 'CAPITAL', 'IDHM Ranking 2010', 'IDHM'
                                 , 'IDHM_Renda', 'IDHM_Longevidade', 'IDHM_Educacao', 'LONG'
                                 , 'LAT', 'ALT'
                                 ])
    
    brazil_df = brazil_df.rename(columns={'CITY': 'city'
                                          , 'STATE': 'state'
                                          , 'CAPITAL': 'capital'
                                          , 'IDHM Ranking 2010': 'hdi_ranking'
                                          , 'IDHM': 'hdi'
                                          , 'IDHM_Renda': 'hdi_gni'
                                          , 'IDHM_Longevidade': 'hdi_life'
                                          , 'IDHM_Educacao': 'hdi_education'
                                          , 'LONG': 'longitude'
                                          , 'LAT': 'latitude'
                                          , 'ALT': 'altitude'
                                         })
#     print(brazil_df.shape)
#     print(brazil_df.head())
    
    brazil = read_dataframe(spark, brazil_df, **options)
    brazil.printSchema()
    brazil.show(5)
    
    brazil.write \
          .mode('overwrite') \
          .partitionBy("city", "state") \
          .parquet(output_data + "brazil_cities")

    

def process_enem_data(spark, enem_path, output_data, input_format='csv', **options):
    if input_format == 'csv':
        enem2018_df = pd.read_csv(enem_path, **options)
    elif input_format == 'json':
        enem2018_df = pd.read_json(enem_path, **options)
        
    enem2018_df.fillna(0, inplace=True)
    enem2018_df.drop_duplicates('NU_INSCRICAO')
    enem2018_df = enem2018_df.rename(columns={'NU_INSCRICAO': 'registration'
                                          , 'CO_MUNICIPIO_RESIDENCIA': 'city_residence_code'
                                          , 'NO_MUNICIPIO_RESIDENCIA': 'city_residence'
                                          , 'CO_UF_RESIDENCIA': 'state_residence_code'
                                          , 'SG_UF_RESIDENCIA': 'state_residence'
                                          , 'NU_IDADE': 'age'
                                          , 'TP_SEXO': 'gender'
                                          , 'TP_ESTADO_CIVIL': 'matiral_status'
                                          , 'TP_COR_RACA': 'color_race'
                                          , 'TP_NACIONALIDADE': 'nationality'
                                          , 'TP_ST_CONCLUSAO': 'high_school_status'
                                          , 'TP_ANO_CONCLUIU': 'high_school_year_conclusion'
                                          , 'TP_ESCOLA': 'school_type'
                                          , 'IN_BAIXA_VISAO': 'def_low_vision'
                                          , 'IN_CEGUEIRA': 'def_blind'
                                          , 'IN_SURDEZ': 'def_deaf'
                                          , 'IN_DEFICIENCIA_AUDITIVA': 'def_low_hearing'
                                          , 'IN_SURDO_CEGUEIRA': 'def_blind_deaf'
                                          , 'IN_DEFICIENCIA_FISICA': 'def_physical'
                                          , 'IN_DEFICIENCIA_MENTAL': 'def_mental'
                                          , 'IN_DEFICIT_ATENCAO': 'def_attention'
                                          , 'IN_DISLEXIA': 'def_dyslexia'
                                          , 'IN_DISCALCULIA': 'def_dyscalculia'
                                          , 'IN_AUTISMO': 'def_autism'
                                          , 'IN_VISAO_MONOCULAR': 'def_monocular_vision'
                                          , 'IN_OUTRA_DEF': 'def_other'
                                          , 'IN_NOME_SOCIAL': 'social_name'
                                          , 'CO_MUNICIPIO_PROVA': 'city_test_code'
                                          , 'NO_MUNICIPIO_PROVA': 'city_test'
                                          , 'CO_UF_PROVA': 'state_test_code'
                                          , 'SG_UF_PROVA': 'state_test'
                                          , 'TP_PRESENCA_CN': 'presence_natural_science'
                                          , 'TP_PRESENCA_CH': 'presence_human_science'
                                          , 'TP_PRESENCA_LC': 'presence_languages'
                                          , 'TP_PRESENCA_MT': 'presence_math'
                                          , 'NU_NOTA_CN': 'grade_natural_science'
                                          , 'NU_NOTA_CH': 'grade_human_science'
                                          , 'NU_NOTA_LC': 'grade_languages'
                                          , 'NU_NOTA_MT': 'grade_math'
                                          , 'TP_STATUS_REDACAO': 'essay_status'
                                          , 'NU_NOTA_REDACAO': 'grade_essay'
                                         })
#     print(enem2018_df.shape)
#     print(enem2018_df.head())

    enem = read_dataframe(spark, enem2018_df, **options)
    enem.printSchema()
    enem.show(5)
    
    enem.write \
          .mode('overwrite') \
          .partitionBy("city_residence", "state_residence") \
          .parquet(output_data + "enem2018")
