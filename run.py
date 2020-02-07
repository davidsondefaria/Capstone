# Do all imports and installs here
import re
import os
import psycopg2
# import requests
import pandas as pd
from etl import process_cities_data, process_enem_data, create_spark_session

from datetime import datetime, timedelta

brazil_cities_path = os.getcwd() + '/dataBrazil/brazil_cities.csv'
brazil_cities_dictionary_path = os.getcwd() + '/dataBrazil/brazil_cities_dictionary.csv'
enem_2018_path = os.getcwd() + '/dataBrazil/enem/enem_2018.csv'
enem_2018_dictionary_path = os.getcwd() + '/dataBrazil/enem/enem_2018_dictionary.xlsx'

def getDataset():
    enem2018_df = pd.read_csv(enem_2018_path, delimiter=";")
    enem2018_df.fillna(0, inplace=True)
    print(f"Enem: {enem2018_df.shape}")
    print(enem2018_df.head())
    
    brazil_df = pd.read_csv(brazil_cities_path, delimiter=";")
    brazil_df.fillna(0, inplace=True)
    print(f"Cidades: {brazil_df.shape}")
    print(brazil_df.head())
    
    return (brazil_df, enem_df)

def main():
    outputS3 = ''
    spark = create_spark_session()
    process_cities_data(spark, brazil_cities_path, output_data=outputS3)
    process_enem_data(spark, enem_2018_path, output_data=outputS3)
    
    
if __name__ == "__main__":
    main()