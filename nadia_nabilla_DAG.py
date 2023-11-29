'''
======================================================================================================================
Milestone 3

Nama        : Nadia Nabilla Shafira
Batch       : HCK-009
Objectives  : Program ini dibuat untuk melakukan otomatisasi load data Consumer Behavior and Shopping Habits dari PostgreSQL,
kemudian data cleaning dan transform ke ElasticSearch.
======================================================================================================================
'''

# Import Libraries
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator



# Task 1: Fetch from PostgreSQL
def fetch():
    '''
    Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan data cleaning

    Parameters
    dbname: string - nama database di mana data disimpan
    host: string - nama host
    user: string - nama user
    password: string - password user
    port: int - nama port

    Return
    df: dataframe - dataframe yang diambil dari PostgreSQL
    P2M3_nadia_nabilla_data_raw.csv: csv - file csv dari dataframe

    Contoh Penggunaan
    df = fetch()
    '''
    conn_string = "dbname='p2m3' host='postgres' user='postgres' password='postgres' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('P2M3_nadia_nabilla_data_raw.csv', index=False)




# Task 2: Data Cleaning
def clean(df_path):
    '''
    Fungsi ini ditujukan untuk melakukan data cleaning terhadap data raw yang sebelumnya diambil dari PostgreSQL

    Parameter
    df: dataframe - nama dataframe yang akan dilakukan data cleaning

    Return
    df: dataframe - dataframe yang telah dilakukan data cleaning
    P2M3_nadia_nabilla_data_clean.csv: csv - file csv dari export dataframe 'df' yang telah dilakukan data cleaning

    Contoh Penggunaan
    df = clean(r'path/to/P2M3_nadia_nabilla_data_raw.csv')
    '''
    df = pd.read_csv(df_path)                                   #load data csv

    # Normalisasi Kolom
    df.columns = [col.replace(' ', '_') for col in df.columns]  #mengubah spasi menjadi underscore pada nama kolom
    df.columns = df.columns.str.lower()                         #mengubah penulisan nama kolom menjadi lowercase

    # Menghapus Data Duplicate
    df = df.drop_duplicates()

    # Menghapus Missing Values
    df = df.dropna().reset_index(drop=True)

    # Define Data Type
    data_types= {
        'customer_id':'int64',
        'age':'int64',
        'gender':'string',
        'item_purchased':'string',
        'category':'string',
        'purchase_amount_(usd)':'float64',
        'location':'string',
        'size':'string',
        'color':'string',
        'season':'string',
        'review_rating':'float64',
        'subscription_status':'string',
        'shipping_type':'string',
        'discount_applied':'string',
        'promo_code_used':'string',
        'previous_purchases':'int64',
        'payment_method':'string',
        'frequency_of_purchases':'string'
    }
    df = df.astype(data_types)

    # Menyimpan Cleaned Data
    df.to_csv('P2M3_nadia_nabilla_data_clean.csv', index=False)





# Task 3: Post to Elasticsearch
def postES():
    '''
    Fungsi ini ditujukan untuk memuat file csv yang sudah dilakukan data cleaning dan memasukannya ke Elasticsearch
    
    Parameter
    Elasticsearch(): module
    P2M3_nadia_nabilla_data_clean.csv: csv - file csv dari export dataframe 'df' yang telah dilakukan data cleaning
    
    Return
    doc
    res

    Contoh Penggunaan
    post = postES()
    '''
    es = Elasticsearch(hosts='http://elasticsearch:9200') 
    df=pd.read_csv('P2M3_nadia_nabilla_data_clean.csv')
    actions = [
        {
            "_index": "p2m3_nadia_nabilla_data_clean",
            "_source": row.to_dict()
        }
        for i, row in df.iterrows()
    ]
    bulk(es, actions)



# Define Default Arguments
default_args = {
    'owner': 'nadia',
    'start_date': datetime(2023, 11, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}


cleanData_args = {
    'df_path': 'P2M3_nadia_nabilla_data_raw.csv'
}


with DAG('fetch_clean_post',
         default_args=default_args,
         schedule_interval="30 6 * * *",  # setiap pukul 06:30 WIB
         ) as dag:

    fetchData = PythonOperator(task_id='fetch_data',
                               python_callable=fetch)
    
    cleanData = PythonOperator(task_id='clean_data',
                               python_callable=clean,
                               op_kwargs=cleanData_args)
    
    postToES = PythonOperator(task_id='post_to_es',
                              python_callable=postES, dag=dag)


# Define Task Order
fetchData >> cleanData >> postToES
