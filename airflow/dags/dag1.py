from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json
import requests
from pyhive import hive
import logging
logging.basicConfig(level=logging.INFO,format='%%((message)s%% : %%(name)s%%')
# Function to read JSON and save to Hive
# def process_and_save_to_hive():
#     logging.getLoggerClass(__name__)
#     hanler=logging.StreamHandler()
#     logging.Handler(hanler)
#     # Read external_data.json
#     response = requests.get('http://localhost:8000/api/v1/externaldata')
#     data = response.json()
#     logging.info("fetching data from Api ")  

#     try :
#     # Connect to Hive (Docker container)
#         conn = hive.Connection(host='hive-server', port=10000, username='hive', database='default')
#         cursor = conn.cursor()
#         logging.info("Connected to Hive")
#         # Create table if not exists
#         cursor.execute("""
#             CREATE TABprocess_external_data_to_hiveLE IF NOT EXISTS fraud_transactions (
#                 id STRING,
#                 amount FLOAT,
#                 timestamp STRING
#             )
#             STORED AS PARQUET
#         """)
#         logging.info("Table created")
#     except Exception as e:
#          logging.error(e)    
    
# Insert data into Hive table
#     for record in data:
#         cursor.execute("""
#             INSERT INTO fraud_transactions (id, amount, timestamp)
#             VALUES (%s, %s, %s)
#         """, (record['id'], record['amount'], record['timestamp']))
    
#     conn.close()

def print():
    print("Hello World")
# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'process_external_data_to_hive',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

  
    print_sucess = PythonOperator(
        task_id='process_and_save_to_hive',
        python_callable=print,
    )

    print_sucess



# def main():
#     process_and_save_to_hive()

# if __name__=="__main__":
#     # main() 
#     conn = hive.Connection(host='hive-server', port=10000, username='hive', database='default')
#     cursor = conn.cursor()
#     logging.info("Connected to Hive")
        
