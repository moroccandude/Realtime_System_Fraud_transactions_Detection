from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from pyhive import hive
import json
import requests
import logging
import os
import subprocess
from typing import Dict, Any
from dotenv import load_dotenv
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email':['ismailsamilacc@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    


}

# Helper function to run HDFS commands with proper error handling
def run_hdfs_command(command, error_message):
    """Run HDFS command with proper error handling"""
    logger = logging.getLogger(__name__)
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300  # 5-minute timeout
        )
        
        if result.returncode != 0:
            logger.error(f"{error_message}: {result.stderr}")
            raise Exception(f"Command failed: {' '.join(command)}\nError: {result.stderr}")
        
        return result.stdout
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out after 5 minutes: {' '.join(command)}")
        raise
    except Exception as e:
        logger.error(f"Error running command {' '.join(command)}: {str(e)}")
        raise

def fetch_data(item: str, **context) -> Dict[str, Any]:
    """
    Fetch data from external API
    
    Args:
        item: API endpoint to fetch data from
        
    Returns:
        Dictionary containing the fetched data
    """
    logger = logging.getLogger(__name__)
    try:
        api_base_url = Variable.get("api_base_url", default_var="http://localhost:8000/api/v1")
        response = requests.get(f"{api_base_url}/{item}")
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully fetched data from API endpoint: {item}")
        
        # Push data to XCom for next task
        context['ti'].xcom_push(key='api_data', value=data)
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        raise
  
def load_to_hdfs(**context):
    """Load data to HDFS using subprocess commands"""
     
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    try:
          os.chdir("/home/usmail/sys_fraud/sys_detection_frauds/system_fraud_transactions_detection/airflow_home/dags")
        # Get data directory paths from Variables
          local_data_path = Variable.get("local_data_path", default_var="./../../data")
          hdfs_target_dir = Variable.get("hdfs_warehouse_dir", default_var="/opt2/hive2/warehouse2")
          subprocess.run(["docker","exec","-i","namenode","hdfs","dfs","-mkdir","-p",f"{hdfs_target_dir} || true"])
          logger.info("creating folder in HDFS")
         # Copy data file to Mount volume  
          subprocess.run(["cp","-r","./data","./hadoop/hadoop_namenode"])
          logger.info("Copying data to data into /hadoop_namenode mounted volume")
        # Store data in HDFS
          subprocess.run(["docker","exec","-i","namenode","hdfs","dfs","-put","./hadoop/dfs/name/data","/user/root/opt/hive/warehouse  || true"])   
          logger.info("Data loaded to HDFS")
    except Exception as e:
        logger.error(f"Error loading data to HDFS: {e}")
        return None    



def connect_to_hive(**context):
    """Connect to Hive and share connection parameters"""
    logger = logging.getLogger(__name__)
    try:
        load_dotenv()
        # Get connection parameters from Variables or environment
        host = Variable.get("hive_host", default_var=os.getenv("HIVE_HOST"))
        port = Variable.get("hive_port", default_var=os.getenv("HIVE_PORT"))    
        username = Variable.get("hive_username", default_var=os.getenv("HIVE_USERNAME"))
        database = Variable.get("hive_database", default_var=os.getenv("HIVE_DATABASE"))
        password = Variable.get("hive_password", default_var=os.getenv("HIVE_PASSWORD"))
        auth = Variable.get("hive_auth", default_var=os.getenv("HIVE_AUTH"))
        
        # Store connection parameters in XCom (not the actual connection)
        connection_params = {
            "host": host,
            "port": port,
            "username": username,
            "database": database,
            "password": password,
            "auth": auth
        }
        print(f"Connection params: {connection_params}")
        # Test connection to ensure it works
        conn = hive.Connection(
            host=host, 
            port=port, 
            username=username, 
            database=database,
            password=password,
            auth=auth
        )
        cursor = conn.cursor()
        cursor.close()
        conn.close()
        
        # Share parameters, not the connection
        context['ti'].xcom_push(key='hive_connection_params', value=connection_params)
        logger.info("Hive connection parameters verified and shared")
        return connection_params
    except Exception as e:
        logger.error(f"Error verifying Hive connection: {e}")
        raise

def create_tables(**context):
    """Create Hive tables using connection parameters from previous task"""
    logger = logging.getLogger(__name__)
    try:
        # Get connection parameters from XCom
        ti = context['ti']
        conn_params = ti.xcom_pull(task_ids='connect_to_hive', key='hive_connection_params')
        
        # Create a fresh connection using parameters
        conn = hive.Connection(
            host=conn_params['host'],
            port=conn_params['port'],
            username=conn_params['username'],
            database=conn_params['database'],
            password=conn_params['password'],
            auth=conn_params['auth']
        )
        cursor = conn.cursor()
        
        # Create table with correct data types
        create_table_customers = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id STRING, 
            account_history ARRAY<STRING>, 
            age INT,
            location STRING,
            avg_transaction_value STRING
        )
        LOCATION 'hdfs://namenode:9001/hive_warehouse'
        """
        create_table_transactions = """
        CREATE TABLE  IF NOT EXISTS  transactions (
        transaction_id STRING,
        date_time TIMESTAMP,
        amount DOUBLE,
        currency STRING,
        merchant_details STRING,
        customer_id STRING,
        transaction_type STRING,
        location STRING
        )
         LOCATION 'hdfs://namenode:9001/hive_warehouse'
        
        """
        create_table_external_data = """
        CREATE TABLE  IF NOT EXISTS  external_data (
        blacklist_info ARRAY<STRING>,
        credit_scores MAP<STRING, INT>,
        fraud_reports MAP<STRING, INT>


        )
         LOCATION 'hdfs://namenode:9001/hive_warehouse'
        """
        cursor.execute(create_table_customers)
        cursor.execute(create_table_transactions)
        cursor.execute(create_table_external_data)

        conn.commit()
        hchgscgs
        # Always close connections
        cursor.close()
        conn.close()
        
        logger.info("Hive tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating Hive tables: {e}")
        raise

# def load_data_to_hive(**context):
#     """Load data from HDFS to Hive"""
#     logger = logging.getLogger(__name__)
    
#     try:
#         # Get connection parameters from XCom
#         ti = context['ti']
#         conn_params = ti.xcom_pull(task_ids='connect_to_hive', key='hive_connection_params')
        
#         # Create a fresh connection
#         conn = hive.Connection(
#             host=conn_params['host'],
#             port=conn_params['port'],
#             username=conn_params['username'],
#             database=conn_params['database'],
#             password=conn_params['password'],
#             auth=conn_params['auth']
#         )
#         cursor = conn.cursor()
        
#         # Get data from API endpoints
#         import requests
        
#         # Fetch customer data
#         response = requests.get("http://localhost:8000/api/v1/customer")
#         response.raise_for_status()
#         customers_data = response.json()
        
#         # Fetch transaction data
#         response = requests.get("http://localhost:8000/api/v1/transactions")
#         response.raise_for_status()
#         transactions_data = response.json()
        
#         # Fetch external data
#         response = requests.get("http://localhost:8000/api/v1/externaldata")
#         response.raise_for_status()
#         external_data = response.json()

#         # Insert customer data
#         for customer in customers_data:
#             # Convert account_history list to Hive array string format
#             account_history_str = "array(" + ",".join([f"'{item}'" for item in customer.get('account_history', [])]) + ")"
            
#             insert_customer_sql = f"""
#             INSERT INTO customers 
#             VALUES (
#                 '{customer.get('customer_id', '')}',
#                 {account_history_str},
#                 {customer.get('age', 0)},
#                 '{customer.get('location', '')}',
#                 '{customer.get('avg_transaction_value', '')}'
#             )
#             """
#             cursor.execute(insert_customer_sql)
        
#         # Insert transaction data
#         for transaction in transactions_data:
#             # Format timestamp properly
#             date_time = f"'{transaction.get('date_time', '')}'"
            
#             insert_transaction_sql = f"""
#             INSERT INTO transactions 
#             VALUES (
#                 '{transaction.get('transaction_id', '')}',
#                 {date_time},
#                 {transaction.get('amount', 0.0)},
#                 '{transaction.get('currency', '')}',
#                 '{transaction.get('merchant_details', '')}',
#                 '{transaction.get('customer_id', '')}',
#                 '{transaction.get('transaction_type', '')}',
#                 '{transaction.get('location', '')}'
#             )
#             """
#             cursor.execute(insert_transaction_sql)
        
#         # Insert external data
#         for external in external_data:
#             # Convert blacklist_info list to Hive array string format
#             blacklist_str = "array(" + ",".join([f"'{item}'" for item in external.get('blacklist_info', [])]) + ")"
            
#             # Convert credit_scores map to Hive map string format
#             credit_scores = external.get('credit_scores', {})
#             credit_scores_str = "map(" + ",".join([f"'{k}',{v}" for k, v in credit_scores.items()]) + ")"
            
#             # Convert fraud_reports map to Hive map string format
#             fraud_reports = external.get('fraud_reports', {})
#             fraud_reports_str = "map(" + ",".join([f"'{k}',{v}" for k, v in fraud_reports.items()]) + ")"
            
#             insert_external_sql = f"""
#             INSERT INTO external_data 
#             VALUES (
#                 {blacklist_str},
#                 {credit_scores_str},
#                 {fraud_reports_str}
#             )
#             """
#             cursor.execute(insert_external_sql)
        
#         # Commit the transaction
#         conn.commit()
        
#         # Close connections
#         cursor.close()
#         conn.close()
        
#         logger.info("Data loaded to Hive successfully")
#         return True
#     except Exception as e:
#         logger.error(f"Error loading data to Hive: {e}")
#         raise
        
      
#         # load_data_sql = f"""
#         # LOAD DATA INPATH '{hdfs_file_path}'
#         # OVERWRITE INTO TABLE customers
#         # """
#         # cursor.execute(load_data_sql)
#         conn.commit()
        
#         # Close connections
#         cursor.close()
#         conn.close()
        
#         logger.info("Data loaded to Hive successfully")
#         return True
#     except Exception as e:
#         logger.error(f"Error loading data to Hive: {e}")
#         raise

# Create the DAG
with DAG(
    'fraud_detection_pipeline',
    default_args=default_args,
    description='A DAG to detect fraudulent transactions',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['fraud', 'detection', 'hive', 'hdfs'],
    max_active_runs=1,
    doc_md="""
    # Fraud Detection Pipeline
    
    This DAG processes transaction data to detect potential fraud:
    
    1. Fetches data from an API
    2. Loads data to HDFS using subprocess commands
    3. Connects to Hive and creates necessary tables
    4. Loads data from HDFS to Hive for analysis
    
    ## Dependencies
    - Docker container with HDFS namenode
    - Hive connection
    - API access
    """
) as dag:
    
    # Task 1: Fetch data from API
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        op_kwargs={'item': 'customer'},
        provide_context=True,
        dag=dag,
    )
    
    # Task 2: Load data to HDFS using subprocess
    load_to_hdfs_task = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=load_to_hdfs,
        provide_context=True,
        dag=dag,
    )
    
    # Task 3: Connect to Hive and verify connection
    connect_to_hive_task = PythonOperator(
        task_id='connect_to_hive',
        python_callable=connect_to_hive,
        provide_context=True,
        dag=dag,
    )
   
    # Task 4: Create Hive tables
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
        provide_context=True,
        dag=dag,
    )
    
    # # Task 5: Load data from HDFS to Hive
    # load_data_to_hive_task = PythonOperator(
    #     task_id='load_data_to_hive',
    #     python_callable=load_data_to_hive,
    #     provide_context=True,
    #     dag=dag,
    # )
    
    # Set task dependencies
    fetch_data_task >> load_to_hdfs_task >> connect_to_hive_task >> create_tables_task 
    # >> load_data_to_hive_task