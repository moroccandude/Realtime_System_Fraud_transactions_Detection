from datetime import datetime
import json
import requests
from pyhive import hive
import logging
from dotenv import load_dotenv
import os
import subprocess
from typing import Union

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s : %(name)s : %(levelname)s: %(asctime)s')
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
logger.addHandler(handler)

def fetching_data(item:str):
    """Fetch data from external API"""
    try:
        response = requests.get(f"http://localhost:8000/api/v1/{item}")
        response.raise_for_status()  # Ensure we catch HTTP errors
        data = response.json()
        logger.info("Fetching data from API successful")
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None

def load_toHdfs(path=None):
    """Load data to HDFS"""
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    try:
    
      subprocess.run(["docker","exec","-i","namenode","hdfs","dfs","-mkdir","-p","opt/hive/warehouse/  || true"])
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
    
def connect_to_hive(localhost,port,username,database,password,auth):
    """Connect to Hive"""
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    try:

        conn = hive.Connection(host=localhost, port=port, username=username, database=database,password=password,auth=auth)
        cursor = conn.cursor()
        logger.info("Connected to Hive")
        return cursor, conn
    except Exception as e:
        logger.error(f"Error connecting to Hive: {e}")
        return None, None
    
def process_and_save_to_hive(cursor,conn,data):
    """Process data and save it to Hive"""
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    try:
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fraud_transactionsssW (
                amount STRING
            )
            STORED AS PARQUET
        """)
        logger.info("Table created or already exists")

        # Fetch data from API
        logger.info("Processing data")
        # if data:
        #     for record in data:
        cursor.execute(
                    "INSERT INTO fraud_transactionsssW (amount) VALUES ('ggggg')",
                    
      )
        conn.commit()
        logger.info("Data inserted successfully")

    except Exception as e:
        logger.error(f"Error processing data: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    load_dotenv()
    data=fetching_data("customer")
    load_toHdfs()
    os.chdir("../System fraud transactions detection ")
    cursor,conn=connect_to_hive(os.getenv("HIVE_HOST"),os.getenv("HIVE_PORT"),os.getenv("HIVE_USERNAME"),os.getenv("HIVE_DATABASE"),os.getenv("HIVE_PASSWORD"),os.getenv("HIVE_AUTH"))
    process_and_save_to_hive(cursor,conn,data)

if __name__ == "__main__":
    main()