import os
from dotenv import load_dotenv

load_dotenv('.env')
"""
    #WSL:
    #pip3 install python-dotenv
    Loads environment variables
    Set the environment variables in the .env file
    For example:
    SPARK_MASTER=local <--- Notice that there is no quotation marks

    Make sure .env file exists, and there are no spaces.
"""


SPARK_APP_NAME = 'eCommerce Data Cleaner and Analyzer'
HDFS_MASTER = os.environ.get("HDFS_MASTER")
HDFS_USER = os.environ.get("HDFS_USER")
HDFS_FILENAME = os.environ.get("HDFS_FILENAME")
HDFS_DATA_DIR = f"{HDFS_MASTER}/user/{HDFS_USER}/{HDFS_FILENAME}"