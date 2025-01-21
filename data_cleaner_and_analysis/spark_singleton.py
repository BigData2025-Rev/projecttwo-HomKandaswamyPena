from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from config import SPARK_APP_NAME

class SparkSingleton:
    _instance = None

    @staticmethod
    def getInstance():
        if SparkSingleton._instance is None:
            SparkSingleton._instance = SparkSession.builder \
                .appName(SPARK_APP_NAME) \
                .getOrCreate()
        return SparkSingleton._instance
