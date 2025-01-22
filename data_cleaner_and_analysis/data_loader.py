from config import HDFS_DATA_DIR
from spark_singleton import SparkSingleton

class DataLoader:
    def __init__(self):
        self.__spark = SparkSingleton.getInstance()
        self.__data = self.__spark.read.csv(HDFS_DATA_DIR, header=True, inferSchema=True)

    @property
    def data(self):
        return self.__data
    
    @property
    def spark(self):
        return self.__spark