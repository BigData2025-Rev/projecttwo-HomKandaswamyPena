import spark_singleton as ss
from pyspark.sql import DataFrame

class ProductPopularity():
    spark = ss.SparkSingleton.getInstance()
    
    def __init__(self, data):
        self.__data = data
    
    @property
    def data(self):
        return self.__data
    
    def get_popularity_over_year(self):
        data: DataFrame = self.__data
        return data.groupBy('product_id').count().orderBy('count', ascending=False)