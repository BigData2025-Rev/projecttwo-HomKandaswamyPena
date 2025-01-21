import spark_singleton as ss

class ProductPopularity():
    spark = ss.SparkSingleton.getInstance()
    
    def __init__(self, data):
        self.__data = data

    def get_results(self):
        data = self.__data
        return data.groupBy('product_id').count().orderBy('count', ascending=False)