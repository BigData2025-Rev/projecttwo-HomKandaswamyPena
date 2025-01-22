import spark_singleton as ss

class TopCategory():
    spark = ss.SparkSingleton.getInstance()

    def __init__(self, data):
        self.__data = data

    def get_results(self):
        data = self.__data
        return data.groupBy('product_category').count().orderBy('count', ascending=False)
    
    def get_results_by_country(self):
        data = self.__data
        return data.groupBy('product_category', 'country').count().orderBy('count', ascending=False)
    