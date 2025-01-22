from pyspark.sql import DataFrame

class TopCategory():

    def __init__(self, data):
        self.__data = data

    def get_results(self):
        data: DataFrame = self.__data
        return data.groupBy('product_category').count().orderBy('count', ascending=False)
    
    def get_results_by_country(self):
        data: DataFrame = self.__data
        return data.groupBy('product_category', 'country').count().orderBy('count', ascending=False)
    