from pyspark.sql import DataFrame

class Payment():

    def __init__(self, data):
        self.__data = data

    def get_results(self):
        data: DataFrame = self.__data
        return data.groupBy('payment_type').count().orderBy('count', ascending=False)
    
    def get_successful_transactions(self):
        data: DataFrame = self.__data
        return data.groupBy('payment_txn_success').count().orderBy('count', ascending=False)
    