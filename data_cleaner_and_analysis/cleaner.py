import spark_singleton as ss
from pyspark.sql import DataFrame

class DataCleaner():
    spark = ss.SparkSingleton.getInstance()
    
    def __init__(self, data):
        self.__data = data

    def remove_failed_transactions(self):
        data: DataFrame = self.__data
        return DataCleaner(data.filter(data['payment_txn_success'] == 'true'))

    def drop_id_columns(self):
        data: DataFrame = self.__data
        return DataCleaner(data.drop('order_id').drop('customer_id').drop('payment_txn_id'))

    def remove_corrupted_rows(self):
        data: DataFrame = self.__data
        return DataCleaner(data.replace(['null', 'NULL', 'Unknown', 'unknown'], None).dropna(how='any'))
    
    def display_data(self):
        data: DataFrame = self.__data
        columns = data.columns
        for column in columns:
            data.select(column).distinct().show(data.count())
            var = input(f"Press enter to continue to the next column: {column}")
    
    @property
    def data(self):
        return self.__data