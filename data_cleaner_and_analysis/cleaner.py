import spark_singleton as ss
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, hour, quarter, concat, lit

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
    
    def split_datetime(self):
        data: DataFrame = self.__data
        return DataCleaner(data.withColumn('year', year(data['datetime'])) \
                            .withColumn('quarter', quarter(data['datetime'])) \
                            .withColumn('month', month(data['datetime'])) \
                            .withColumn('hour', hour(data['datetime'])) \
                            .withColumn("year_quarter", concat(year(data['datetime']), lit(" Q"), quarter(data['datetime'])))
                            .drop('datetime'))
    
    def remove_pre2021_data(self):
        data: DataFrame = self.__data
        return DataCleaner(data.filter(data['year'] >= 2021))
    
    def replace_country_names(self):
        data: DataFrame = self.__data
        country_names = {
            'US': 'United States of America',
            'UK': 'United Kingdom',
            'IN': 'India',
            'JP': 'Japan',
            'FR': 'France',
            'DE': 'Germany'
        }
        return DataCleaner(data.replace(country_names, subset=['country']))

    def display_data(self):
        data: DataFrame = self.__data
        columns = data.columns
        for column in columns:
            data.select(column).distinct().show(data.count())
            var = input(f"Press enter to continue to the next column: {column}")
    
    @property
    def data(self):
        return self.__data