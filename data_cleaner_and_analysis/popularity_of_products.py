from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth, hour, col, lit, concat

class ProductPopularity():
    
    def __init__(self, data):
        self.__data = data
    
    @property
    def data(self):
        return self.__data
    
    def truncate_irrelevant_columns(self):
        data: DataFrame = self.__data
        return ProductPopularity(data.drop('order_id', 'customer_id', 'customer_name', 'product_id', 'product_category', 'payment_type', 'price', 'city', 'ecommerce_website_name', 'payment_txn_id', 'payment_txn_success', 'failure_reason'))
    
    def filter_by_popularity(self):
        data: DataFrame = self.__data
        popular_products = data.groupBy('product_name').sum('qty').withColumnRenamed('sum(qty)', 'popularity').orderBy('popularity', ascending=False).limit(5)
        return ProductPopularity(data.filter(col('product_name').isin([row['product_name'] for row in popular_products.collect()])))
    
    def get_results(self):
        data: DataFrame = self.__data
        return data.groupBy('product_name') \
                    .pivot('year_quarter') \
                    .sum('qty') \
                    .fillna(0)
                    # .sum('qty') \
                    # .withColumnRenamed('sum(qty)', 'popularity') \
                    # .orderBy('popularity', ascending=False)
    
    def get_results_by_country(self):
        data: DataFrame = self.__data
        return data.groupBy('product_name', 'year_quarter', 'country') \
                .sum('qty') \
                .withColumnRenamed('sum(qty)', 'popularity') \
                .orderBy('popularity', ascending=False)
    
    def save_results(self, file_path):
        data: DataFrame = self.__data
        data.write.mode('overwrite').csv(file_path, header=True, mode='overwrite')