from pyspark.sql import DataFrame

class ProductPopularity():
    
    def __init__(self, data):
        self.__data = data
    
    @property
    def data(self):
        return self.__data
    
    def truncate_irrelevant_columns(self):
        data: DataFrame = self.__data
        return ProductPopularity(data.drop('order_id', 'customer_id', 'customer_name', 'product_id', 'product_category', 'payment_type', 'price', 'city', 'ecommerce_website_name', 'payment_txn_id', 'payment_txn_success', 'failure_reason'))
    
    def get_results(self):
        data: DataFrame = self.__data
        return data.groupBy('product_id').count().orderBy('count', ascending=False)
    
    def get_results_by_country(self):
        data: DataFrame = self.__data
        return data.groupBy('product_id', 'country').count().orderBy('count', ascending=False)