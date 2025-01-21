import spark_singleton as ss

class ProductPopularity():
    spark = ss.SparkSingleton.getInstance()
    
    def __init__(self, data):
        self.data = data
