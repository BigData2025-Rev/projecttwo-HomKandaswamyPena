import spark_singleton as ss

class PopularLocations:
    spark = ss.SparkSingleton.getInstance()

    def __init__(self, data):
        self.__data = data
    
