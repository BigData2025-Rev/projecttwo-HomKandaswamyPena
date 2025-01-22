import spark_singleton as ss

class PopularLocations:
    spark = ss.SparkSingleton.getInstance()

    def __init__(self, data):
        self.__data = data

    def get_popular_countries(self):
        return self.__data.groupBy('country').count().orderBy('count', ascending=False)

    def get_popular_cities(self):
        return self.__data.groupBy('city', 'country').count().orderBy('count', ascending=False)
