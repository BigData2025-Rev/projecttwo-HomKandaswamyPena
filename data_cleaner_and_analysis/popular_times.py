import spark_singleton as ss

class PopularTimes:
    spark = ss.SparkSingleton.getInstance()

    def __init__(self, data):
        self.__data = data

    def popular_times_overall(self):
        print('placeholder')

    def popular_times_countries(self):
        print('placeholder')