from pyspark.sql import functions

class PopularTimes:

    def __init__(self, data):
        self.__data = data

    def popular_times_overall(self):
        times_df = self.__data.withColumn('hour', hour(self.__data.datetime))
        times_df = times_df.withColumn('period',
                                 when((col('hour') >= 6) & (col('hour') < 12), "morning")
                                .when((col('hour') >= 12) & (col('hour') < 18), "afternoon")
                                .when((col('hour') >= 18) & (col('hour') < 24), "evening")
                                .otherwise('off-hours'))
        times_df = times_df.groupBy('period').count().orderBy('count', ascending=False)
        return times_df

    def popular_times_countries(self):
        print('placeholder')