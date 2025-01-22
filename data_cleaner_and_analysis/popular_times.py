from pyspark.sql import functions as F

class PopularTimes:

    def __init__(self, data):
        self.__data = data

    def popular_times_overall(self):
        times_df = self.__data.withColumn('period',
                                F.when((F.col('hour') >= 6) & (F.col('hour') < 12), "morning")
                                .F.when((F.col('hour') >= 12) & (F.col('hour') < 18), "afternoon")
                                .F.when((F.col('hour') >= 18) & (F.col('hour') < 24), "evening")
                                .otherwise('off-hours'))
        return times_df.groupBy('period').count().orderBy('count', ascending=False)

    def popular_times_countries(self):
        times_df = self.__data.withColumn('period',
                                F.when((F.col('hour') >= 6) & (F.col('hour') < 12), "morning")
                                .F.when((F.col('hour') >= 12) & (F.col('hour') < 18), "afternoon")
                                .F.when((F.col('hour') >= 18) & (F.col('hour') < 24), "evening")
                                .otherwise('off-hours'))
        return times_df.groupBy('country', 'period').count().orderBy('count', ascending=False)