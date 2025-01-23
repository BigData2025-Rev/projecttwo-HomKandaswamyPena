from pyspark.sql import functions as F
from base_analysis import BaseAnalysis

class PopularTimes(BaseAnalysis):

    def __init__(self, data):
        self.__data = data

    @property
    def data(self):
        return self.__data

    def get_popular_times_overall(self):
        times_df = self.__data.withColumn('period',
                                F.when((F.col('hour') >= 6) & (F.col('hour') < 12), 'Morning')
                                .when((F.col('hour') >= 12) & (F.col('hour') < 18), 'Afternoon')
                                .when((F.col('hour') >= 18) & (F.col('hour') < 24), 'Evening')
                                .otherwise('Off-hours'))
        return times_df.groupBy('period').count().orderBy('count', ascending=False)

    def get_popular_times_countries(self):
        times_df = self.__data.withColumn('period',
                                F.when((F.col('hour') >= 6) & (F.col('hour') < 12), 'Morning')
                                .when((F.col('hour') >= 12) & (F.col('hour') < 18), 'Afternoon')
                                .when((F.col('hour') >= 18) & (F.col('hour') < 24), 'Evening')
                                .otherwise('Off-hours'))
        return times_df.groupBy('country', 'period').count().orderBy('count', ascending=False)
    
    def get_popular_months(self):
        months_df = self.__data.withColumn('month_name', 
                                F.when(F.col('month') == 1, 'January')
                                .when(F.col('month') == 2, 'February')
                                .when(F.col('month') == 3, 'March')
                                .when(F.col('month') == 4, 'April')
                                .when(F.col('month') == 5, 'May')
                                .when(F.col('month') == 6, 'June')
                                .when(F.col('month') == 7, 'July')
                                .when(F.col('month') == 8, 'August')
                                .when(F.col('month') == 9, 'September')
                                .when(F.col('month') == 10, 'October')
                                .when(F.col('month') == 11, 'November')
                                .when(F.col('month') == 12, 'December')
                                .otherwise('Invalid Month'))
        return months_df.groupBy('country', 'month_name').count().orderBy(['country', 'count'], ascending=False)