from base_analysis import BaseAnalysis

class PopularLocations(BaseAnalysis):

    def __init__(self, data):
        self.__data = data

    @property
    def data(self):
        return self.__data

    def get_popular_countries(self):
        return self.__data.groupBy('country').count().orderBy('count', ascending=False).limit(4)

    def get_popular_cities(self):
        return self.__data.groupBy('city', 'country').count().orderBy('count', ascending=False).limit(10)
