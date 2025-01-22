class PopularTimes:

    def __init__(self, data):
        self.__data = data

    def popular_times_overall(self):
        hour = 0
        period = 'placeholder'
        if 6 <= hour < 12:
            period = 'morning'
        elif 12 <= hour < 18:
            period = 'afternoon'
        elif 18 <= hour < 24:
            period = 'evening'
        else:
            period = 'off-hours'

    def popular_times_countries(self):
        print('placeholder')