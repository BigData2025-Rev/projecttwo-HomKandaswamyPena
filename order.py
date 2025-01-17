import random as rand
import datetime as dt
import radar

class Order:
    id = 1
    ecommerce_websites = [
    "www.cvs.com",
    "www.walgreens.com",
    "www.riteaid.com",
    "www.goodrx.com",
    "www.healthmart.com",
    "www.chemistwarehouse.com.au",
    "www.pharmacytimes.com",
    "www.drugstore.com",
    "www.amazon.com/pharmacy",
    "www.alldaychemist.com",
    "www.iherb.com",
    "www.1mg.com",
    "www.pillpack.com",
    "www.medicalnewstoday.com/pharmacy",
    "www.expresspharmacy.co.uk",
    "www.safeway.com/pharmacy",
    "www.britishpharmacy.co.uk",
    "www.walmart.com/pharmacy",
    "www.mysmartpharmacy.com",
    "www.chemistdirect.co.uk"
]

    def __init__(self):
        self.__orderid = Order.id
        Order.id += 1
        self.__qty = rand.randint(1,9)
        rng = rand.random()
        if rng >= 0.00 and rng <= 0.20:
            self.__timestamp = radar.random_datetime(
                start = dt.datetime(year=2024, month=12, day=1),
                stop = dt.datetime(year=2024, month=12, day=31)
            )
        else:
            self.__timestamp = radar.random_datetime(
                start = dt.datetime(year=2024, month=1, day=1),
                stop = dt.datetime(year=2024, month=11, day=30)
            )
        rng = rand.random()
        if rng >= 0.00 and rng <= 0.10:
            self.__ecommercename = Order.ecommerce_websites[0]
        elif rng > 0.10 and rng <= 0.30:
            self.__ecommercename = Order.ecommerce_websites[1]
        else: self.__ecommercename = Order.ecommerce_websites[rand.randint(2,19)]

    def get_current_season(self):
        if self.__timestamp.month == 1 or self.__timestamp.month == 2 or self.__timestamp.month == 12: return "winter"
        elif self.__timestamp.month >= 3 and self.__timestamp.month <= 5: return "spring"
        elif self.__timestamp.month >= 6 and self.__timestamp.month <= 8: return "summer"
        elif self.__timestamp.month >= 9 and self.__timestamp.month <= 11: return "fall"
        return "winter"

    @property
    def orderid(self):
        return self.__orderid
    
    @property
    def qty(self):
        return self.__qty
    
    @property
    def timestamp(self):
        return self.__timestamp
    
    @property
    def ecommercename(self):
        return self.__ecommercename