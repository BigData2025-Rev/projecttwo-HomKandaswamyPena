import random as rand
import datetime
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
        self.__qty = rand.randint(1,10)
        self.__timestamp = radar.random_datetime(
            start = datetime.datetime(year=2024, month=1, day=1),
            stop = datetime.datetime(year=2024, month=12, day=31)
        )
        random_ecommerce = rand.randint(0,19)
        self.__ecommercename = Order.ecommerce_websites[random_ecommerce]

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