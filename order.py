import numpy
import datetime

class Order:

    def __init__(self):
        self.__orderid = 0
        self.__qty = 0
        self.__timestamp = datetime.datetime.now()
        self.__ecommercename = ""
        print("Order info created")

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