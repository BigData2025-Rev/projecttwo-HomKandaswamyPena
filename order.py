import random as rand
import datetime

class Order:
    orderid = 1

    def __init__(self):
        self.__orderid = Order.orderid
        Order.orderid += 1
        self.__qty = rand.randint(1,10)
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