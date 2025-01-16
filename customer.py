from random_json_selector import RandomJSONSelector
CUSTOMERS_FILE = "customers.json"

"""
    This class will generate a customer object with random properties based on a loaded json file.
    Intended for use in OrderGenerator class, where a row will be generated.
"""

    
class Customer(RandomJSONSelector):
    customers = RandomJSONSelector.load_list(CUSTOMERS_FILE)

    def __init__(self):
        """
            Initializes the customer object with random properties
        """
        rnd_index = Customer.get_normalized_rnd_integer(len(Customer.customers))
        self.__name = Customer.customers[rnd_index].get("name")
        self.__country = Customer.customers[rnd_index].get("country")
        self.__city = Customer.customers[rnd_index].get("city")
        self.__id = Customer.customers[rnd_index].get("id")
    
    @property
    def id(self):
        return self.__id

    @property
    def name(self):
        return self.__name
    
    @property
    def country(self):
        return self.__country
    
    @property
    def city(self):
        return self.__city

