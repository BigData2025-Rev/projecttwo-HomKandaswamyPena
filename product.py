from random_json_selector import RandomJSONSelector

PRODUCTS_FILE = "products.json"

"""
    This class will generate a product object with random properties based on a loaded json file.
    Intended for use in generator.py, where a row will be generated.
"""
    
class Product(RandomJSONSelector):
    products = RandomJSONSelector.load_products(PRODUCTS_FILE)

    def __init__(self):
        """
            Initializes the product object with random properties
        """
        rnd_index = Product.get_normalized_rnd_integer(len(Product.products))
        self.__name = Product.products[rnd_index].get("name")
        self.__price = Product.products[rnd_index].get("price")
        self.__category = Product.products[rnd_index].get("category")
        self.__id = Product.products[rnd_index].get("id")
    
    @property
    def id(self):
        return self.__id

    @property
    def name(self):
        return self.__name
    
    @property
    def price(self):
        return self.__price
    
    @property
    def category(self):
        return self.__category

