from random_json_selector import RandomJSONSelector

PRODUCTS_FILE = "products.json"

"""
    This class will generate a product object with random properties based on a loaded json file.
    Intended for use in generator.py, where a row will be generated.
"""
    
class Product(RandomJSONSelector):
    products = RandomJSONSelector.load_list(PRODUCTS_FILE)

    def __init__(self, season):
        """
            Initializes the product object with random properties
        """
        self.products = self.get_seasonal_products(season)
        rnd_index = Product.get_normalized_rnd_integer(len(self.products))
        self.__name = self.products[rnd_index].get("name")
        self.__price = self.products[rnd_index].get("price")
        self.__category = self.products[rnd_index].get("category")
        self.__id = self.products[rnd_index].get("id")
    
    def get_seasonal_products(season):
        """
            Returns a list of products that are in the specified season
        """
        seasonal_categories = {
            'summer':[],
            'winter':["Shaving and Grooming", "Feminine Care", "Cough, Cold, and Flu"],
            'spring':[],
            'fall':[]
        }
        current_season = seasonal_categories.get(season)
        seasonal_products = [product for product in Product.products if product.get("category") in current_season ]
        
        return seasonal_products

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

