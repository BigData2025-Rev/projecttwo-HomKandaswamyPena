import json
import numpy as np

from abc import ABC, abstractmethod

PRODUCTS_FILE = "products.json"

"""
    This class will generate a product object with random properties based on a loaded json file.
    Intended for use in generator.py, where a row will be generated.
"""

class IProduct(ABC):

    @staticmethod
    def get_normalized_rnd_integer(n):
        """
            Returns a random integer based on a normal distribution.
            The integer is clipped to the range of [0, n - 1].
            Args:
                n (int): The upper bound of the range.
            Returns:
                int: A random integer in the range of [0, n - 1].
        """
        mean = n / 2
        std_dev = n / 4
        dim_size = 1
        random_integer = np.clip(np.round(np.random.normal(mean, std_dev, dim_size)).astype(int), 0, n - 1)
        return random_integer[0]
    
    @staticmethod
    def load_products():
        """
            Loads the products from the json file.
            Returns:
                list: A list of products.
        """
        with open(PRODUCTS_FILE, "r") as file:
            products = json.load(file)
        return products
    
class Product(IProduct):
    products = IProduct.load_products()

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

