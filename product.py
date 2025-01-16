import json
import numpy as np
from spark_singleton import SparkSingleton
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from abc import ABC, abstractmethod

PRODUCTS_FILE = "products.json"

"""
    This class will generate a product object with random properties based on a loaded json file.
    Intended for use in generator.py, where a row will be generated.
"""

class IProduct(ABC):
    @abstractmethod
    def get_dataframe(self):
        """
            Returns a Spark DataFrame with product information.
        """
        pass
    @staticmethod
    def get_normalized_rnd_integer(n):
        mean = n / 2
        std_dev = n / 4
        dim_size = 1
        random_integer = np.clip(np.round(np.random.normal(mean, std_dev, dim_size).astype(int)), 0, n - 1)
        return random_integer[0]
    
    @staticmethod
    def load_products():
        with open(PRODUCTS_FILE, "r") as file:
            products = json.load(file)
        return products
    
class Product(IProduct):
    products = IProduct.load_products()

    def __init__(self):
        rnd_index = Product.get_normalized_rnd_integer(len(Product.products))
        self._name = Product.products[rnd_index].get("name")
        self._price = Product.products[rnd_index].get("price")
        self._category = Product.products[rnd_index].get("category")
        self._id = Product.products[rnd_index].get("id")
        
    def get_dataframe(self):
        spark_session = SparkSingleton.getInstance()
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("price", FloatType(), False),
            StructField("category", StringType(), False)
        ])

        df = spark_session.createDataFrame([(self._id, self._name, self._price, self._category)], schema=schema)
        return df

def test_product_creation():
    """
        Test function to check if the product object is created correctly.
        Make sure it is run from the root directory. 
        It should print a dataframe with a single row, containing product information from a randomly selected product.
    """
    for i in range(10):
        product = Product()
        df = product.get_dataframe()
        df.show()

if __name__ == "__main__":
    test_product_creation()
