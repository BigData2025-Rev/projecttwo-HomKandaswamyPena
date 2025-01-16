import json
import numpy as np
from spark_singleton import SparkSingleton
from pyspark.sql import DataFrame
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
            Returns a Spark DataFrame with product information. Can be unioned or joined with other dataframes.
        """
        pass

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
        NOTE: It takes a long time to run, as it creates 1000 product dataframes, and unions them together. 
        Let me know if you have any optimization suggestions.
    """
    main_df: DataFrame = None
    for i in range(1000):
        product = Product()
        df = product.get_dataframe()
        if main_df is None:
            main_df = df
        else:
            main_df = main_df.union(df)
    
    main_df.show(main_df.count(), truncate=False)

if __name__ == "__main__":
    test_product_creation()
