import requests
import json
from spark_singleton import SparkSingleton
from abc import ABC, abstractmethod

"""
    This class will generate a product object with random properties.
    Intended for use in generator.py, where a row will be generated.
"""

class IProduct(ABC):
    @abstractmethod
    def get_dataframe(self):
        """
            Returns a Spark DataFrame with product information.
        """
        pass

class Product(IProduct):
    def __init__(self):
        print("Product created")
    def get_dataframe(self):
        """
            Returns a Spark DataFrame with product information.
        """
        spark_session = SparkSingleton.getInstance()
        df = spark_session.createDataFrame([{
            "id": 1,
            "name": "Product 1",
            "price": 10.0,
            "category": "TestCategory"
        }])
        return df

def main():
    """
        Only executed once to retrieve products from an API
    """
    tst = Product()
    test_df = tst.get_dataframe()
    test_df.show()

if __name__ == "__main__":
    main()
