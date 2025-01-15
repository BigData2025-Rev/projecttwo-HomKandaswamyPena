import requests
from bs4 import BeautifulSoup
import json
from spark_singleton import SparkSingleton
from abc import ABC, abstractmethod

URL = "https://www.walgreens.com/"

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
        spark_session = SparkSingleton.getInstance()
        df = spark_session.createDataFrame([{
            "id": 1,
            "name": "Product 1",
            "price": 10.0,
            "category": "TestCategory"
        }])
        return df

def get_bottom_tier_elements(element):
    bottom_tier_elements = []
    for li in element.find_all('li', recursive=False):
        if li.find('ul') is None:  # No child ul means it's a bottom-tier element
            bottom_tier_elements.append(li)
        else:
            bottom_tier_elements.extend(get_bottom_tier_elements(li.find('ul')))
    return bottom_tier_elements


def get_categories():
    """
        Retrieves categories from an API
    """
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, "html.parser")
    root = soup.find('li', id='menu-shop-products')
    
    # print(soup.contents)
    # categories = soup.find_all("select", class_="container__list-lvl-2 show-next-lvl")
    # print(categories)
    # for element in main_categories:
    #     print(str(element))
    #     val = input("Press Enter to continue...")
    #     if val == '0':
    #         break

    # for element in sub_categories:
    #     print(str(element))
    #     val = input("Press Enter to continue...")
    #     if val == '0':
    #         break
        
        
    # category_list = []
    # for category in categories:
    #     h3 = category.find("h3")
    #     if h3 is None:
    #         continue
    #     category_list.append(h3.text)

    # print(category_list)
    # return category_list
def get_products():
    """
        Retrieves products from an API
    """
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, "html.parser")
    products = soup.find_all("div", class_="product")
    print(products)
    product_list = []
    for product in products:
        h3 = product.find("h3")
        span = product.find("span", class_="price")
        if h3 is None or span is None:
            continue
        product_dict = {
            "name": h3.text,
            "price": span.text
        }
        product_list.append(product_dict)

    print(product_list)
    # return product_list

def main():
    """
        Only executed once to retrieve products from an API
    """
    get_categories()
    # get_products()
    # products = get_products()
    # print(json.dumps(products, indent=2))
    # tst = Product()
    # test_df = tst.get_dataframe()
    # test_df.show()

if __name__ == "__main__":
    main()
