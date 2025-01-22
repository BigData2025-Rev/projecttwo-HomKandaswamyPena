from pyspark.sql import DataFrame

from data_loader import DataLoader
from cleaner import DataCleaner
from popularity_of_products import ProductPopularity
from top_item_category import TopCategory

"""
    cd into the directory containing this file and run the following command:
    python main.py
    WSL:
    python3 main.py

    Likely issue to encounter: load_env() returning None values
    Solution: Check the .env file and make sure the environment variables are set correctly. See config file for details.
    Make sure that the .env file is in the same directory as the main.py file.
    Make sure to cd into this directory before running the script.
"""

def main():
    data_loader = DataLoader()
    data_df: DataFrame = data_loader.data

    cleaned_data = DataCleaner(data_df) \
                    .remove_corrupted_rows() \
                    .drop_id_columns() \
                    .remove_failed_transactions() \
                    .data
    
    product_popularity: DataFrame = ProductPopularity(cleaned_data).get_popularity_over_year()
    product_popularity.show(product_popularity.count(), truncate=False)

    cleaned_data.show(cleaned_data.count(), truncate=False)
    print(cleaned_data.count())

    top_category: DataFrame = TopCategory(cleaned_data).get_results()
    top_category.show()

    top_category_country: DataFrame = TopCategory(cleaned_data).get_results_by_country()
    top_category_country.show(top_category_country.count())

if __name__ == "__main__":
    main()