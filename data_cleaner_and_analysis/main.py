from pyspark.sql import DataFrame

from data_loader import DataLoader
from cleaner import DataCleaner
from popularity_of_products import ProductPopularity
from top_item_category import TopCategory
from popular_locations import PopularLocations
from popular_times import PopularTimes

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
                    .split_datetime() \
                    .remove_pre2021_data() \
                    .remove_failed_transactions() \
                    .data
    
    truncated_data: ProductPopularity = ProductPopularity(cleaned_data) \
                                    .truncate_irrelevant_columns()
    
    # product_popularity: DataFrame = truncated_data.get_results().data
    # product_popularity_by_country: DataFrame = truncated_data.get_results_by_country().data
    
    # product_popularity.show(5, truncate=False)
    # product_popularity_by_country.show(5, truncate=False)   

    truncated_data.get_results().save_results('product_popularity.csv')

    top_category: DataFrame = TopCategory(cleaned_data).get_results()
    top_category.show()

    top_category_country: DataFrame = TopCategory(cleaned_data).get_results_by_country()
    top_category_country.show(top_category_country.count())

    popular_countries: DataFrame = PopularLocations(cleaned_data).get_popular_countries()
    popular_countries.show()

    popular_cities: DataFrame = PopularLocations(cleaned_data).get_popular_cities()
    popular_cities.show()

    popular_times_overall: DataFrame = PopularTimes(cleaned_data).get_popular_times_overall()
    popular_times_overall.show()

    popular_times_country: DataFrame = PopularTimes(cleaned_data).get_popular_times_countries()
    popular_times_country.show()

if __name__ == "__main__":
    main()