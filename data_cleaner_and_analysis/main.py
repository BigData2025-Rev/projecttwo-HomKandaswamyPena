from pyspark.sql import DataFrame

from data_loader import DataLoader
from cleaner import DataCleaner
from popularity_of_products import ProductPopularity
from top_item_category import TopCategory
from popular_locations import PopularLocations
from popular_times import PopularTimes
from payment import Payment

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

def display_pena_results(data):
    truncated_data: ProductPopularity = ProductPopularity(data) \
                                    .truncate_irrelevant_columns() \
                                    .filter_by_popularity()
    
    product_popularity: DataFrame = truncated_data.get_results()
    product_popularity_by_country: DataFrame = truncated_data.get_results_by_country()
    
    product_popularity.show(truncate=False)
    product_popularity_by_country.show(truncate=False)   

    ProductPopularity(product_popularity).save_results('product_popularity.csv')
    ProductPopularity(product_popularity_by_country).save_results('product_popularity_by_country.csv')


def display_kandaswamy_results(data):
    popular_countries: DataFrame = PopularLocations(data).get_popular_countries()
    popular_countries.show()

    popular_cities: DataFrame = PopularLocations(data).get_popular_cities()
    popular_cities.show()

    popular_times_overall: DataFrame = PopularTimes(data).get_popular_times_overall()
    popular_times_overall.show()

    popular_times_country: DataFrame = PopularTimes(data).get_popular_times_countries()
    popular_times_country.show()

def display_hom_results(data):
    top_category: DataFrame = TopCategory(data).get_results()
    top_category.show()

    top_category_country: DataFrame = TopCategory(data).get_results_by_country()
    top_category_country.show(top_category_country.count())
    payments : DataFrame = Payment(data).get_results()
    payments.show()
    payments_by_country : DataFrame = Payment(data).get_results_by_country()
    payments_by_country.show()
    payments_success: DataFrame = Payment(data).get_successful_transactions()
    payments_success.show()

def main():
    data_loader = DataLoader()
    data_df: DataFrame = data_loader.data

    cleaned_data = DataCleaner(data_df) \
                    .remove_corrupted_rows() \
                    .drop_id_columns() \
                    .split_datetime() \
                    .remove_pre2021_data() \
                    .remove_failed_transactions() \
                    .replace_country_names() \
                    .data
    
    display_hom_results(cleaned_data)
    display_kandaswamy_results(cleaned_data)
    display_pena_results(cleaned_data)
    

    

   

    

if __name__ == "__main__":
    main()