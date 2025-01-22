from spark_singleton import SparkSingleton
from pyspark.sql import DataFrame

from config import HDFS_DATA_DIR
from cleaner import DataCleaner
from popularity_of_products import ProductPopularity
from top_item_category import TopCategory

HDFS_DATA_DIR = "hdfs://localhost:9000/user/ehom/P2datafile.csv"

def load_data():
    spark = SparkSingleton.getInstance()
    data_df: DataFrame = spark.read.csv(HDFS_DATA_DIR, header=True, inferSchema=True)
    return data_df

def main():
    data_df: DataFrame = load_data()
   
    cleaned_data = DataCleaner(data_df) \
                    .remove_corrupted_rows() \
                    .drop_id_columns() \
                    .remove_failed_transactions() \
                    .data
    
    product_popularity: DataFrame = ProductPopularity(cleaned_data).get_results()
    product_popularity.show(product_popularity.count(), truncate=False)

    cleaned_data.show(cleaned_data.count(), truncate=False)
    print(cleaned_data.count())

    top_category: DataFrame = TopCategory(cleaned_data).get_results()
    top_category.show()

    top_category_country: DataFrame = TopCategory(cleaned_data).get_results_by_country()
    top_category_country.show(top_category_country.count())

if __name__ == "__main__":
    main()