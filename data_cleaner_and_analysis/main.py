from spark_singleton import SparkSingleton

from config import HDFS_DATA_DIR

def main():
    spark = SparkSingleton.getInstance()
    data_df = spark.read.csv(HDFS_DATA_DIR, header=True, inferSchema=True)
    data_df.show()

if __name__ == "__main__":
    main()