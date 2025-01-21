from spark_singleton import SparkSingleton



def main():
    spark = SparkSingleton.getInstance()
    data_df = spark.read.csv("data/P2datafile.csv", header=True, inferSchema=True)
    data_df.show()

if __name__ == "__main__":
    main()