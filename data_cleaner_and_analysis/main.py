from spark_singleton import SparkSingleton



def main():
    spark = SparkSingleton.getInstance()
    print(spark.version)

if __name__ == "__main__":
    main()