from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f



def main():
    spark = SparkSession.builder.appName("Cheesecake").getOrCreate()
    print("Hello from cheesecake!")


if __name__ == "__main__":
    main()

    

