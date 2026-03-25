from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f



def main():
    spark = SparkSession.builder.appName("Cheesecake").getOrCreate()
    
    # Create a simple DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)
    
    # Perform a simple transformation
    df_filtered = df.filter(df.age > 26)
    
    # Show results
    print("Original DataFrame:")
    df.show()
    print("\nFiltered DataFrame (age > 26):")
    df_filtered.show()
    
    spark.stop()


if __name__ == "__main__":
    main()

    

