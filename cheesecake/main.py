import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

def main(src_path, out_path):
    spark = SparkSession.builder.appName("Cheesecake").getOrCreate()

    schema = StructType([
        StructField("set_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("theme", StringType(), True),
        StructField("subtheme", StringType(), True),
        StructField("themeGroup", StringType(), True),
        StructField("category", StringType(), True),
        StructField("pieces", IntegerType(), True),
        StructField("minifigs", IntegerType(), True),
        StructField("agerange_n", StringType(), True),
        StructField("US_retailPr", DoubleType(), True),
        StructField("bricksetURL", StringType(), True),
        StructField("thumbnailURL", StringType(), True),
        StructField("imageURL", StringType(), True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(src_path)
    )

    # temp view example (your Example 1-3)
    df.createOrReplaceTempView("lego")
    df2 = spark.sql("SELECT * FROM lego WHERE year >= 2015 AND US_retailPr IS NOT NULL")
    df2.createOrReplaceTempView("lego_filtered")
    df3 = spark.sql("SELECT theme, COUNT(*) AS set_count, AVG(US_retailPr) AS avg_price FROM lego_filtered GROUP BY theme")
    df3.cache()

    df2.write.mode("overwrite").parquet(f"{out_path}/filtered")
    df3.write.mode("overwrite").parquet(f"{out_path}/aggregates")

    print("filtered count:", df2.count())
    print("aggregates count:", df3.count())
    df2.show(5, truncate=False)
    df3.orderBy("set_count", ascending=False).show(10, truncate=False)

    assert sorted(df3.collect()) == sorted(
        spark.sql("SELECT theme, COUNT(*) AS set_count, AVG(US_retailPr) AS avg_price FROM lego_filtered GROUP BY theme").collect()
    )

    spark.catalog.dropTempView("lego")
    spark.catalog.dropTempView("lego_filtered")
    spark.stop()

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "data/lego_sets.csv"
    out = sys.argv[2] if len(sys.argv) > 2 else "data/output"
    main(src, out)



