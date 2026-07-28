from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from delta import configure_spark_with_delta_pip


def main():
    #to remove any cached spark session 
    SparkSession.builder.getOrCreate().stop()
    
    
    spark =( 
        SparkSession.builder.appName("ReadingDeltaTable")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ).getOrCreate()
    )

    
    # read stream from kafka
    delta_df = (
        spark.read.format("delta")
        .option("path", "C:/delta/bronze")
        .load()
    )
    delta_df.show()
    


if __name__ == "__main__":
    main()
