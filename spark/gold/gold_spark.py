from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, window, count, to_date, col
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import sys

# spark was not seeing correct root so I had to explicitly set it
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

sys.path.insert(0, PROJECT_ROOT)


from utils import upsert_to_delta
from utils import GOLD_CHECKPOINT_PATH, SILVER_DELTA_PATH, GOLD_DELTA_PATH

"""
As we are going with medillion architecture in this project so each file represents a model ,
this one as you can precieve from the name is depecting GOLD Model.

We would be performing aggregations for final business requirements

"""


def main():
    # spark session
    spark = (
        SparkSession.builder.appName("GoldSparkPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[*]")
        .getOrCreate()
    )

    # Read from delta via readStream
    gold_df = spark.readStream.format("delta").load(SILVER_DELTA_PATH)



    #we will just perform agg with windown and watermark to state store healthy and not overloaded.
    # aggregation using window
    gold_df  = gold_df.withWatermark("timestamp","10 minutes").groupby(
        window("timestamp", "10 minutes"), "event_type"
    ).agg(count("*").alias("event_count"))

    gold_df = gold_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type",
        "event_count"
    )
        
    query = (
        gold_df.writeStream.foreachBatch(
            lambda df, batch_id: upsert_to_delta(
                df,
                batch_id,
                "",
                GOLD_DELTA_PATH,
                False,
                
            )
        )
        .outputMode("complete")
        .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
        .start()
    )
    try:
        query.awaitTermination()

    except KeyboardInterrupt:
        print("Stopping...")
        query.stop()
        spark.stop()

    pass


if __name__ == "__main__":
    main()
