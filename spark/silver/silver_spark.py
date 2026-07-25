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
from utils import SILVER_CHECKPOINT_PATH, SILVER_DELTA_PATH, BRONZE_DELTA_PATH

"""
As we are going with medillion architecture in this project so each file represents a model ,
this one as you can precieve from the name is depecting Silver Model.

We would be performing Schema Transformations and other bussiness related Transformations Here

"""


def main():
    # spark session
    spark = (
        SparkSession.builder.appName("SilverSparkPipeline")
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
    silver_df = spark.readStream.format("delta").load(BRONZE_DELTA_PATH)

    # json parsing and transformation to be handled here with dedup

    # lets setup the expecting schema for the upstream data , hardcoded field values do not handle schema evaluation (ps learned it the hard way)
    schema = StructType(
        [
            StructField("user_id", IntegerType()),
            StructField("event_type", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("event_id", LongType()),
        ]
    )

    # kafka gives key/value as binary
    silver_df = silver_df.selectExpr(
        "CAST(key as STRING) as key", "CAST(value as STRING) as value"
    )

    # parsing from string to json
    silver_df = silver_df.withColumn("parsed", from_json("value", schema)).select(
        "parsed.*"
    )

    # creating another column to be used for partitioning
    silver_df = silver_df.withColumn("event_date", to_date("timestamp"))

    # watermarking and dedup are important in real time pipeline , they find duplicates between state stored in spark and new data all together
    silver_df = (
        silver_df.withWatermark("timestamp", "10 minutes")
        .dropDuplicates(["event_id"])
        .filter(
            col("event_id").isNotNull()
            & col("event_type").isNotNull()
            & col("timestamp").isNotNull()
        )
    )

    query = (
        silver_df.writeStream.foreachBatch(
            lambda df, batch_id: upsert_to_delta(
                df,
                batch_id,
                "target.event_id=source.event_id and target.event_date=source.event_date",
                SILVER_DELTA_PATH,
                True,
                ["event_date"],
            )
        )
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
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
