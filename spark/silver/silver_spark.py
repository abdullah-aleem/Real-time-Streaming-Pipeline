from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, window, count, to_date
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import sys

# spark was not seeing correct root so I had to explicitly set it
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

sys.path.insert(0, PROJECT_ROOT)


from utils import upsert_to_delta
from utils import SILVER_CHECKPOINT_PATH, SILVER_DELTA_PATH,BRONZE_DELTA_PATH

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

    # Read from Kafka via readStream
    silver_df = (
        spark.readStream.format("delta")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "user_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    # no transformation and no schema convertion also just write the data
    query = (
        bronze_df.writeStream.foreachBatch(
            lambda df, batch_id: upsert_to_delta(
                df, batch_id, query, BRONZE_DELTA_PATH, False
            )
        )
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
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
