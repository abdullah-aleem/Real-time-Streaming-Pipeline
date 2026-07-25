from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, window, count, to_date
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from utils.delta_table_helper_function import upsert_to_delta

def main():
    #just to stop if any previous session was running
    SparkSession.builder.getOrCreate().stop()
    
    spark = (
        SparkSession.builder.appName("KafkaStructuredDeltaStreamingConsuming")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # read stream from kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "user_events")
        .option("startingOffsets", "earliest")
        .load()
    )
    
    #bronze layer no transformation
    bronze_delta_path="C:/delta/bronze"



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
    messages_df = kafka_df.selectExpr(
        "CAST(key as STRING) as key", "CAST(value as STRING) as value"
    )

    # parsing from string to json
    messages_df = messages_df.withColumn("parsed", from_json("value", schema)).select(
        "parsed.*"
    )

    # creating another column to be used for partitioning
    messages_df = messages_df.withColumn("event_date", to_date("timestamp"))

    # watermarking and dedup are important in real time pipeline , they find duplicates between state stored in spark and new data all together
    messages_df = messages_df.withWatermark("timestamp", "10 minutes").dropDuplicates(
        ["event_id"]
    )

    #silver layer with data being 


    # # aggregation using window
    # messages_df = messages_df.groupby(
    #     window("timestamp", "10 minutes"), "event_type"
    # ).agg(count("*").alias("event_count"))


    # write stream to delta
    query = (
        messages_df.writeStream.foreachBatch(upsert_to_delta)
        .option("checkpointLocation", "C:/spark-checkpoints/kafka-stream")
        .start()
    )

    try:
        query.awaitTermination()

    except KeyboardInterrupt:
        print("Stopping...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
