import streamlit as st
from pyspark.sql import SparkSession
import os
import sys
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder.appName("GoldViewer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
df = spark.read.format("delta").load("C:/delta/gold")

st.title("Real Time Analytics Dashboard")

st.dataframe(df.toPandas())
