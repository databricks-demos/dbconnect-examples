from pyspark.sql.functions import col, lit, expr, when
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame


from datetime import datetime
import time


def load_data(spark: SparkSession):
    """Creates and loads the data from files into a DF"""
    nyc_schema = StructType(
        [
            StructField("vendor", StringType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("rate_code", StringType(), True),
            StructField("store_and_forward", StringType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("payment_type", StringType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("surcharge", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    )
    yellow = (
        spark.read.format("csv")
        .options(header=True)
        .schema(nyc_schema)
        .load(
            "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz"
        )
    )
    green = (
        spark.read.format("csv")
        .options(header=True)
        .schema(nyc_schema)
        .load(
            "dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2019-12.csv.gz"
        )
    )
    df = yellow.unionAll(green)
    return df


def filter_columns(df: DataFrame) -> DataFrame:
    return df.select(
        df.vendor,
        df.passenger_count,
        df.trip_distance,
        df.fare_amount.alias("amount"),
        df.total_amount.alias("total"),
        df.tolls_amount.alias("tolls"),
    )


def transform_columns(df: DataFrame) -> DataFrame:
    cleaned = (
        df.withColumn(
            "passenger_type",
            F.when(df.passenger_count > 1, "multi").otherwise("single"),
        )
        .withColumn("has_tolls", F.when(df.tolls > 0, True).otherwise(False))
        .withColumn("amount_rounded", F.ceil(df.amount))
    )
    cleaned = cleaned.select(
        cleaned.vendor,
        cleaned.passenger_type,
        cleaned.has_tolls,
        cleaned.trip_distance.alias("distance"),
        cleaned.amount_rounded.alias("amount"),
    )
    return cleaned


def filter_invalid(df: DataFrame) -> DataFrame:
    filtered = df.filter(df.distance > 0).filter(df.amount > 0)
    filtered = filtered.filter(~((filtered.distance < 5) & (filtered.amount > 100)))
    return filtered


def save_data(df: DataFrame, name: str) -> None:
    df.write.saveAsTable(name, format="delta", mode="overwrite")


def create_view(spark: SparkSession, source: str, dest: str, vendor: int) -> None:
    spark.sql(
        f"""
  create or replace view {dest} as select * except(vendor) from {source} where vendor = {vendor}
  """
    )
