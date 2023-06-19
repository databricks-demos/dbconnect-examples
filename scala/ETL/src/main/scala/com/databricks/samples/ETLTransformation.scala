package com.databricks.samples
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{functions => F}

object LoadData {
  def run(spark: SparkSession): DataFrame = {
    val nyc_schema = StructType(
      Seq(
        StructField("vendor", StringType, true),
        StructField("pickup_datetime", TimestampType, true),
        StructField("dropoff_datetime", TimestampType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("pickup_longitude", DoubleType, true),
        StructField("pickup_latitude", DoubleType, true),
        StructField("rate_code", StringType, true),
        StructField("store_and_forward", StringType, true),
        StructField("dropoff_longitude", DoubleType, true),
        StructField("dropoff_latitude", DoubleType, true),
        StructField("payment_type", StringType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("surcharge", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("total_amount", DoubleType, true)))
    val yellow = (
      spark.read
        .format("csv")
        .option("header", "true")
        .schema(nyc_schema)
        .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz")
    )
    val green = (
      spark.read
        .format("csv")
        .option("header", "true")
        .schema(nyc_schema)
        .load("dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2019-12.csv.gz")
    )
    yellow.unionAll(green)
  }
}

object FilterColumns {
  def run(df: DataFrame): DataFrame = {
    df.select(
      df.col("vendor"),
      df.col("passenger_count"),
      df.col("trip_distance"),
      df.col("fare_amount").alias("amount"),
      df.col("total_amount").alias("total"),
      df.col("tolls_amount").alias("tolls"))
  }
}

object TransformColums {
  def run(df: DataFrame): DataFrame = {
    val cleaned = df
      .withColumn(
        "passenger_type",
        F.when(df.col("passenger_count") > 1, "multi").otherwise("single"))
      .withColumn("has_tolls", F.when(df.col("tolls") > 0, true).otherwise(false))
      .withColumn("amount_rounded", F.ceil(df.col("amount")))

    cleaned.select(
      cleaned.col("vendor"),
      cleaned.col("passenger_type"),
      cleaned.col("has_tolls"),
      cleaned.col("trip_distance").alias("distance"),
      cleaned.col("amount_rounded").alias("amount"))
  }
}

object FilterInvalid {
  def run(df: DataFrame): DataFrame = {
    df.filter(df.col("distance") > 0)
      .filter(df.col("amount") > 0)
      .filter(!((df.col("distance") < 5) && (df.col("amount") > 100)))
  }
}

object SaveData {
  def run(df: DataFrame, name: String): Unit = {
    df.write.format("delta").mode("overwrite").saveAsTable(name)
  }
}


object CreateView {
  def run(spark: SparkSession, source: String, dest: String, vendor: Int): Unit = {
    spark.sql(
      s"""
        |create or replace view ${dest} as select * except(vendor) from ${source} where vendor = ${vendor}
        |""".stripMargin)
  }
}
