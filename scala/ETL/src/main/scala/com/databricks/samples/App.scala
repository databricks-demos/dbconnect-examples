package com.databricks.samples

import com.databricks.connect.DatabricksSession

object SparkConnectSampleApplication {
  def main(args: Array[String]) {

    val spark = DatabricksSession.builder.getOrCreate()

    val catalog = "main"
    val schema = "martingrund"

    println("Step 1 ....")
    val table_name_bronze = s"${catalog}.${schema}.taxi_demo_bronze"
    var df = LoadData.run(spark)
    SaveData.run(df, table_name_bronze)

    println("Step 2 ....")
    df = spark.table(table_name_bronze)
    df = FilterColumns.run(df)
    df = TransformColums.run(df)
    df = FilterInvalid.run(df)

    val table_name_silver = s"${catalog}.${schema}.taxi_demo_silver"
    SaveData.run(df, table_name_silver)

    println("Step 3 ...")
    val yellow_view = s"${catalog}.${schema}.taxi_demo_view_yellow"
    val green_view = s"${catalog}.${schema}.taxi_demo_view_green"
    CreateView.run(spark, table_name_silver, yellow_view, 1)
    CreateView.run(spark, table_name_silver, green_view, 2)

    println("Done ...")
  }
}
