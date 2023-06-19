package com.databricks.samples
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.functions.window

object SparkConnectSampleApplication {

  def configString: String = {
    s"sc://${System.getenv("HOST")}:443/;user_id=na;token=${System.getenv(
        "TOKEN")};x-databricks-cluster-id=${System.getenv("CLUSTER")}"
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().remote(configString).getOrCreate()

    println("Step 1 ....")
    val table_name_bronze = "main.martingrund.taxi_demo_bronze"
    var df = LoadData.run(spark)
    SaveData.run(df, table_name_bronze)

    println("Step 2 ....")
    df = spark.table(table_name_bronze)
    df = FilterColumns.run(df)
    df = TransformColums.run(df)
    df = FilterInvalid.run(df)

    val table_name_silver = "main.martingrund.taxi_demo_silver"
    SaveData.run(df, table_name_silver)

    println("Step 3 ...")
    val yellow_view = "main.martingrund.taxi_demo_view_yellow"
    val green_view = "main.martingrund.taxi_demo_view_green"
    CreateView.run(spark, table_name_silver, yellow_view, 1)
    CreateView.run(spark, table_name_silver, green_view, 2)

    println("Done ...")
  }
}
