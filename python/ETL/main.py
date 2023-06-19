from databricks.connect import DatabricksSession as SparkSession
from databricks.sdk.core import Config

import transformations as trans



def main():
  c = Config(profile="integ", cluster_id="0105-123836-uaih6uzs")
  spark = SparkSession.builder.sdkConfig(c).getOrCreate()
  print(spark.range(100).collect())

  # Step 1 - Load the Bronze Tables
  print("Step 1 .....")
  table_name_bronze = "main.martingrund.taxi_demo_bronze"
  df = trans.load_data(spark)
  trans.save_data(df, table_name_bronze)

  # Step 2 - Load the Silver Tables
  print("Step 2 .....")
  df = spark.table(table_name_bronze)
  df = trans.filter_columns(df)
  df = trans.transform_columns(df)
  df = trans.filter_invalid(df)

  table_name_silver = "main.martingrund.taxi_demo_silver"
  trans.save_data(df, table_name_silver)

  print(spark.table(table_name_silver).count())

  # Step 3 Prepare the views
  print("Step 3 .....")
  yellow_view = "main.martingrund.taxi_demo_view_yellow"
  green_view = "main.martingrund.taxi_demo_view_green"
  trans.create_view(spark, table_name_silver, yellow_view, 1)
  trans.create_view(spark, table_name_silver, green_view, 2)

if __name__ == "__main__":
  main()



