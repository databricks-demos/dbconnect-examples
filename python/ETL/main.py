from databricks.connect import DatabricksSession

import transformations as trans

def main():
  # For more detail on configuring the connection properties for your
  # Databricks workspace please refer to this documentation:
  # https://docs.databricks.com/en/dev-tools/databricks-connect/python/install.html#configure-connection-python

  # If you have a default profile set in your .databrickscfg no additional code
  # changes are needed.
  spark = DatabricksSession.builder.serverless().getOrCreate()

  # Alternate way to configure your Spark session:
  # spark = DatabricksSession.builder.profile("PROFILE").clusterId("CLUSTER_ID").getOrCreate()

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



