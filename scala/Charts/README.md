# Example JFreeCharts Application (Scala)

This is a sample application which makes a chart based on remote server response.
The application uses the existing Databricks sample datasets.

## Getting started

1. Import the project into your favorite IDE that can build Scala projects.
2. To run, make sure to set the environment variables for `DATABRICKS_HOST`,
`DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`.
Alternatively, please see [docs](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html#set-up-the-client)
for other ways to set up the client.

## Plots

The application produces the following plots

![Number of trips per hour](/scala/Charts/pics/trips_by_hour.png)

![Fraction of local trips](/scala/Charts/pics/localtrips.png)

![Number of trips per day](/scala/Charts/pics/trips_per_day.png)
