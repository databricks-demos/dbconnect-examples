# Dash Related Imports
from dash import Dash, html, dcc, Output, Input
import plotly.express as px

# PySpark and databricks-connect related imports.
from databricks.connect.session import DatabricksSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

# Generic Imports
import json

# Load the JS and CSS for Syntax Highlighting
external_scripts = ["https://cdn.tailwindcss.com"]

external_stylesheets = [
    "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.7.0/styles/default.min.css"
]

app = Dash(
    __name__,
    external_scripts=external_scripts,
    external_stylesheets=external_stylesheets,
)
app.layout = html.Div(
    className="container mx-auto max-w-4xl mt-8 bg-white p-3",
    children=[
        html.H1(
            className="text-2xl mb-4 text-red-500 font-bold",
            children="NYC Taxi Cockpit: Plotly x Databricks Demo",
        ),
        html.P(
            """This is a sample application to show-case how easy it is to get started with
            Databricks Connect and build interactive Python applications. The dataset used for
            this application is the standard Databricks samples dataset. All you need to get
            started is a Databricks cluster and this simple application.""",
        ),
        html.Div(
            [
                html.H2(
                    "NYC Taxi analysis (data processing on Databricks)",
                    className="text-xl mt-4 mb-4 text-red-500 font-bold",
                ),
                html.P(
                    """The below visualization uses a heatmap display based on geo-coordinates for
         either the pickup or dropoff dimension and a second dimension is used for coloring. """
                ),
                html.Div(
                    [
                        html.Div("Dimension 1"),
                        html.Div(
                            dcc.Dropdown(
                                ["pickup_zip", "dropoff_zip"],
                                "dropoff_zip",
                                id="Map-dropdown-zip",
                                className="form-input",
                            )
                        ),
                    ],
                    className="columns-2",
                ),
                html.Div(
                    [
                        html.Div("Dimension 2"),
                        html.Div(
                            dcc.Dropdown(
                                [
                                    "avg_trip_duration",
                                    "cout_trips",
                                    "avg_trip_distance",
                                ],
                                "avg_trip_duration",
                                id="Map-dropdown-agg",
                            )
                        ),
                    ],
                    className="columns-2",
                ),
                dcc.Graph(id="NYC-zip-map-analysis"),
                html.H2(
                    "Example Code", className="text-xl mt-4 mb-4 text-red-500 font-bold"
                ),
                html.Div(
                    [
                        html.Pre(
                            """
@app.callback(
    Output("NYC-zip-map-analysis", "figure"),
    [Input('Map-dropdown-zip', 'value'),
     Input('Map-dropdown-agg', 'value')]
)
def update_output(zip_plot, column_map_show):
    df = spark.read.table("samples.nyctaxi.trips")
    df = df.withColumn('DurationInMin', F.round(
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2))
    df = df.where(df.DurationInMin < 200)
    df = df.withColumn("day_of_week", F.date_format("tpep_pickup_datetime", 'E'))
    df_map = (df.groupBy(col(zip_plot).alias('zip_code')).agg(
        F.avg('DurationInMin').alias('avg_trip_duration'),
        F.count('DurationInMin').alias('cout_trips'),
        F.avg('trip_distance').alias('avg_trip_distance'))
    )

    df_map_show = df_map.toPandas()
    df_map_show.head()

    nycmap = json.load(open("nyc-zip-code-tabulation-areas-polygons.geojson"))
    return px.choropleth_mapbox(df_map_show,
                                geojson=nycmap,
                                locations="zip_code",
                                featureidkey="properties.postalCode",
                                color=column_map_show,
                                color_continuous_scale="viridis",
                                mapbox_style="carto-positron",
                                zoom=9, center={"lat": 40.7, "lon": -73.9},
                                opacity=0.7,
                                hover_name="zip_code"
                                )
                """,
                            className="text-xs border-solid border-2 border-slate-400 rounded bg-slate-200 p-4 overflow-scroll",
                            id="code-hl",
                        )
                    ]
                ),
            ]
        ),
        html.Div(
            [
                html.H2(
                    "Trips by pickup zip(data processing on Databricks)",
                    className="text-xl mt-4 mb-4 text-red-500 font-bold",
                ),
                html.P(),
                html.Div(
                    className="columns-2 gap-8",
                    children=[
                        html.Div("Count >="),
                        dcc.Input(
                            id="count-input",
                            value="10",
                            type="number",
                            className="border border-solid form-input border-slate-400",
                        ),
                    ],
                ),
                html.Div(dcc.Graph(id="postcode-trip-count")),
                html.H2("Example Code", className="text-xl mt-4 mb-4"),
                html.Div(
                    [
                        html.Pre(
                            """
@app.callback(
    Output("postcode-trip-count", "figure"),
    Input("count-input", "value")
)
def update_trip_count(greaterThan):
    df = spark.read.table("samples.nyctaxi.trips")
    df = df.withColumn("pickup_zip",
                       col("pickup_zip").cast(StringType())).withColumn("dropoff_zip",
                                                                        col("dropoff_zip").cast(
                                                                            StringType()))
    df = df.groupBy("pickup_zip", "dropoff_zip").count()
    df = df.filter(col("count") >= int(greaterThan))

    return px.scatter(df.toPandas(), x="pickup_zip", y="dropoff_zip", size="count")
                """,
                            className="text-xs border-solid border-2 border-slate-400 rounded bg-slate-200 p-4 overflow-scroll",
                        )
                    ]
                ),
            ]
        ),
    ],
)

# For more detail on configuring the connection properties for your
# Databricks workspace please refer to this documentation:
# https://docs.databricks.com/en/dev-tools/databricks-connect/python/install.html#configure-connection-python

# If you have a default profile set in your .databrickscfg no additional code
# changes are needed.
spark = DatabricksSession.builder.serverless().getOrCreate()

# Alternate way to configure your Spark session:
# spark = DatabricksSession.builder.profile("PROFILE").clusterId("CLUSTER_ID").getOrCreate()

@app.callback(Output("postcode-trip-count", "figure"), Input("count-input", "value"))
def update_trip_count(greaterThan):
    df = spark.read.table("samples.nyctaxi.trips")
    df = df.withColumn("pickup_zip", col("pickup_zip").cast(StringType())).withColumn(
        "dropoff_zip", col("dropoff_zip").cast(StringType())
    )
    df = df.groupBy("pickup_zip", "dropoff_zip").count()
    df = df.filter(col("count") >= int(greaterThan))

    return px.scatter(df.toPandas(), x="pickup_zip", y="dropoff_zip", size="count")


@app.callback(
    Output("NYC-zip-map-analysis", "figure"),
    [Input("Map-dropdown-zip", "value"), Input("Map-dropdown-agg", "value")],
)
def update_output(zip_plot, column_map_show):
    df = spark.read.table("samples.nyctaxi.trips")
    df = df.withColumn(
        "DurationInMin",
        F.round(
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60,
            2,
        ),
    )
    df = df.where(df.DurationInMin < 200)
    df = df.withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "E"))
    df_map = df.groupBy(col(zip_plot).alias("zip_code")).agg(
        F.avg("DurationInMin").alias("avg_trip_duration"),
        F.count("DurationInMin").alias("cout_trips"),
        F.avg("trip_distance").alias("avg_trip_distance"),
    )

    df_map_show = df_map.toPandas()
    df_map_show.head()

    nycmap = json.load(open("nyc-zip-code-tabulation-areas-polygons.geojson"))
    return px.choropleth_mapbox(
        df_map_show,
        geojson=nycmap,
        locations="zip_code",
        featureidkey="properties.postalCode",
        color=column_map_show,
        color_continuous_scale="viridis",
        mapbox_style="carto-positron",
        zoom=9,
        center={"lat": 40.7, "lon": -73.9},
        opacity=0.7,
        hover_name="zip_code",
    )


if __name__ == "__main__":
    app.run_server(debug=True)
