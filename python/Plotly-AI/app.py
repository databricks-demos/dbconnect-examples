# Dash Related Imports
from dash import Dash, html, dcc, Output, Input, State
import plotly.express as px

# PySpark and databricks-connect related imports.
from databricks.connect.session import DatabricksSession as SparkSession
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

from pyspark_ai import SparkAI

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
            children="NYC Taxi Cockpit: Plotly x Databricks Demo with AI Power!",
        ),
        html.P(
            """This is a sample application to show-case how easy it is to get started with
            Databricks Connect and build interactive Python applications. The dataset used for
            this application is the standard Databricks samples dataset. All you need to get
            started is a Databricks cluster and this simple application.

            To run the integration with OpenAI, it is necessary to provide an OpenAI API token
            as an environment variable before starting the application. Simply export the variable
            "OPENAI_API_KEY" with the key as value.
            """,
            className="text-sm"
        ),
        html.Div(
            [
                html.H2(
                    "NYC Taxi analysis (data processing on Databricks)",
                    className="text-xl mt-4 mb-4 text-red-500 font-bold",
                ),
                html.P(
                    """The below visualization uses a heatmap display based on geo-coordinates for
                    either the pickup or drop-off dimension and a second
                    dimension is used for coloring.""",
                    className="text-sm pb-2"
                ),
                html.P("""
                Next, we have added a simple text box that the user can type an english
                description of the transformation of the input dataset that they want
                to apply. This "input" is sent to OpenAI via Langchain to generate the
                corresponding SQL transformation.
                """, className="text-sm pb-4"),
                html.Div(
                    [
                        html.Div("Dimension 1", className="font-bold"),
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
                        html.Div("Dimension 2", className="font-bold"),
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
                html.Div(
                    [
                        html.Div([html.P("English Filter", className="font-bold"),
                                  html.P("Using natural text to apply an additional transformation on the data powered by PySpark AI",
                                         className="italic text-xs")]
                                 ),
                        html.Div(
                            dcc.Textarea(id="english-filter",
                                         value="",
                                         className="columns-2 border-2",
                                         rows=5,
                                         style={"width": "100%"})
                        ),
                    ],
                    className="columns-2",
                ),
                html.Div(
                    [
                        html.Div("Action"),
                        html.Div(
                            html.Button('Submit', id="submit-val", n_clicks=0, className="border-2")
                        ),
                    ],
                    className="columns-2",
                ),
                dcc.Graph(id="NYC-zip-map-analysis")
            ]
        ),
    ],
)

config = WorkspaceClient(profile="PROFILE_NAME").config
spark = SparkSession.builder.sdkConfig(config).getOrCreate()
ai = SparkAI(spark_session=spark, verbose=True)
ai.activate()


@app.callback(
    Output("NYC-zip-map-analysis", "figure"),
    [Input("submit-val", "n_clicks")],
    [State("Map-dropdown-zip", "value"), State("Map-dropdown-agg", "value"), State("english-filter", "value")],
)
def update_output(clicks, zip_plot, column_map_show, filter):
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

    # English Filter
    if len(filter) > 0:
        df = df.ai.transform(filter)
        ai.commit()

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
