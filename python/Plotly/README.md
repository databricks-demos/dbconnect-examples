# Databricks Connect ❤️ Plotly

This is a sample app on how to show how easy it is to build a new application
using [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html)
and Plotly.

From DBR 13 onwards, Databricks Connect is now built on open-source Spark Connect.
Spark Connect introduces a decoupled client-server architecture for Apache Spark™
that allows remote connectivity to Spark clusters using the DataFrame API and
unresolved logical plans as the protocol. With this new architecture based on
Spark Connect, Databricks Connect becomes a thin client that is simple and easy
to use! It can be embedded everywhere to connect to Databricks: in IDEs, Notebooks
and any application, allowing customers and partners alike to build new (interactive)
user experiences based on their Databricks Lakehouse!

All you need to get started is a Databricks cluster and this simple Python
application. The dataset used in this application is the standard Databricks `samples`
dataset.

To get started, create a new virtual environment and install the reuired
dependencies

```commandline
pip instal -r requirements.txt
```

```python
from databricks.connect.session import DatabricksSession as SparkSession
from databricks.sdk import WorkspaceClient

config = WorkspaceClient(profile="PROFILE", cluster_id="CLUSTER_ID").config
spark = SparkSession.builder.sdkConfig(config).getOrCreate()
```


In the app.py file configure the values for `HOST`, `CLUSTER` and `TOKEN` with
correct values that identify your Databricks workspace, cluster ID and your personal
access token.

Run the plotly app

```shell
python app.py
```

![Screenshot](img/SCR-20230405-et1.png)


## Dependencies

This sample application is meant for illustration purposes only. The
application uses the follwing third-party dependencies:

  * Plotly / Dash - https://github.com/plotly/dash - The MIT License (MIT)
  * Tailwind CSS - https://github.com/tailwindlabs/tailwindcss - - The MIT License (MIT)