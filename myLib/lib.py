"""
Library functions for PySpark Data Processing on the airline-safety dataset.
"""
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

LOG_FILE = "pyspark_output.md"

def reset_log():
    """Clears the log file at the beginning of each run."""
    with open(LOG_FILE, "w") as file:
        file.write("# PySpark Operation Logs\n\n")

def log_output(operation, output, query=None):
    """Adds operation logs to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def start_spark(appName):
    """Starts a Spark session."""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    """Stops the Spark session."""
    spark.stop()
    return "Stopped Spark session"

def extract(
    url="https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv",
    file_path="data/airline-safety.csv",
    directory="data",
):
    """Extracts a CSV from a URL and saves it locally."""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

def load_data(spark, data="data/airline-safety.csv", name="AirlineSafety"):
    """Loads the airline safety dataset and selects specific columns."""
    df = spark.read.option("header", "true").csv(data)
    df = df.select(
        "airline",
        "avail_seat_km_per_week",
        "incidents_85_99",
        "fatal_accidents_85_99",
        "fatalities_85_99",
        "incidents_00_14",
        "fatal_accidents_00_14",
        "fatalities_00_14"
    )
    log_output("load data", df.limit(10).toPandas().to_markdown())
    return df

def query(spark, df, query, name):
    """Executes a Spark SQL query."""
    df.createOrReplaceTempView(name)
    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)
    return spark.sql(query).show()

def describe(df):
    """Generates descriptive statistics for the dataset."""
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)
    return df.describe().show()

def example_transform(df):
    """Performs transformations on the airline safety dataset, adding derived columns."""
    df = df.withColumn(
        "Total_Incidents",
        col("incidents_85_99") + col("incidents_00_14")
    )
    df = df.withColumn(
        "Total_Fatalities",
        col("fatalities_85_99") + col("fatalities_00_14")
    )
    df = df.withColumn(
        "Risk_Flag",
        when(col("Total_Incidents") > 10, 1).otherwise(0)
    )
    log_output("transform data", df.limit(10).toPandas().to_markdown())
    return df.show()
