from mylib.lib import (
    start_spark,
    end_spark,
    extract,
    load_data,
    describe,
    example_transform,
    log_output,
    reset_log,  # reset_log
)

def main():
    reset_log()  # Clear log at the beginning to avoid duplication

    # Step 1: Extract the data
    extract()

    # Step 2: Start Spark session
    spark = start_spark("Airline_Safety_Analysis")

    # Step 3: Load the data into a DataFrame
    df = load_data(spark)

    # Step 4: Describe the dataset
    describe(df)

    # Step 5: Run example SQL queries
    run_query(spark, df, "incidents_85_99 > 10")
    run_query(spark, df, "fatal_accidents_00_14 > 5")

    # Step 6: Perform data transformation
    example_transform(df)

    # Step 7: End Spark session
    end_spark(spark)

def run_query(spark, df, condition):
    """Filter the data based on a condition and log the results."""

    df.createOrReplaceTempView("AirlineSafety")

    query = f"SELECT * FROM AirlineSafety WHERE {condition}"
    result = spark.sql(query)

    log_output(
        f"Results for condition: {condition}",
        result.toPandas().to_markdown(),
        query,
    )
    result.show(truncate=False)

if __name__ == "__main__":
    main()
