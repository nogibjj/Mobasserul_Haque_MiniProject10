"""
Test suite for Airline Safety data analysis
"""

import os
import pytest
from lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)

@pytest.fixture(scope="module")
def spark():
    """Fixture for initializing and stopping Spark session for tests"""
    spark = start_spark("TestApp_Airline_Safety")
    yield spark
    end_spark(spark)

def test_extract():
    """Test file extraction from URL"""
    file_path = extract()
    assert os.path.exists(file_path), "Extracted file does not exist."

def test_load_data(spark):
    """Test loading data into Spark DataFrame"""
    df = load_data(spark)
    assert df is not None, "Data loading failed: DataFrame is None."

def test_describe(spark):
    """Test generating statistical summary of data"""
    df = load_data(spark)
    describe(df)  # Expecting only successful execution

def test_query(spark):
    """Test querying records based on specific conditions"""
    df = load_data(spark)
    view_name = "AirlineSafety"

    # Run query for incidents > 10 in 1985-1999
    query(
        spark,
        df,
        "SELECT airline, incidents_85_99 FROM AirlineSafety WHERE incidents_85_99 > 10",
        view_name,
    )

    # Run query for fatal accidents > 5 in 2000-2014
    query(
        spark,
        df,
        "SELECT airline, fatal_accidents_00_14 FROM AirlineSafety WHERE fatal_accidents_00_14 > 5",
        view_name,
    )

def test_example_transform(spark):
    """Test that example_transform runs and modifies the DataFrame"""
    df = load_data(spark)
    example_transform(df)  # Expecting only successful execution

if __name__ == "__main__":
    pytest.main()
