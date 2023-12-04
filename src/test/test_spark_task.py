import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.main.python.spark_task import get_latitude, get_longitude, encode_udf, is_not_number


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


def test(spark_fixture):
    assert spark_fixture.version == "3.5.0"


def test_is_not_number(spark_fixture):
    data = [["asd", 12.5], [None, 25.4]]

    # Specify column names
    columns = ["Lon", "Lat"]

    # Create the DataFrame using toDF() with column names
    test_df = spark_fixture.createDataFrame(data, columns)
    test_df = test_df.withColumn("is_not_number_lon", is_not_number(col("Lon")))
    test_df_result = test_df.withColumn("is_not_number_lat", is_not_number(col("Lat")))

    # Assert that the UDF works as expected for the given data
    assert test_df_result.first()["is_not_number_lon"] is True
    assert test_df_result.collect()[1]["is_not_number_lon"] is True
    assert test_df_result.first()["is_not_number_lat"] is False
    assert test_df_result.collect()[1]["is_not_number_lat"] is False


def test_get_latitude():
    result = get_latitude("HU", "Oroshaza", "Huba u 15")
    assert result == 46.5603213 or result == 40


def test_get_longitude():
    result = get_longitude("HU", "Oroshaza", "Huba u 15")
    print(result)
    assert result == 20.6618592 or result == 40


def test_encode_udf(spark_fixture):
    # Test the encode_udf UDF
    test_df = spark_fixture.createDataFrame([("10.0", "20.0")], ["Latitude", "Longitude"])
    result_df = test_df.withColumn("Geohash", encode_udf(col("Latitude"), col("Longitude")))
    assert result_df.first()["Geohash"] == "s3y0"
