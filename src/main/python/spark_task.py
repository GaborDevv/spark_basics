import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import BooleanType, StringType
from pygeohash import encode


# A User-Defined Functions (UDF) for finding data that is not a number
@udf(BooleanType())
def is_not_number(s) -> bool:
    try:
        float(s)
        return False
    except ValueError:
        return True
    except TypeError:
        return True


# OpenCage API key for geocoding
geocode_key = "0488dd6dec4b4f2f855d55387fb7da66"


# Get latitude using OpenCage API, if address is not properly given use the country and city (geohash is only using
# precision = 4)
def get_latitude(country, city, address) -> float:
    api_url = f"https://api.opencagedata.com/geocode/v1/json?q={country}+{city}+{address}&key={geocode_key}"
    response = requests.get(api_url).json()
    if response["results"]:
        return response["results"][0]["geometry"]["lat"]
    else:
        api_url = f"https://api.opencagedata.com/geocode/v1/json?q={country}+{city}&key={geocode_key}"
        response = requests.get(api_url).json()
        if response["results"]:
            return response["results"][0]["geometry"]["lat"]
        else:
            return 40


# Get longitude using OpenCage API, if address is not properly given use the country and city(geohash is only using
# small precision)
def get_longitude(country, city, address) -> float:
    api_url = f"https://api.opencagedata.com/geocode/v1/json?q={country}+{city}+{address}&key={geocode_key}"
    response = requests.get(api_url).json()
    if response["results"]:
        return response["results"][0]["geometry"]["lng"]
    else:
        api_url = f"https://api.opencagedata.com/geocode/v1/json?q={country}+{city}&key={geocode_key}"
        response = requests.get(api_url).json()
        if response["results"]:
            return response["results"][0]["geometry"]["lng"]
        else:
            return 40


# create udf for getting longitude, latitude and generating geohash code (using pygeohash library)
encode_udf = udf(lambda lat, lng: encode(float(lat), float(lng), precision=4), StringType())
get_longitude_udf = udf(get_longitude)
get_latitude_udf = udf(get_longitude)


def main():
    # Create a SparkSession object
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,"
                                                               "com.microsoft.azure:azure-storage:8.6.6,"
                                                               "com.google.guava:guava:32.0.1-jre").getOrCreate()

    # Set up Azure Storage account credentials
    storage_account_name = "gaborsstorage"
    storage_account_access_key = ("32Dmym0hdFxTYgwvHK5M2AsapmfkYrPe3JQpRXwMEhz5dHe73kyuOusVoyzXhO7dH4WraEZjenYo"
                                  "+AStnenrgA==")
    # Configure Azure Storage account for Spark
    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_access_key)
    # Disabling broadcast join because of size
    spark.conf.set("spark.sql.broadcastTimeout", 1200)

    # Define file location and type for hotel data which will be the first dataframe
    file_location = f"wasbs://hotels@{storage_account_name}.blob.core.windows.net/"
    file_type = "csv"

    # Define schema for hotel data
    hotel_schema = ("Id double, Name STRING, Country STRING, City STRING, Address STRING, Latitude double, Longitude "
                    "double")

    # Read hotel data into a DataFrame
    hotel_df = spark.read.format(file_type).option("header", "true").schema(hotel_schema).load(file_location)

    # Check Latitude and Longitude columns in the hotel dataframe and update it if not valid
    hotel_df = hotel_df.withColumn("Latitude", when(is_not_number(col("Latitude")) | col("Latitude").isNull(),
                                                    get_latitude_udf(col("Country"),
                                                                     col("City"),
                                                                     col("Address"))).otherwise(col("Latitude")))

    hotel_df = hotel_df.withColumn("Longitude", when(is_not_number(col("Longitude")) | col("Longitude").isNull(),
                                                     get_longitude_udf(col("Country"),
                                                                       col("City"),
                                                                       col("Address"))).otherwise(col("Longitude")))

    # generate Geohash column
    hotel_df = hotel_df.withColumn(
        "Geohash", encode_udf(col("Latitude"), col("Longitude"))
    )
    # set up variables to load parquet files containing weather data
    weather_files_location = f"wasbs://weather@{storage_account_name}.blob.core.windows.net/weather"
    weather_files_type = "parquet"
    # read weather data using inferSchema to recognize data types
    weather_df = spark.read.format(weather_files_type).option("header", "true").option("inferSchema", "true").load(
        weather_files_location)
    # create Geohash column
    weather_df = weather_df.withColumn("Geohash", when((col("lat").isNull()) | (col("lng").isNull()), "NaN")
                                       .otherwise(encode_udf(col("lat"), col("lng"))))
    # dropping duplicated data
    weather_df = weather_df.drop("lat", "lng")
    # left join hotel data and weather data
    result_df = weather_df.join(hotel_df, "Geohash", "left")
    # create variables to config spark to connect to datalake
    storage_account_name2 = "stgabordevvwesteurope"
    storage_account_access_key2 = "ia4K+6SaKozCtW+KD1mpRbVn+tplzwYGO8otsIFSn/APZV3XbUtGH0HW7CIZRuCsE600Ta5xZFOE+ASt3UM9YA=="
    spark.conf.set(f"fs.azure.account.key.{storage_account_name2}.dfs.core.windows.net", storage_account_access_key2)
    # writing results to azure gen 2 datalake
    write_location = f"abfss://data@{storage_account_name2}.dfs.core.windows.net/"
    result_df.write.format("parquet").mode("overwrite").partitionBy("year", "month", "day").save(
        write_location + "/results")
    spark.stop()


if __name__ == "__main__":
    main()
