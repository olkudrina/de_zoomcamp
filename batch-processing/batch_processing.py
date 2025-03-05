import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, unix_timestamp, max, asc
from log_utils import set_logger


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("INFO")

logger = set_logger()

# output spark version
logger.info("pyspark version: %s", spark.version)

# read the data and show first rows
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df.show()

# What is the average size of the Parquet (ending with .parquet extension)
# Files that were created (in MB)? Select the answer which most closely matches.
df.repartition(4).write.mode("append").parquet("./data/")

# How many taxi trips were there on the 15th of October?
# Consider only trips that started on the 15th of October.
logger.info(
    "Total amount of trips: %s",
    df.filter(
        (to_date(df.tpep_pickup_datetime) == "2024-10-15")
        & (to_date(df.tpep_dropoff_datetime) == "2024-10-15")
    ).count(),
)

# What is the length of the longest trip in the dataset in hours?
longest_trip = df.select(
    (
        max(
            unix_timestamp(df.tpep_dropoff_datetime)
            - unix_timestamp(df.tpep_pickup_datetime)
        )
        / 3600
    ).alias("hours")
)
longest_trip.show()

# Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?
zones = spark.read.options(header=True).csv("taxi_zone_lookup.csv")
zones.createOrReplaceTempView("zones")
df = df.join(zones, df.PULocationID == zones.LocationID)
df.select("Zone").groupBy("Zone").count().orderBy(asc("count")).show(3)
