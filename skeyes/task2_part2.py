from pyspark.sql import SparkSession
from opensky_api import OpenSkyApi
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType, FloatType
from pyspark.sql.functions import col, sha2, concat_ws, lit, current_timestamp
import psycopg2
from psycopg2 import sql
import os
from pyspark import SparkContext
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def read():
    spark = SparkSession.builder \
        .appName("Extract") \
        .master("local") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
        .getOrCreate()
    
    api = OpenSkyApi()
    arrivals_2019 = api.get_arrivals_by_airport("EDDF", 1517227200, 1517230800)
    departures_2019 = api.get_departures_by_airport("EBBR", 1561939200, unix_increase(1561939200))


    arrivals_2021= api.get_arrivals_by_airport("EBBR", 1625097600, unix_increase(1625097600))
    departures_2021 = api.get_departures_by_airport("EBBR", 1625097600, unix_increase(1625097600))

    df_2019 = spark.createDataFrame(arrivals_2019 + departures_2019)
    df_2021 = spark.createDataFrame(arrivals_2021 + departures_2021)

    local_data_2019 = spark.read.option("header",True).csv("2019.csv")
    local_data_2021 = spark.read.option("header",True).csv("2021.csv")

    local_data_2019 = local_data_2019.filter(local_data_2019["ADES"] == "EBBR")
    local_data_2021 = local_data_2021.filter(local_data_2021["ADES"] == "EBBR")

    missing_2019 = local_data_2019.join(df_2019, local_data_2019["Call Sign"] == df_2019["callsign"], how="leftanti")
    missing_2021 = local_data_2021.join(df_2021, local_data_2021["Call Sign"] == df_2021["callsign"], how="leftanti")

    missing_2019.show()
    missing_2021.show()

    print("For the given period of July 1st to July 7th 2019 are " + str(missing_2019.count()) + " flights in the OpenSky DataSet that Skeyes has data for.")
    print("For the given period of July 1st to July 7th 2021 are " + str(missing_2021.count()) + " flights in the OpenSky DataSet that Skeyes has data for.")

    spark.stop()


def unix_increase(start_time):
    res = start_time + 7 * 24 * 60 * 60
    # print(res)
    return res

if __name__ == "__main__":
    read()
   

