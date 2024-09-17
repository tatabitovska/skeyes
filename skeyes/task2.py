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

def extract():

    spark = SparkSession.builder \
        .appName("Extract") \
        .master("local") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
        .getOrCreate()

    bucket_schema = StructType([
        StructField("baro_altitude", FloatType(), nullable=True),
        StructField("callsign", StringType(), nullable=True),
        StructField("category", IntegerType(), nullable=True),
        StructField("geo_altitude", FloatType(), nullable=True),
        StructField("icao24", StringType(), nullable=True),
        StructField("last_contact", IntegerType(), nullable=True),
        StructField("latitude", FloatType(), nullable=True),
        StructField("longitude", FloatType(), nullable=True),
        StructField("on_ground", BooleanType(), nullable=True),
        StructField("origin_country", StringType(), nullable=True),
        StructField("position_source", IntegerType(), nullable=True),
        StructField("sensors", StringType(), nullable=True),
        StructField("spi", BooleanType(), nullable=True),
        StructField("squawk", StringType(), nullable=True),
        StructField("time_position", IntegerType(), nullable=True),
        StructField("true_track", FloatType(), nullable=True),
        StructField("velocity", FloatType(), nullable=True),
        StructField("vertical_rate", FloatType(), nullable=True)
    ])
    api = OpenSkyApi()
    states = api.get_states()

    data = []
    for state in states.states:
        data.append({
            'baro_altitude': float(state.baro_altitude) if state.baro_altitude is not None else None,
            'callsign': state.callsign,
            'category': int(state.category) if state.category is not None else None,
            'geo_altitude': float(state.geo_altitude) if state.geo_altitude is not None else None,
            'icao24': state.icao24,
            'last_contact': int(state.last_contact) if state.last_contact is not None else None,
            'latitude': float(state.latitude) if state.latitude is not None else None,
            'longitude': float(state.longitude) if state.longitude is not None else None,
            'on_ground': state.on_ground,
            'origin_country': state.origin_country,
            'position_source': int(state.position_source) if state.position_source is not None else None,
            'sensors': state.sensors,
            'spi': state.spi,
            'squawk': state.squawk,
            'time_position': int(state.time_position) if state.time_position is not None else None,
            'true_track': float(state.true_track) if state.true_track is not None else None,
            'velocity': float(state.velocity) if state.velocity is not None else None,
            'vertical_rate': float(state.vertical_rate) if state.vertical_rate is not None else None
        })

# Create the DataFrame with the defined schema
    df = spark.createDataFrame(data, schema=bucket_schema)

    # df = spark.createDataFrame(states.states, schema=bucket_schema)
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    file_name = f"{current_timestamp}"
    df.write.parquet(file_name)

    spark.stop()
    return current_timestamp
    

def create_db_and_tables():    

    conn = psycopg2.connect(
        dbname="postgres",  
        user="ana",  
        password="ana",  
        host="localhost",  
        port="5432" 
    )
    conn.autocommit = True 
    cur = conn.cursor()
    
    try:
        cur.execute("CREATE DATABASE openskies;")  
        print("Database created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print("Database already exists.")

    
    cur.close()
    conn.close()

    conn = psycopg2.connect(database="openskies",
                        host="localhost",
                        user="ana",
                        password="ana",
                        port="5432"
                        )
    
    conn.autocommit = True 
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS dim_airplane (
            callsign VARCHAR(10),
            category INT,
            icao24 VARCHAR(6) PRIMARY KEY,
            origin_country VARCHAR(100),
            spi BOOLEAN
        );
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS fact_status (
            baro_altitude FLOAT,
            geo_altitude FLOAT,
            icao24 VARCHAR(6),
            last_contact INT,
            latitude FLOAT,
            longitude FLOAT,
            on_ground BOOLEAN,
            position_source INT,
            sensors TEXT,
            squawk TEXT,
            time_position INT,
            true_track FLOAT,
            velocity FLOAT,
            vertical_rate FLOAT,
            hash_diff TEXT,
            hash TEXT,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP
        );
    ''')

    print("Tables created successfully.")
    conn.commit()
    cur.close()
    conn.close()

    
def load(ct):

    spark = SparkSession \
    .builder \
    .appName("Load") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
    .getOrCreate()

    url = "jdbc:postgresql://localhost:5432/openskies"

    properties = {
        "user": "ana",
        "password": "ana",
        "driver": "org.postgresql.Driver"
    }

    f = None
    for filename in os.listdir(f"{ct}"):
        if filename.endswith('.parquet'):
            f = filename
    
    if not f:
        spark.stop()
        print("No new file today.")
        return -1

    df = spark.read.parquet(f"{ct}/{f}")

    dim_airplane = df.select(["callsign", "category", "icao24", "origin_country", "spi"])
    dim_airplane = dim_airplane.dropDuplicates(["icao24"])
    # dim_airplane.show()

    fact_status = df.select(['baro_altitude', 'geo_altitude', 'icao24', 'last_contact',
                             'latitude', 'longitude', 'on_ground', 'position_source', 
                             'sensors', 'squawk', 'time_position', 'true_track', 
                             'velocity', 'vertical_rate'])

    fact_status = fact_status.dropDuplicates()

    fact_status = fact_status.withColumn(
        "hash", sha2(concat_ws("||", col("icao24"), col("last_contact")), 256)
    )

    columns_to_hash = ['baro_altitude', 'geo_altitude', 'latitude', 'longitude', 'on_ground',
                       'position_source', 'sensors', 'squawk', 'time_position', 'true_track',
                       'velocity', 'vertical_rate']
    
    fact_status = fact_status.withColumn(
        "hash_diff", sha2(concat_ws("||", *[col(c) for c in columns_to_hash]), 256)
    )

    fact_status = fact_status.withColumn("valid_from", lit(None).cast("timestamp")) \
                            .withColumn("valid_to", lit(None).cast("timestamp"))

    # fact_status.show(truncate=False)


    sql_dim_airplane = "dim_airplane"
    sql_dim_airplane_df = spark.read.jdbc(url, sql_dim_airplane, properties=properties)

    sql_fact_status = "fact_status"
    sql_fact_status_df = spark.read.jdbc(url, sql_fact_status, properties=properties)
    

    new_dim_airplane = dim_airplane.join(sql_dim_airplane_df, on="icao24", how="left_anti")

    new_dim_airplane.write.jdbc(
    url=url,
    table="dim_airplane",
    mode="append",
    properties=properties
    )

    #update fact status with scd type 2 and new data

    active_sql_fact_status_df = sql_fact_status_df.filter(F.col("valid_to").isNull())
    active_fact_status = fact_status.filter(F.col("valid_to").isNull())

    updated_records = active_sql_fact_status_df.join(
    active_fact_status.select(
        F.col("hash").alias("hash_u"), 
        F.col("hash_diff").alias("hash_diff_u")
    ),
    on=F.col("hash") == F.col("hash_u"),  
    how="inner"
    ).filter(
        F.col("hash_diff_u") != F.col("hash_diff")  
    )
    
    updated_records.show()
    droped_historical_data_updates = updated_records.drop("hash_u").drop("hash_diff_u")
    historical_data_updates = droped_historical_data_updates.withColumn("valid_to", F.current_timestamp())
    new_records = active_fact_status.subtract(sql_fact_status_df.select(fact_status.columns))

    window_spec = Window.partitionBy("hash").orderBy(F.desc("valid_to"))

    # dropped_new_records = active_sql_fact_status_df.drop("valid_to").drop("valid_from")
    new_records = new_records.join(
    active_sql_fact_status_df.select(
        F.col("hash").alias("hash_u"), 
        F.col("hash_diff").alias("hash_diff_u")
    ),
    on=F.col("hash") == F.col("hash_u"),
    how="left"
    ).withColumn(
        "valid_from", 
        F.coalesce(F.lag("valid_to", 1).over(window_spec), F.current_timestamp())
    ).withColumn(
        "valid_to", 
        F.lit(None)  
    )

    new_records = new_records.drop("hash_u").drop("hash_diff_u")

    final_df = historical_data_updates.union(new_records)

    final_df.write.jdbc(url=url, table="fact_status", mode="append", properties = properties)

    spark.stop()

def unix_increase(start_time):
    res = start_time + 24 * 60 * 60
    return res

def check_db_entries():
    spark = SparkSession \
    .builder \
    .appName("Load") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
    .getOrCreate()

    url = "jdbc:postgresql://localhost:5432/openskies"

    properties = {
        "user": "ana",
        "password": "ana",
        "driver": "org.postgresql.Driver"
    }

    sql_dim_airplane = "dim_airplane"
    sql_dim_airplane_df = spark.read.jdbc(url, sql_dim_airplane, properties=properties)

    sql_fact_status = "fact_status"
    sql_fact_status_df = spark.read.jdbc(url, sql_fact_status, properties=properties)

    sql_dim_airplane_df.show()
    sql_fact_status_df.filter(sql_fact_status_df["valid_to"].isNotNull()).show()
    spark.stop()

if __name__ == "__main__":
    # create_db_and_tables()
    # ct = extract()
    # load(ct)
    check_db_entries()
    
    