import os
import pandas as pd
import sqlite3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, substring, dayofmonth
from pyspark.sql.functions import mean, dayofyear, avg, udf
from pyspark.sql.types import *


schema = StructType([
        StructField("date", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("value", FloatType(), True),
        StructField("unit", StringType(), True),
        StructField("averagingPeriod", StringType(), True),
        StructField("location", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("coordinates", StringType(), True),
        StructField("attribution", StringType(), True),
        StructField("sourceName", StringType(), True),
        StructField("sourceType", StringType(), True),
        StructField("mobile", BooleanType(), True),
    ])


def get_last_csv(file_path) -> str:
    """
    A Function to get the last csv file in a directory.
    :param file_path: The path to the directory.
    :return: The path to the last csv file.
    """
    try:
        files = os.listdir(file_path)
        files.sort(key=lambda x: os.path.getmtime(os.path.join(file_path, x)))
        return os.path.join(file_path, files[-1])
    except FileNotFoundError:
        return "File not found"


def calculate_pm25_aqi(pm25: float) -> float:
    """
    A function to calculate the AQI for PM25.
    :param pm25: The PM25 value.
    :return: The AQI for PM25.
    """
    if pm25 <= 0:
        return 0
    elif 0 < pm25 <= 12:
        return ((50 - 0) / (12 - 0) * (pm25 - 0)) + 0
    elif 12.1 <= pm25 <= 35.4:
        return ((100 - 51) / (35.4 - 12.1) * (pm25 - 12)) + 51
    elif 35.5 <= pm25 <= 55.4:
        return ((150 - 101) / (55.4 - 35.4) * (pm25 - 35.5)) + 101
    elif 55.5 <= pm25 <= 150.4:
        return ((200 - 151) / (150.4 - 55.5) * (pm25 - 55.5)) + 151
    elif 150.5 <= pm25 <= 250.4:
        return ((300 - 201) / (250.4 - 150.5) * (pm25 - 150.5)) + 201
    elif 250.5 <= pm25 <= 350.4:
        return ((400 - 301) / (350.4 - 250.5) * (pm25 - 250.5)) + 301
    elif 350.5 < pm25 <= 500.4:
        return ((500 - 401) / (500.4 - 350.5) * (pm25 - 350.5)) + 401
    elif pm25 > 500.5:
        return ((999 - 501) / (99999.9 - 500.5) * (pm25 - 500.5)) + 501


def calculate_pm10_aqi(pm10: float) -> float:
    """
    A function to calculate the AQI for PM10.
    :param pm10: The PM10 value.
    :return: The AQI for PM10.
    """
    if pm10 <= 0:
        return 0
    elif 0 < pm10 <= 54:
        return ((50 - 0) / (54 - 0) * (pm10 - 0)) + 0
    elif 55 <= pm10 <= 154:
        return ((100 - 51) / (154 - 55) * (pm10 - 55)) + 51
    elif 155 <= pm10 <= 254:
        return ((150 - 101) / (254 - 155) * (pm10 - 155)) + 101
    elif 255 <= pm10 <= 354:
        return ((200 - 151) / (354 - 255) * (pm10 - 255)) + 151
    elif 355 <= pm10 <= 424:
        return ((300 - 201) / (424 - 355) * (pm10 - 355)) + 201
    elif 425 <= pm10 <= 504:
        return ((400 - 301) / (504 - 425) * (pm10 - 425)) + 301
    elif 505 <= pm10 <= 604:
        return ((500 - 401) / (604 - 505) * (pm10 - 505)) + 401
    elif pm10 > 604:
        ((999 - 501) / (99999.9 - 604) * (pm10 - 604)) + 501


def main():
    """
    The function to run the preprocessing.
    Creating sqlite database and inserting data into it.
    Creating a csv for data_catalog.
    """
    csv_path = get_last_csv("./spark_pipelines/airquality/resources")
    print(f"The last csv file is: {csv_path}")
    if csv_path != "File not found":
        spark = SparkSession.builder.getOrCreate()
        print("Spark session created")
        airquality_df = (spark
                         .read
                         .options(header='true')
                         .schema(schema)
                         .csv(csv_path))
        # Drop unnecessary columns
        df = airquality_df.drop(*('coordinates', 'unit', 'location',
                                'attribution', 'sourceName',
                                  'sourceType', 'mobile'))
        # Split the date column into utc and local
        df = df.withColumn("utc_date", split(df['date'], ',').getItem(0)) \
               .withColumn("local_date", split(df['date'], ',').getItem(1))
        df = df.withColumn('utc_date', substring(df['utc_date'], 10, 16)
                           .cast(TimestampType()))
        df = df.withColumn('local_date', substring(df['local_date'], 12, 16)
                           .cast(TimestampType()))
        # Drop the date column
        df = df.drop('date')
        # filter NL-FR-GB
        df = df.filter((col('country') == 'NL') | (col('country') == 'FR') |
                       (col('country') == 'GB'))
        # Drop rows which SO2 is unnecessery
        df = df.filter(col('parameter') != 'so2')
        # drop values which are NaN and
        df = df.drop(col('value').isNull())
        # Filter 24 hour average
        df = df.filter(col('averagingPeriod').contains("1"))
        # col('averagingPeriod').contains("24"))
        aggrated = df.groupby("country", "city", "parameter",
                              dayofmonth("utc_date").alias('day')) \
                     .agg(avg("value").alias("avg_24h"))
        aggrated = aggrated.drop('day')
        # create pandas df
        pandas_df = aggrated.toPandas()
        # Calculate the AQI for PM25 and PM10
        pandas_df['pm10_aqi'] = pandas_df.apply(lambda x: calculate_pm10_aqi(x['avg_24h'])if x['parameter'] == 'pm10' else 0, axis=1)
        pandas_df['pm25_aqi'] = pandas_df.apply(lambda x: calculate_pm25_aqi(x['avg_24h']) if x['parameter'] == 'pm25' else 0, axis=1)
        # create sqlite database

        conn = sqlite3.connect('test_database')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS airquality (country TEXT, \
                    city TEXT, parameter TEXT, avg_24h REAL)')
        conn.commit()
        pandas_df.to_sql('airquality', conn, if_exists='replace', index=False)

        c.execute('SELECT * FROM airquality')

        for row in c.fetchall():
            print(row)
    else:
        print("Resource file not found")


if __name__ == '__main__':
    main()
