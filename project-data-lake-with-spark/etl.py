import configparser
import datetime as dt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
    TimestampType,
)

config = configparser.ConfigParser()
config.read("dl_template.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"

    # read song data file
    song_schema = StructType(
        [
            StructField("artist_id", StringType()),
            StructField("artist_latitude", FloatType()),
            StructField("artist_longitude", FloatType()),
            StructField("artist_location", StringType()),
            StructField("artist_name", StringType()),
            StructField("duration", FloatType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ]
    )
    df = spark.read.format("json").option("path", song_data).schema(song_schema).load()

    df.createOrReplaceTempView("songs_stage")
    # extract columns to create songs table
    songs_table = spark.sql(
        """SELECT DISTINCT song_id, title, artist_id, year, duration
FROM songs_stage
    """
    )

    # write songs table to parquet files partitioned by year and artist

    songs_table.write.format("parquet").partitionBy("year", "artist_id").option(
        "path", f"{output_data}/songs_table.parquet"
    ).mode("overwrite").save()

    # extract columns to create artists table
    artists_table = spark.sql(
        """SELECT DISTINCT artist_id, artist_name, artist_location, 
        artist_latitude, artist_longitude FROM songs_stage
    """
    )

    # write artists table to parquet files
    artists_table = (
        artists_table.write.format("parquet")
        .option("path", f"{output_data}/artists_table.parquet")
        .mode("overwrite")
        .save()
    )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    event_schema = StructType(
        [
            StructField("artist", StringType()),
            StructField("auth", StringType()),
            StructField("firstName", StringType()),
            StructField("gender", StringType()),
            StructField("itemInSession", IntegerType()),
            StructField("lastName", StringType()),
            StructField("length", FloatType()),
            StructField("level", StringType()),
            StructField("location", StringType()),
            StructField("method", StringType()),
            StructField("page", StringType()),
            StructField("registration", StringType()),
            StructField("sessionId", IntegerType()),
            StructField("song", StringType()),
            StructField("status", IntegerType()),
            StructField("ts", TimestampType()),
            StructField("userAgent", StringType()),
            StructField("userId", IntegerType()),
        ]
    )
    # read log data file
    df = (
        spark.read.format("json")
        .option("mode", "PERMISSIVE")
        .schema(event_schema)
        .option("path", log_data)
        .load()
    )
    df.createOrreplaceTempView("events_stage")
    # filter by actions for song plays
    df = spark.sql("select * from event_stage where page='NextSong'")

    # extract columns for users table
    users_table = spark.sql(
        """
    SELECT DISTINCT user_id, firstName, gender,
                lastName, level
FROM events_stage WHERE user_id IS NOT NULL"""
    )

    # write users table to parquet files
    users_table.write.format("parquet").mode("overwrite").option(
        "path", f"{output_data}/sparkify/users_table.parquet"
    ).save()

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    get_hour = udf(lambda x: int(dt.datetime(x).hour), IntegerType())
    get_day = udf(lambda x: int(dt.datetime(x).day), IntegerType())
    get_week = udf(lambda x: int(dt.datetime(x).week), IntegerType())
    get_weekday = udf(lambda x: int(dt.datetime(x).weekday), IntegerType())
    get_month = udf(lambda x: int(dt.datetime(x).month), IntegerType())
    get_year = udf(lambda x: int(dt.datetime(x).year), IntegerType())
    df = df.withColumn("hour", get_hour(df["ts"]))
    df = df.withColumn("day", get_day(df["ts"]))
    df = df.withColumn("week", get_week(df["ts"]))
    df = df.withColumn("weekday", get_weekday(df["ts"]))
    df = df.withColumn("month", get_month(df["ts"]))
    df = df.withColumn("year", get_year(df["ts"]))
    df = df.withColumnRenamed("ts", "start_time")

    # extract columns to create time table
    time_table = spark.sql(
        """
    SELECT ts, hour, day, week, month, year, weekday
    FROM events_stage
    """
    )

    # write time table to parquet files partitioned by year and month
    time_table.format("parquet").partitionBy("year", "month").mode("overwrite").option(
        "path", f"{output_data}/sparkify/time_table.parquet"
    ).save()
    # read in song data to use for songplays table
    song_df = (
        spark.read.format("parquet")
        .option("path", f"{output_data}/sparkify/songs_table.parquet")
        .option("inferSchema", "true")
        .load()
    )
    song_df.createOrreplaceTempView("songs_stage")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """
    SELECT  es.ts AS start_time,
        es.user_id AS user_id,
        es.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        es.session_id AS session_id,
        es.location AS location,
        es.userAgent AS user_agent
        FROM events_stage AS es
        JOIN songs_stage AS ss
            ON (es.artist_name = ss.artist_name)
        """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://mybucket-for-gdrive/sparkify-dev"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
