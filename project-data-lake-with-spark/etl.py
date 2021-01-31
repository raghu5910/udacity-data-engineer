import configparser
import datetime as dt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (
    year,
    month,
    hour,
    dayofmonth,
    dayofweek,
    weekofyear,
    date_format,
)
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.functions import to_timestamp


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
            StructField("ts", StringType()),
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
    get_timestamp = udf(
        lambda x: dt.datetime.utcfromtimestamp(int(x) / 1000), TimestampType()
    )
    df = df.withColumn("ts", get_timestamp("ts"))
    df.createOrReplaceTempView("events_stage")
    # filter by actions for song plays
    df = spark.sql("select * from events_stage where page='NextSong'")

    # extract columns for users table
    users_table = spark.sql(
        """
    SELECT DISTINCT userId, firstName, gender,
                lastName, level
FROM events_stage WHERE userId IS NOT NULL"""
    )

    # write users table to parquet files
    users_table.write.format("parquet").mode("overwrite").option(
        "path", f"{output_data}/users_table.parquet"
    ).save()

    # create timestamp column from original timestamp column

    df = df.withColumn("hour", hour("ts"))
    df = df.withColumn("day", dayofmonth("ts"))
    df = df.withColumn("week", weekofyear("ts"))
    df = df.withColumn("weekday", dayofweek("ts"))
    df = df.withColumn("month", month("ts"))
    df = df.withColumn("year", year("ts"))
    df = df.withColumnRenamed("ts", "start_time")

    df.createOrReplaceTempView("events_stage")
    # extract columns to create time table
    time_table = spark.sql(
        """
    SELECT start_time, hour, day, week, month, year, weekday
    FROM events_stage
    """
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.format("parquet").partitionBy("year", "month").mode(
        "overwrite"
    ).option("path", f"{output_data}/time_table.parquet").save()
    # read in song data to use for songplays table
    song_df = (
        spark.read.format("parquet")
        .option("path", f"{output_data}/songs_table.parquet")
        .option("inferSchema", "true")
        .load()
    )
    song_df.createOrreplaceTempView("songs_stage")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """
    SELECT  es.ts AS start_time,
        es.userId AS user_id,
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
