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


config = configparser.ConfigParser()
config.read("dl_template.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    return a spark session object
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read data from S3 bucket and process song data to
    song, artist dimension tables and write them as parquet
    files into a S3 bucket
    """
    song_data = input_data + "song_data/*/*/*/*"

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
    songs_table = spark.sql(
        """SELECT DISTINCT song_id, title, artist_id, year, duration
FROM songs_stage
    """
    )

    songs_table.write.format("parquet").partitionBy("year", "artist_id").option(
        "path", f"{output_data}/songs_table/songs.parquet"
    ).mode("overwrite").save()

    artists_table = spark.sql(
        """SELECT DISTINCT artist_id, artist_name, artist_location, 
        artist_latitude, artist_longitude FROM songs_stage
    """
    )

    artists_table = (
        artists_table.write.format("parquet")
        .option("path", f"{output_data}/artists_table/artists.parquet")
        .mode("overwrite")
        .save()
    )


def process_log_data(spark, input_data, output_data):
    """
    Read data from S3 bucket and process log data to
    users, time dimension tables and songplays fact table
    - write them as parquet files into a S3 bucket
    """
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
    df = spark.sql("select * from events_stage where page='NextSong'")

    users_table = spark.sql(
        """
    SELECT DISTINCT userId, firstName, gender,
                lastName, level
FROM events_stage WHERE userId IS NOT NULL"""
    )

    users_table.write.format("parquet").mode("overwrite").option(
        "path", f"{output_data}/users_table/users.parquet"
    ).save()

    df = df.withColumn("hour", hour("ts"))
    df = df.withColumn("day", dayofmonth("ts"))
    df = df.withColumn("week", weekofyear("ts"))
    df = df.withColumn("weekday", dayofweek("ts"))
    df = df.withColumn("month", month("ts"))
    df = df.withColumn("year", year("ts"))
    df = df.withColumnRenamed("ts", "start_time")

    df.createOrReplaceTempView("events_stage")
    time_table = spark.sql(
        """
    SELECT start_time, hour, day, week, month, year, weekday
    FROM events_stage
    """
    )

    time_table.write.format("parquet").partitionBy("year", "month").mode(
        "overwrite"
    ).option("path", f"{output_data}/time_table/time.parquet").save()
    song_df = (
        spark.read.format("parquet")
        .option("path", f"{output_data}/songs_table/songs.parquet")
        .option("inferSchema", "true")
        .load()
    )
    song_df.createOrReplaceTempView("songs_stage")

    songplays_table = spark.sql(
        """
        SELECT es.start_time,
        es.userId AS user_id,
        es.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        es.sessionId AS session_id, 
        es.location AS location,
        es.userAgent AS user_agent
        FROM events_stage AS es
        JOIN songs_stage AS ss
            ON (es.song = ss.title)
        """
    )
    songplays_table = songplays_table.withColumn("year", year("start_time"))
    songplays_table = songplays_table.withColumn("month", month("start_time"))

    songplays_table.write.format("parquet").partitionBy("year", "month").option(
        "path", f"{output_data}/songplays_table/songplays.parquet"
    ).save()


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://mybucket-for-gdrive/sparkify-dev"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
