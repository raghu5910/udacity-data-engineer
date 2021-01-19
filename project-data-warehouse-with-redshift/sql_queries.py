import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_stage"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stage"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS events_stage (
    artist_name VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    last_name VARCHAR,
    item_in_session INT,
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    session_id INT,
    song_title VARCHAR,
    status INT,
    ts TIMESTAMP SORTKEY,
    user_agent VARCHAR,
    user_id INT DISTKEY)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS songs_stage (
    num_songs INT,
    artist_id VARCHAR NOT NULL DISTKEY,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR PRIMARY KEY,
    song_title VARCHAR,
    duration NUMERIC,
    year INT SORTKEY
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL REFERENCES users(user_id) SORTKEY DISTKEY,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL REFERENCES songs(song_id),
    artist_id VARCHAR  NOT NULL REFERENCES artists(artist_id),
    session_id VARCHAR NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR PRIMARY KEY SORTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
) DISTSTYLE ALL
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL ,
    year INT, 
    duration NUMERIC NOT NULL 
) DISTSTYLE ALL
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    artist_name VARCHAR,
    artist_location VARCHAR,
    artist_latitude numeric,
    artist_longitude numeric
) DISTSTYLE ALL
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    time_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
) DISTSTYLE ALL
"""

# STAGING TABLES

staging_events_copy = (
    """
COPY events_stage FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON {}
REGION 'us-west-2';
"""
).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (
    """
COPY songs_stage FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
"""
).format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id,
                        artist_id,session_id, location,user_agent)
SELECT  es.ts AS start_time,
        es.user_id AS user_id,
        es.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        es.session_id AS session_id,
        es.location AS location,
        es.user_agent AS user_agent
FROM events_stage AS es
JOIN songs_stage AS ss
    ON (es.artist = ss.artist_name)
WHERE es.page = 'NextSong';
"""

user_table_insert = """
INSERT INTO users (user_id,first_name, gender,
                    last_name, level)
SELECT DISTINCT user_id, first_name, gender,
                last_name, level
FROM events_stage;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM songs_stage
"""

artist_table_insert = """
INSERT INTO artists (artist_id, artist_name, artist_location,
                    artist_latitude, artist_longitude)
SELECT artist_id, artist_name, artist_location, 
        artist_latitude, artist_longitude
FROM songs_stage
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week,
                    month, year, weekday)
SELECT  start_time,
        DATE_TRUNC('h',start_time),
        DATE_TRUNC('doy',start_time),
        DATE_TRUNC('w',start_time),
        DATE_TRUNC('mon',start_time),
        DATE_TRUNC('y',start_time),
        DATE_TRUNC('dow',start_time),
FROM songplays
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
