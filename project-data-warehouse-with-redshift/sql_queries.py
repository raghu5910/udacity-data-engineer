import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_stage"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stage"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
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
""")

staging_songs_table_create = ("""
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
""")

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL REFERENCES users(user_id),
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
    level VARCHAR NOT NULL DISTKEY
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL DISTKEY ,
    year INT, 
    duration NUMERIC NOT NULL 
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY DISTKEY SORTKEY,
    artist_name VARCHAR,
    artist_location VARCHAR,
    artist_latitude numeric,
    artist_longitude numeric
) DISTSTYLE
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    time_id BIGSERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL DISTKEY,
    weekday INT NOT NULL
)
"""

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
