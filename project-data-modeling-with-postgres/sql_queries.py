# DROP TABLES

songplay_table_drop = " DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGSERIAL PRIMARY KEY,
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
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR NOT NULL
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INT, 
    duration NUMERIC NOT NULL 
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR,
    artist_location VARCHAR,
    artist_latitude numeric,
    artist_longitude numeric
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    time_id BIGSERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays (
    start_time, user_id, level, 
    song_id, artist_id, session_id, 
    location, user_agent) VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s) 
"""

user_table_insert = """
INSERT INTO users (
    user_id, first_name, last_name,
    gender, level) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET
    (first_name, last_name, gender, level)=
    (EXCLUDED.first_name, EXCLUDED.last_name,
    EXCLUDED.gender, EXCLUDED.level)
"""

song_table_insert = """
INSERT INTO songs (
    song_id, title, artist_id, year, duration
    ) VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (song_id) DO NOTHING
"""

artist_table_insert = """
INSERT INTO artists (
    artist_id, artist_name, artist_location,
    artist_latitude, artist_longitude) VALUES
    (%s, %s, %s, %s, %s) ON CONFLICT (artist_id)
    DO NOTHING
"""


time_table_insert = """
INSERT INTO time (
    start_time, hour, day, week,
    month, year, weekday) VALUES
    (%s, %s, %s, %s, %s, %s, %s)
"""

# FIND SONGS

song_select = """
SELECT songs.song_id, artists.artist_id FROM 
songs JOIN artists ON songs.artist_id=artists.artist_id 
WHERE (songs.title=%s AND artists.artist_name=%s AND duration=%s)
"""

# QUERY LISTS

create_table_queries = [
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
