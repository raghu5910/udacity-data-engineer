import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):

    # read from json file
    df = pd.read_json(filepath, lines=True)
    df = df.where(pd.notnull(df), None)

    # insert song record
    song_data = df.loc[
        0, ["song_id", "title", "artist_id", "year", "duration"]
    ].to_list()

    # psycopg2 has trouble taking numpy type as input values
    # convert to native types
    if song_data[3] != None:
        song_data[3] = float(song_data[3])
    if song_data[4] != None:
        song_data[4] = float(song_data[4])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df.loc[
        0,
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ],
    ].to_list()
    if artist_data[3] != None:
        artist_data[3] = float(artist_data[3])
    if artist_data[4] != None:
        artist_data[4] = float(artist_data[4])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = df.where(pd.notnull(df), None)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    df["hour"] = pd.DatetimeIndex(df["ts"]).hour
    df["day"] = pd.DatetimeIndex(df["ts"]).day
    df["week-of-year"] = pd.DatetimeIndex(df["ts"]).isocalendar().week.to_list()
    df["month"] = pd.DatetimeIndex(df["ts"]).month
    df["year"] = pd.DatetimeIndex(df["ts"]).year
    df["weekday"] = pd.DatetimeIndex(df["ts"]).weekday
    time_df = df.loc[
        :, ["ts", "hour", "day", "week-of-year", "month", "year", "weekday"]
    ]

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Bad input values")
            print(e)

    # load user table
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Bad input values")
            print(e)
    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, float(row.length)))
        except psycopg2.Error as e:
            print("Bad input values")
            print(e)
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Check values to be inserted")
            print(e)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()