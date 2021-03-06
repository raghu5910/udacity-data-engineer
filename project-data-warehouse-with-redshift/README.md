## Data Warehousing with Amazon Redshift

Sparkify is a music streaming company. The data of users activity and metadata of songs is stored in json files. In this project, our goal is to transform this data into a Redshift database with a schema that emphasizes on song play analysis.

### Schema

Since our final goal is to analyze songplays, we chose schema based on **star-schema** and our schema achieves upto 2NF.
When creating tables in Redshift, an important step is to choose good `DISTKEY` and `SORTKEY` and a `DISTSTYLE` for each table (or Wherever it is necessary). We can also add additional constraints for each column, Redshift does not emphasize much on things like Referential Integrity (`Foreign Key`) and NOT NULL constraints. But these keys help query optimizer to choose a better query plan.

#### Facts Table

1.  songplays
    - `user_id` - SORTKEY, DISTKEY

#### Dimension tables

1.  artists
    - `artist_id` - `SORTKEY`
    - `DISTSTYLE ALL`
2.  songs
    - `song_id` - `SORTKEY`
    - `DISTSTYLE ALL`
3.  users
    - `user_id` - `SORTKEY`
    - `DISTSTYLE ALL`
4.  time
    - `start_time` - `SORTKEY`
    - `DISTSTYLE ALL`

#### Staging tables

We also have two additional tables to stage songs data and events data from S3 bucket. From these tables we query and insert into facts and dimensional tables.

1. events_stage
2. songs_stage

### How to Run?

- Create a Amazon Redshift Cluster
- Add necessary credentials to dwh.cfg (configuration file)
- Install Python (Above 3.6)

1. Create a python virtual environment (optional)
2. Install dependencies `$pip install -r requirements.txt`
3. Run `$python create_tables.py`. This script drops tables if already exists and creates tables based on `sql_queries.py` script.
4. Run `$python etl.py` . This script populates database with user activity data and songs metadata from Amazon S3 bucket - [S3://udacity-dend/]([s3://udacity-dend/])

### Sample Queries

- Popular browsers used by users

  `SELECT user_agent, count(*) FROM songplays GROUP BY user_agent ORDER BY user_agent;`

- Most popular songs

  `SELECT song_id, count(*) FROM songplays GROUP BY song_id ORDER BY song_id`

- Most popular artists

  `SELECT artist_id, count(*) FROM songplays GROUP BY artist_id ORDER BY artist_id`

- Number of songs listened by non-premium users in single session

  `SELECT count(*) FROM songplays WHERE level='free' GROUP BY session_id'`

### Credits

This project is a part of Udacity's Data Engineer Nanodegree program.
