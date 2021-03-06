## Data Modeling with Postgres

Sparkify is a music streaming company. The data of users activity and metadata of songs is stored in json files. In this project, our goal is to transform this data into a database with a schema that emphasizes on song play analysis.

### Schema

Since our final goal is to analyze songplays, we chose schema based on **star-schema** and our schema achieves upto 2NF.

#### Facts Table

1.  songplays

#### Dimension tables

1.  artists
2.  songs
3.  users
4.  time

### How to Run?

- Insall Postgres
- Install Python (Above 3.6)

1. Create a python virtual environment (optional)
2. Install dependencies `$pip install -r requirements.txt`
3. Run `$python create_tables.py`. This script creates a database named `sparkify` (also removes sparkify database if exists) and tables based on `sql_queries.py` script.
4. Run `$python etl.py` . This script populates database with user activity data and songs metadata. A stet-by-step approach is implemented in `etl.ipynb`.

### Tests

- Run `jupyter notebook` and run each cell in `tests.ipynb`
  - Each code cell tests 1 table in database.

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
