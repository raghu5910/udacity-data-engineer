## Data Modeling with Postgres

Sparkify is a music streaming company. The data of users activity and metadata of songs is stored in json files. In this project, our goal is to transform this data into a database with a schema that emphasizes on song play analysis.

### How to Run?

- Insall Postgres
- Install Python (Above 3.6)

1. Create a python virtual environment (optional)
2. Install dependencies `$pip install -r requirements.txt`
3. Run `$python create_tables.py`. This script creates a database named `sparkify` (also removes sparkify database if exists) and tables based on `sql_queries.py` script.
4. Run `$python etl.py` . This script populates database with user activity data and songs metadata. A stet-by-step approach is implemented in `etl.ipynb`.

### Sample Queries

- Popular browsers used by users

  `select user_agent, count(*) from songplays group by user_agent order by user_agent;`

- Most popular songs

  `select song_id, count(*) from songplays group by song_id order by song_id`

- Most popular artists

  `select artist_id, count(*) from songplays group by artist_id order by artist_id`

- Number of songs listened by non-premium users in single session

  `select count(*) from songplays where level='free' groupby session_id'`
