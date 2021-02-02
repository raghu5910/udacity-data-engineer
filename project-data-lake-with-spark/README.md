# Data Lakes with Apache Spark and Amazon S3

## Data Lake over Data Warehouse

Data Lake is not a replacement for Data Warehouse but it is an extension of the Data Warehouse. Unlike a Data Warehouse, we don't store data in tables of a database but in file formats like csv, parquet, avro etc .,. This allows us to have following advantages.

- Schema-on-Read.
- We can have unstructured data that can be distlled later through a ETL process.
- We can store data in different formats.
- We are not conformed to a single rigid reprsentation of data.
- Allows us to cater the needs of different people. (Business Analyts, Data Analysts, Machine Learning Engineers etc .,.)
- Scalable.

### Project workflow

Sparkify is a music streaming company. The data of users activity and metadata of songs is stored in json files. In this project, our goal is to transform this data into dimension tables and fact tables based our business analysis requirement and store them as files in a Data Lake. We do this by first ingesting data from S3 source and then we do a series of transformations, after which we save the result in a Data Lake.

## Data Ingestion

We ingest data from a S3 bucket `s3://udacity-dend`. The Objects `log_data` and `event_data` contains several files in JSON format which need to be transformed to achieve our goal.

## Processing

After ingesting data from S3 bucket, we do series of tranformations over our intial data, to get desired facts and dimension tables.

#### Fact Table

1.  songplays

#### Dimension tables

1.  artists
2.  songs
3.  users
4.  time

## Storing

We sink our fact tables and Dimensional table into our Data Lake in parquet format. Before writing, we partition our data based upon specific columns to make reads and queries easier in future.

### Credits

This project is a part of Udacity's Data Engineer Nanodegree program.
