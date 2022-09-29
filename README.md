# AWS-data-lakes-with-spark

# Project: Spark and Data Lake

This project builds an ETL pipeline for a data lake. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app. We loaded data from S3, process the data into analytics tables using Spark, and load them back into S3.
Project Structure

Spark and Data Lake
|____etl.py              # ETL builder
|____dl.cfg              # AWS configuration file
|____test.ipynb          # testing

# How to Run

    Add AWS credentials and S3 target bucket endpoint in dl.cfg

    Please do not put your access/secret key in public

    [AWS]
    AWS_ACCESS_KEY_ID = [your access key]
    AWS_SECRET_ACCESS_KEY = [your secret key]

    [S3]
    SOURCE_S3_BUCKET = s3a://udacity-dend/
    DEST_S3_BUCKET = [your S3 endpoint]

    In the terminal, install pyspark and run etl.py python script

    pip install pyspark
    python3 etl.py

    Test and check the results by directly running test.ipynb jupyter notebook

# ELT Pipeline
etl.py

ELT pipeline builder

    process_song_data
        Load raw data from S3 buckets to Spark stonealone server and process song dataset to insert record into songs and artists dimension table

    process_log_data
        Load raw data from S3 buckets to Spark stonealone server and Process event(log) dataset to insert record into time and users dimensio table and songplays fact table

# Database Schema
