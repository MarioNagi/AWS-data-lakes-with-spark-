# AWS-data-lakes-with-spark

# Project: Spark and Data Lake

This project builds an ETL pipeline for a data lake. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app. We loaded data from S3, process the data into analytics tables using Spark, and load them back into S3.
Project Structure

Spark and Data Lake

sparkify.ipynb              # ETL builder
dl.cfg                      # AWS configuration file


# How to Run

    Add AWS credentials in dl.cfg

    Please do not put your access/secret key in public

    [AWS]
    AWS_ACCESS_KEY_ID = [your access key]
    AWS_SECRET_ACCESS_KEY = [your secret key]
    
    first, you run EMR cluster (using 1 Master node and 2 Core nodes), then run Pyspark notebook via JupyterLab and the following notebook


# ELT Pipeline
sparkify.ipynb

ELT pipeline builder

    process_song_data
        Load raw data from S3 buckets to Spark stonealone server and process song dataset to insert record into songs and artists dimension table

    process_log_data
        Load raw data from S3 buckets to Spark stonealone server and Process event(log) dataset to insert record into time and users dimensio table and songplays fact table

# Database Schema
