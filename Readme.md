# Project: Data Modeling with Cassandra
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions by creating a database for the analysis..

# Project Overview
In this project we will need to model our data by creating tables in Apache Cassandra to run queries, build an ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

## Tables - fields and data types

|Table music_library| Data type | Is Null | Description|
|-------------|-----------|----------|-----------|
|artist| text | not null |Name of the artist|
|itemInSession  | float | null | itemSession of the song - Partition key column|
|length | float | null | Length of the song|
|sessionId | float | null | Session of the song -Primary key column|
|song | float | null | Name of the song|

|Table artist_library| Data type | Is Null | Description|
|-------------|-----------|----------|-----------|
|artist| text | not null | Name of the artist(Composite primary key)|
|firstName | text | not null | First name of the user - partition key column|
|itemInSession  | float | null | itemSession of the song - cluster column|
|lastName | float | null | Last name of the user - partition key column|
|sessionId | float | null | Session of the song|
|song | float | null | Name of the song|
|userId | float | null | as a primary key since it will uniquely identify the user)|

|Table user_history| Data type | Is Null | Description|
|-------------|-----------|----------|-----------|
|firstName | text | not null | First name of the user - partition key column |
|lastName | float | null | Last nameof the user|
|song | float | null | Name of the song - Composite partition key |
|userId | float | null | Composite partition key)|


etl.py - ETLpipeline file has the ETL process to load all the fact and dimension tables

eventdatefile_new.xlsx - excel file containing all the data of all the event data files 

<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
## Some websites referred for help

[w3 school](www.w3schools.com)

