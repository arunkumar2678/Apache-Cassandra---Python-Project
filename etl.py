# ETL Pipeline for Pre-Processing the Files

# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

# Creating list of filepaths to process original event csv data files
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(filepath,'*'))
    
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            full_data_rows_list.append(line) 
            
# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))

# Make a connection to a Cassandra instance (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()

# To Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = 
    {'class': 'SimpleStrategy', 'replication_factor' : 3 }"""
)

except Exception as e:
    print(e)

# To Set KEYSPACE
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

## Query 1:  Get artist, song title and song's length in the music app history sessionId = 338, and itemInSession = 4
dropquery = "drop table if exists music_library"
try:
    session.execute(dropquery)
    print("Table music_library dropped")
except Exception as e:
    print(e)
    
## choosing primary keys - sessionid and itemInSession columns are chosen as Primary keys because they togehther uniquely identify a record in the table. itemInSession column serves as the cluster column adn it helps in grouping the records.
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(sessionId int, itemInSession int, artist text,  length float,  song text, PRIMARY KEY (sessionId, itemInSession))"

try:
    session.execute(query)
    print("Table music_library created")
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_library (sessionId, itemInSession, artist, length, song)"
        query = query + "VALUES (%s,%s,%s,%s,%s)"
        session.execute(query,(int(line[8]), int(line[3]), line[0], float(line[5]),  line[10]))
            
## Add in the SELECT statement to verify the data was entered into the table
#sessionId = 338, and itemInSession = 4
rows = session.execute("Select artist, length, song from music_library where sessionId = 338 and itemInSession = 4 ALLOW FILTERING")
for row in rows:
    print (row[0], row[1], row[2])
           
            
## Query 2: Get name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

dropquery = "drop table if exists artist_library"
try:
    session.execute(dropquery)
    print("Table artist_library dropped")
except Exception as e:
    print(e)

# artist, itemInSession, firstName, lastName together act as the composote key to identify a unique record
#itemInSession is used as the clustering key to sort the records
query = "CREATE TABLE IF NOT EXISTS artist_library (artist text,  firstName text, lastName text, itemInSession int, "
query = query + "sessionId int, song text, userId int, PRIMARY KEY (artist, itemInSession, firstName, lastName)) WITH CLUSTERING ORDER BY (itemInSession ASC)"

try:
    session.execute(query)
    print("Table artist_library created")
except Exception as e:
    print(e)
           

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO artist_library (artist,  firstName, lastName,  itemInSession, sessionId, song, userId)"
        query = query + "VALUES (%s,%s,%s,%s,%s,%s,%s)"
        session.execute(query, (line[0],  line[1],  line[4], int(line[3]), int(line[8]), line[9], int(line[10])))
        
## Add in the SELECT statement to verify the data was entered into the table name for userid = 10, sessionid = 182
rows = session.execute("Select * from artist_library where userId = 10 and sessionId = 182 ALLOW FILTERING")
for row in rows:
    print (row[0], row[1], row[2], row[3], row[4], row[5], row[6])

## Query 3: Get all user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

dropquery = "drop table if exists user_history"
try:
    session.execute(dropquery)
    print("Table user_history dropped")
except Exception as e:
    print(e)

# song, userId together act as the composite key to identify a unique record
#userId is the partition and it will be used to pull songs and group by users


query = "CREATE TABLE IF NOT EXISTS user_history (song text, userId int, firstName text, "
query = query + "lastName text,  PRIMARY KEY (song, userId))"

try:
    session.execute(query)
    print("Table user_history created")
except Exception as e:
    print(e)
                    
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO user_history (song, userId, firstName, lastName)"
        query = query + "VALUES (%s,%s,%s,%s)"
        session.execute(query, ( line[9], int(line[10]), line[1], line[4]))
        
## Add in the SELECT statement to verify the data was entered into the table with the song "All Hands Against His Own"
rows = session.execute("Select song, firstName, lastName from user_history where song = 'All Hands Against His Own'")
for row in rows:
    print (row[0], row[1], row[2])

## Drop tables before closing out the sessions

dropMusiclibrary = "drop table music_library"
try:
    rows = session.execute(dropMusiclibrary)
    print("Table music_library dropped")
except Exception as e:
    print(e)

dropArtistlibrary = "drop table artist_library"
try:
    rows = session.execute(dropArtistlibrary)
    print("Table artist_library created")
except Exception as e:
    print(e)

dropUserHistory = "drop table user_history"
try:
    rows = session.execute(dropUserHistory)
    print("Table user_history dropped")
except Exception as e:
    print(e)

# Close the session and cluster connection
session.shutdown()
cluster.shutdown()
