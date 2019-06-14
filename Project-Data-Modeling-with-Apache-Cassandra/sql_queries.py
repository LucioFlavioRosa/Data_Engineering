### CREATE KEYSPACE ###
create_keyspace = ("""
CREATE KEYSPACE IF NOT EXISTS sparkifydb 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
;""")


### DROP TABLES ###

session_table_drop = "DROP TABLE IF EXISTS session_table"
user_table_drop    = "DROP TABLE IF EXISTS user_table"
song_table_drop    = "DROP TABLE IF EXISTS song_table"


### CREATE TABLES ###

# Session Table with 2 Partition Keys (sessionId, itemInSession) to include in the WHERE statement
session_table_create = ("""
CREATE TABLE IF NOT EXISTS session_table (
    sessionId int, itemInSession int, artist text, song text, length float,
    PRIMARY KEY (sessionId, itemInSession))
    ;""")

# User Table with 2 Partion Keys (userId, sessionId) to include in the WHERE statement ...
# ... and 1 Clustering Key (itemInSession) to order songs by
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    artist text, song text, itemInSession int, firstName text, lastName text, userId int, sessionId int,  
    PRIMARY KEY ((userId, sessionId), itemInSession))
    ;""")

# Song Table with 1 Partition Key (song) to include in the WHERE statement ...
# ... and 1 Clustering Key (userId) to make the Partition Key unique
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    firstName text, lastName text, userId int, song text, 
    PRIMARY KEY ((song), userId))
    ;""")


### INSERT RECORDS ###

session_table_insert = ("""
INSERT INTO session_table (
    sessionId, itemInSession, artist, song, length)
    VALUES (%s, %s, %s, %s, %s)
    ;""")

user_table_insert = ("""
INSERT INTO user_table (
    artist, song, itemInSession, firstName, lastName, userId, sessionId)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ;""")

song_table_insert = ("""
INSERT INTO song_table (
    firstName, lastName, userId, song)
    VALUES (%s, %s, %s, %s)
    ;""")


### SELECT STATEMENTS ###

session_table_select = ("""
SELECT artist, song, length FROM session_table 
    WHERE sessionId=%s AND itemInSession=%s
    ;""")

user_table_select = ("""
SELECT artist, song, itemInSession, firstName, lastName FROM user_table 
    WHERE userid=%s AND sessionid=%s
    ;""")

song_table_select = ("""
SELECT firstName, lastName, userId, song FROM song_table 
    WHERE song=%s
    ;""")