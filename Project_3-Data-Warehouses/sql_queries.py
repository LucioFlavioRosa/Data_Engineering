# A. CONFIG

import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')


# B. DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS song"
artist_table_drop         = "DROP TABLE IF EXISTS artist"
time_table_drop           = "DROP TABLE IF EXISTS time"


# C. CREATE TABLES
## C1. Create Staging Tables from S3

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER,
    last_name       VARCHAR,
    length          DECIMAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    DECIMAL,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    user_agent      VARCHAR,
    user_id         INTEGER
    )
    ;""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs ( 
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL,
    year             INTEGER
    )
    ;""")
  
## C2. Create OLAP Tables
## Star Schema: 1 Fact Table (songplays), 4 Dimension Tables (users, songs, artists, time)
    
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id  INTEGER  IDENTITY(0,1)  NOT NULL  PRIMARY KEY, 
    start_time   BIGINT                  NOT NULL, 
    user_id      INTEGER                 NOT NULL,
    level        VARCHAR                 NOT NULL, 
    song_id      VARCHAR                 NOT NULL, 
    artist_id    VARCHAR                 NOT NULL, 
    session_id   INTEGER, 
    location     VARCHAR,
    user_agent   VARCHAR)
    ;""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER  NOT NULL  PRIMARY KEY, 
    first_name  VARCHAR, 
    last_name   VARCHAR, 
    gender      VARCHAR,
    level       VARCHAR)
    ;""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id    VARCHAR  NOT NULL  PRIMARY KEY, 
    title      VARCHAR, 
    artist_id  VARCHAR,
    year       INTEGER, 
    duration   DECIMAL)
    ;""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id  VARCHAR  NOT NULL  PRIMARY KEY,
    name       VARCHAR, 
    location   VARCHAR, 
    latitude   DECIMAL, 
    longitude  DECIMAL) 
    ;""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT  NOT NULL  PRIMARY KEY,
    hour       INTEGER NOT NULL, 
    day        INTEGER NOT NULL, 
    week       INTEGER NOT NULL, 
    month      INTEGER NOT NULL, 
    year       INTEGER NOT NULL, 
    weekday    INTEGER NOT NULL) 
    ;""")


# D. STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    IAM_ROLE {}
    JSON {}
    ;""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    JSON 'auto'
    ;""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# E. FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT (events.start_time, events.user_id, events.level, songs.song_id, songs.artist_id, events.session_id,
    events.location, events.user_agent)
    FROM staging_events AS events
    LEFT JOIN staging_songs AS songs ON events.song = songs.title AND events.artist = songs.artist_name
    WHERE events.page = 'NextSong'
    ;""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT (DISTINCT user_id, first_name, last_name, gender, level)
    FROM staging_events
    WHERE page = 'NextSong'
    ;""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT (DISTINCT song_id, title, artist_id, year, duration)
    FROM staging_songs
    ;""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT (DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    FROM staging_songs
    ;""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT (DISTINCT start_time, EXTRACT(hour from start_time), EXTRACT(day from start_time), EXTRACT(week from start_time), 
            EXTRACT(month from start_time), EXTRACT(year from start_time), EXTRACT(weekday from start_time))
    FROM staging_events
    WHERE page = 'NextSong'
    ;""")


# F. QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, 
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, 
                        user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries   = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]