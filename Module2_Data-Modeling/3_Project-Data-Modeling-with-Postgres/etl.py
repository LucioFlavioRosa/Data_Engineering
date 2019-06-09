import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Perform ETL on song_data to create the songs and artists dimensional tables: 
    - Process a single song data file and upload to database. 
    - Extract song data + implement sql query to insert records into table.
    - Extract artist data + implement sql query to insert records into table.
    
    Parameters:
    - cur: cursor object that allows Python to execute PostgreSQL commands in a database session
    - filepath (string) : path to a song_data file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                      'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Perform ETL on log_data to create the time and users dimensional tables, 
    as well as the songplays fact table:
    - Process a single log file and load a single record into each table.
    - Extract time data + implement sql query to insert records for the timestamps into table.
    - Extract user data + implement sql query to insert records into table.
    - Extract and inserts data for songplays table from different tables by implementing a select query.
    
    Parameters:
    - cur: cursor object that allows Python to execute PostgreSQL commands in a database session
    - filepath (string) : path to a log_data file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True) 

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process all files within the filepath directory through the input function.
    
    Parameters:
    - cur: cursor object that allows Python to execute PostgreSQL commands in a database session
    - conn: connection created to the database
    - filepath (string) : path to data file
    - func: process function
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Build ETL Pipeline for Sparkify song play data:
    
    Instantiate a session to the Postgres database, 
    acquire a cursor object to process SQL queries,
    and process both the song and the log data.    
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur  = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data',  func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()