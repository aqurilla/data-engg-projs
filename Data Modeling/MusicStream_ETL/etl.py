# ETL pipeline for MusicStream database

# import prereq libraries
import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from io import StringIO
from sql_queries import *

def get_files(filepath):
    '''
    Helper function to process files
    
    Args: 
        filepath: Path to data files location
  
    Returns:
        None
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_df(cur, insert_query, df):
    '''
    Helper function to load dataframe entries to tables
    
    Args: 
        cur: cursor to database
        insert_query: sql query to execute
        df: dataframe containing data to load
  
    Returns:
        None
    '''
    for i, row in df.iterrows():
        cur.execute(insert_query, row)

def process_song_file(cur, datafile):
    '''
    Function to process song files and load
    artists and tracks tables
  
    Args:
        cur: cursor to musicstream database
        datafile: filepath to song data file
        
    Returns:
        None
    '''
    df = pd.read_json(datafile, lines=True)
    df = df.replace({pd.np.nan: None})
    
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    track_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    artist_data = artist_data.drop_duplicates()
    track_data = track_data.drop_duplicates()
    
    process_df(cur, artists_table_insert, artist_data)
    process_df(cur, tracks_table_insert, track_data)
      
    
def process_log_file(cur, datafile):
    '''
    Function to process log files and load
    timetb, users, and track_plays tables
  
    Args: 
        cur: cursor to musicstream database
        datafile: filepath to log file
  
    Returns:
        None
    '''
    df = pd.read_json(datafile, lines=True)
    df = df.replace({pd.np.nan: None})
    df = df.drop_duplicates()

    # filter records by nextsong action
    # These are the records that are relevant to us
    df = df[df['page'] == 'NextSong']

    # Fill timetb and users tables
    
    # Convert the ts timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # Extract timestamp information
    time_data = []

    for line in df['ts']:
        time_data.append([line, line.hour, line.day, line.week, line.month, line.year, line.day_name()])

    # Specify labels for these columns and set to column_labels
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)
    
    users_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    process_df(cur, timetb_table_insert, time_df)
    process_df(cur, users_table_insert, users_df)

    # Fill trackplays table
    
    # The log files dont have artist_id and track_id, so these have to be identified 
    # first using track_search query
    for ind, row in df.iterrows():
        cur.execute(track_search, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            track_id, artist_id = results
        else:
            track_id, artist_id = None, None

        # Select the timestamp, user ID, level, song ID, artist ID, session ID, location, 
        # and user agent and set to trackplay_data
        trackplay_data = (
            ind,
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            track_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent)

        cur.execute(trackplays_table_insert, trackplay_data)
    

def process_data(cur, conn, filepath, func):
    '''
    Function for processing all log files at the
    specified filepath
  
    Args: 
        cur: cursor to musicstream database
        conn: connection to musicstream database
        filepath: filepath to logs parent dir
        func: function for processing log file
  
    Returns:
        None
    '''
    # Get files
    all_files = get_files(filepath)
    print(f'{len(all_files)} files found in {filepath}')
    
    for i, datafile in enumerate(all_files):
        func(cur, datafile)
        conn.commit()
        print(f'File {i+1} processed.')
    
    
def main():
    '''
    Function implementing ETL pipeline for 
    transferring data from song and log 
    data files to the MusicStream database
    created using create_tables.py
  
    Args: 
        None 
  
    Returns:
        None
    '''
    # Connect to db and obtain cursor
    conn = psycopg2.connect("host=127.0.0.1 dbname=musicstreamdb user=student password=student")
    cur = conn.cursor()
    
    # Process song and log data files
    process_data(cur, conn, filepath = 'data/song_data', func = process_song_file)
    process_data(cur, conn, filepath = 'data/log_data', func = process_log_file)

    # Close cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
