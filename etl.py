import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime
import numpy as np
pd.options.display.float_format = '{:.0f}'.format

def process_song_file(cur, filepath):
    """
    The function loads songs JSON files into the songs table
    
    parameters:
        - the cursor of connection to the database
        - the filepath JSON song files
    output : the data from JSON file are loaded into the songs table    
    """
    
    # open song file
    data = pd.read_json(filepath, lines = True)
    song_df = pd.DataFrame(data)

    # insert song record
    song_data = song_df[['song_id','title','artist_id','year','duration']]
    song_data = song_data.values.tolist()

    for row in song_data:
        cur.execute(song_table_insert, row)
    
    # insert artist record
    artist_data = song_df[['artist_id' ,'artist_name' , 'artist_location' , \
    'artist_latitude' ,'artist_longitude' ]]
    artist_data = artist_data.values.tolist()

    for row in artist_data:
        cur.execute(artist_table_insert, row)

def process_log_file(cur, filepath):
    """
    The function loads JSON log files into the tables : time,users and 
    songplays.
    
    parameters:
        - the cursor of connection to the database
        - the filepath of the JSON log file.
    
    processing steps:
        - entries from log file are filtered by the criteria page = 'NextSong'
        - timestamp data is broken down into : 
            day, hour, month, week, year, weekday before insertion into 
            dimension time table.
        - data related to users are loaded into the users table
        - data related to song plays are loaded into the songplays table. 
          artist_id and song_id are retrieved based on sql 'song_select'.
    output :         
        the data from JSON file are loaded into the table    
    """
        
    # open log file
    log_data = pd.read_json(filepath, lines = True)
    log_data_df = pd.DataFrame(log_data) 

    # filter by NextSong action
    log_data_df = log_data_df[log_data_df.page == 'NextSong']

    # convert timestamp column to datetime
    log_data_df['datetime'] = pd.to_datetime(log_data_df['ts'],unit = 'ms')
    ts_df = log_data_df[['ts','datetime']].drop_duplicates()

    ts_df['timestamp'] = ts_df['datetime'].astype('int64')/1000000
    ts_df['hour'] = ts_df['datetime'].dt.hour
    ts_df['day'] = ts_df['datetime'].dt.day
    ts_df['week'] = ts_df['datetime'].dt.week
    ts_df['month'] = ts_df['datetime'].dt.month
    ts_df['year'] = ts_df['datetime'].dt.year
    ts_df['weekday'] = ts_df['datetime'].dt.weekday


    ts_df = ts_df[['timestamp','hour','day','week','month','year','weekday']]
    
    # insert time data records

    time_data = ts_df[['timestamp','hour','day','week','month','year', \
    'weekday']].values.tolist()
    time_data = np.array(time_data)
    time_data = time_data.transpose()
    time_data = time_data.astype('int64')
    column_labels = ['start_time','hour','day','week','month','year', \
    'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = log_data_df[['userId' ,'firstName' ,'firstName' ,'gender' , \
    'level' ]]


    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))


    # insert songplay records

    songplay_df = log_data_df[['ts' ,'userId' ,'level' ,'sessionId' , \
    'location','userAgent','song','artist','length' ]]

    for index, row in songplay_df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts,row.userId, row.level,songid, artistid, \
        row.sessionId,row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

    


def process_data(cur, conn, filepath, func):
    """
    The function gets all the JSON files from filepath and calls a 
    function set in parameter to load data into database.
    
    parameters:
        - the filepath
        - connection,cursor for database connection
        - the function that loads JSON files into database.
    output :
        - The function iterates over each JSON file found.
        - It calls the function set in argument to load the data into tables.
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
    The main function connects to the database and calls other functions
    in the module to load JSON files into the database.
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student \
    password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()