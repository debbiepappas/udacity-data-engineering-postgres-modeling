import os
import glob
import psycopg2
import pandas as pd
import json
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    
    # open song file
    one_song = filepath
    with open(one_song, 'r', encoding='utf-8') as data_file:    
        data_song = json.load(data_file)
    df_song = pd.DataFrame.from_dict(data_song,orient='index').T

    # create columns to use in dataframe
    columns1 = ['song_id', 'title', 'artist_id', 'year', 'duration']
    df_song1 = pd.DataFrame(df_song, columns=columns1)
    
    ## create empty array and set values in it
    out1 = np.empty(df_song1.shape[0], dtype=object)
    out1[:] = df_song1.values.tolist()
    
    ## create flat list from array
    list_data1 = out1.tolist()
    flat_list1 = [item for sublist in list_data1 for item in sublist]
    song_data = flat_list1
 
    # insert song record
    cur.execute(song_table_insert, song_data)
    
    # create columns for artist table
    columns2 = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    df_song2 = pd.DataFrame(df_song, columns=columns2)
    df_song2.columns = ['artist_id', 'name', 'location', 'latitude', 'longitude']
    
    # create empty array and set values in it
    out2 = np.empty(df_song1.shape[0], dtype=object)
    out2[:] = df_song2.values.tolist()
    
    # create list from array
    list_data2 = out2.tolist()
    flat_list2 = [item for sublist in list_data2 for item in sublist]
    artist_data = flat_list2
    
    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    # open log file
    one_log = filepath
    data = [json.loads(line) for line in open(one_log, 'r')]
    df_log = pd.DataFrame.from_records(data)

    # filter by NextSong action
    next_song = df_log['page'] == 'NextSong'
    df_next = df_log[next_song]

    # convert timestamp column to datetime
    t = pd.to_datetime(df_next['ts'],unit='ms')
    
    # extract from the ts column asn set time_data to list
    timestamp = t.dt.time
    timestamp_list = timestamp.tolist()
    hour = t.dt.hour
    hour_list = hour.tolist()
    day = t.dt.day
    day_list = day.tolist()
    weekofyear = t.dt.weekofyear
    weekofyear_list = weekofyear.tolist()
    month = t.dt.month
    month_list = month.tolist()
    year = t.dt.year
    year_list = year.tolist()
    weekday = t.dt.weekday
    weekday_list = weekday.tolist()
    time_data = [timestamp_list,hour_list,day_list,weekofyear_list,month_list,year_list,weekday_list]
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    
    # create dataframe time_df
    dic = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame.from_dict(dic)
    
    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user = pd.DataFrame(df_log, columns=['userId', 'firstName', 'lastName', 'gender', 'level'])
    user.rename(columns = {'userId':'user_id','firstName':'first_name','lastName':'last_name'}, inplace=True)
    user_df = user

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for i, row in df_next.head().iterrows():


        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

    
        start_time = row.ts
        user_id = row.userId
        level = row.level
        song_id = songid
        artist_id = artistid
        session_id = row.sessionId
        location = row.location
        user_agent = row.userAgent
        
        # insert songplay record
    
        songplay_data = (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
        cur.execute(songplay_table_insert, list(songplay_data))
        # conn.commit()

def process_data(cur, conn, filepath, func):
    
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()