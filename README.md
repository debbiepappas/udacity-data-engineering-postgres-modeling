# udacity-data-engineering-postgres-modeling
## Data Modeling with Postgres (Project 1)

The goal of this project is to convert data json files in the data/song_data and data/log_data directories into records using python pandas and then loaded into the Sparkify database. 

In a single song_data json file the following keys were given: ``` num_songs,
artist_id,
artist_latitude,
artist_longitude,
artist_location,
artist_name,
song_id,
title,
duration,
year``` 

In a single log_data json file the following keys were given:  ```artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId```

### The following steps were taken to enter data in the Sparkify Database : 

1. The **sql_queries.py** file was created to create the tables for the sparkify database , insert data from the json files , and drop the tables.  
The tables consist of the following information: 

    - **Fact Table**
    
    **songplays** - records in log data associated with song plays i.e. records with page NextSong
    
    <u>columns</u> :  *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

    - **Dimension Tables**

    **users** - users in the app
    
    <u>columns</u> : *user_id, first_name, last_name, gender, level*
    
    **songs** - songs in music database
    
    <u>columns</u> : *song_id, title, artist_id, year, duration*
    
    **artists** - artists in music database
    
    <u>columns</u> : *artist_id, name, location, lattitude, longitude*
    
    *time - timestamps of records in songplays broken down into     specific units
    
    <u>columns</u> : *start_time, hour, day, week, month, year, weekday*
    
  The *song_select query* was created to fill in the song_id and artist_id columns in the songplays table. 
  
2.  The **create_tables.py** file was run to do the following:

    - create the Sparkify database
    - connect to the Sparkify database
    - the tables created in sql_queries.py were dropped if they 
      existed to clear out the Sparkify database 
    - the tables are then created
    

3. The **etl.ipynb** notebook was created to develop the ETL process for each of the tables. The following steps were taken :  

    1. *The data in data/song_data was extracted to create the songs and artists table.* An ETL was done on a single song file and then a single record was loaded into each table. The get_files function was used to get a list of all song JSON files. *Json.load* was used to open and read a single file. A dataframe was then created.
>```song_files = get_files('data/song_data')```  
```first_song = song_files[0]```  
```with open(first_song, 'r', encoding='utf-8') as data_file:    
    data_song = json.load(data_file)```
    
    The dataframe df_song was created. 
    
    ```df_song = pd.DataFrame.from_dict(data_song,orient='index').T```    

        1. From df_song the columns and values for the *songs* table were selected. One record was selected and made into a list. The list was inserted into the songs table. 
>```columns1 = ['song_id', 'title', 'artist_id', 'year', 'duration']```  
```df_song1 = pd.DataFrame(df_song, columns=columns1)```    
```out1 = np.empty(df_song1.shape[0], dtype=object)```    
```out1[:] = df_song1.values.tolist()```     
```list_data1 = out1.tolist()```    
```flat_list1 = [item for sublist in list_data1 for item in sublist]```    
```song_data = flat_list1```  
        
        2.  From the dataframe that was created the columns and values for the *artists* table were selected. One record was selected and made into a list. The list was inserted into the artists table.
>```columns2 = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']```  
```df_song2 = pd.DataFrame(df_song, columns=columns2)```  
```df_song2.columns = ['artist_id', 'name', 'location', 'latitude', 'longitude']```  
```out2 = np.empty(df_song1.shape[0], dtype=object)```  
```out2[:] = df_song2.values.tolist()```  
```list_data2 = out2.tolist()```  
```flat_list2 = [item for sublist in list_data2 for item in sublist]```  
```artist_data = flat_list2```  
        
    2.  In the second part, an ETL is performed on the second dataset, *log_data*, to create the *time* dimensional table, the *users* dimensional table, and the *songsplay* fact table. The *get_files* function was used to get a list of all log JSON files in *data/log_data*. Then the first log file was selected in the list. The df_log dataframe was created from the first record. 
>```log_files = get_files('data/log_data')```  
```first_log = log_files[0]```  
```data = [json.loads(line) for line in open(first_log, 'r')]```  
```df_log = pd.DataFrame.from_records(data).T```  
    
        1. The data for the time table was extracted and the records were filtered by *NextSong* action. The *"ts"* timestamp column was converted to datetime. Values of *timestamp, hour, day, weekofyear, month, year, and weekday* were extracted from the *ts* column and the *time_data* list was created from these values. The record in the dataframe was inserted in the time table.
>```next_song = df_log['page'] == 'NextSong'```  
```df_next = df_log[next_song]```   
```t = pd.to_datetime(df_next['ts'],unit='ms')```   
```timestamp = t.dt.time```  
```timestamp_list = timestamp.tolist()```  
```hour = t.dt.hour```  
```hour_list = hour.tolist()```  
```day = t.dt.day```  
```day_list = day.tolist()```  
```weekofyear = t.dt.weekofyear```  
```weekofyear_list = weekofyear.tolist()```  
```month = t.dt.month```  
```month_list = month.tolist()```  
```year = t.dt.year```  
```year_list = year.tolist()```  
```weekday = t.dt.weekday```  
```weekday_list = weekday.tolist()```  
```time_data = [timestamp_list,hour_list,day_list,weekofyear_list,month_list,year_list,weekday_list]```   
```column_labels = ['start_time','hour','day','week','month','year','weekday']```  
```dic = dict(zip(column_labels,time_data))```  
```time_df = pd.DataFrame.from_dict(dic)```  
        
        2. The *user_df* dataframe was created by selecting the columns and values from the original log dataframe to match the columns for the users table. 
>```user = pd.DataFrame(df_log, columns=['userId', 'firstName', 'lastName', 'gender', 'level'])```  
```user.rename(columns = {'userId':'user_id','firstName':'first_name','lastName':'last_name'}, inplace=True)```  
```user_df = user```  

        3. Data was then extracted for the songplays table. The log file was used for this data. The *song_id* and *artist_id* was gotten by querying the songs and artists tables to find matches based on song title, artist name, and song duration time. The records were then inserted in the songplay table. 
>```for i, row in df_next.head().iterrows():```  
    ```cur.execute(song_select, (row.song, row.artist, row.length))```  
    ```results = cur.fetchone()```  
    ```if results:  
        songid, artistid = results  
        else:  
        songid, artistid = None, None```  
    ```start_time = row.ts```  
    ```user_id = row.userId```  
    ```level = row.level```  
    ```song_id = songid```  
    ```artist_id = artistid```  
    ```session_id = row.sessionId```  
    ```location = row.location```  
    ```user_agent = row.userAgent```  
    ```songplay_data = (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)```
    
        A query was run to get song_id and artist_id. 
>```song_select = "SELECT songs.song_id AS songid , artists.artist_id AS artistid```  
```FROM songs 
LEFT JOIN artists ON (songs.artist_id = artists.artist_id)```  
```WHERE (songs.title = %s AND artists.name = %s AND songs.duration = %s)"```
        
4. The ETL processes from **etl.ipynb** were used as input to etl.py to process all the files in *data/song_data* and *data/log_data* and insert the records in the Sparkify database.         
