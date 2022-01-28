import pymysql
from auth import *
import pandas as pd
import time
#read df already saved from running previous script
df = pd.read_csv('youtube.csv', index_col=0)

def connect_to_db():
    try:
        connection = pymysql.connect(host=ENDPOINT,user=USERNAME,\
            password=DB_PASSWORD,database=DATABASE_NAME,port=PORT)
    except pymysql.OperationalError as e:
        raise e
    else:
        cursor = connection.cursor()
        print('Connected!')

    return connection, cursor

def create_table(cursor):
    query_create_table = ("""CREATE TABLE IF NOT EXISTS videos_details
    (video_id VARCHAR(100) PRIMARY KEY,
    video_title TEXT NOT NULL ,
    upload_date DATE NOT NULL,
    view_count int NOT NULL,
    like_count int NOT NULL,
    dislike_count int NOT NULL,
    comment_count int NOT NULL 
    )
    """)
    cursor.execute(query_create_table)

def insert_row(cursor, video_id, video_title, upload_date, view_count, like_count, dislike_count, comment_count):
    query_insert_row = ("""
    INSERT INTO videos_details (video_id,video_title,upload_date,view_count
    , like_count, dislike_count,comment_count)
    VALUES (%s,%s,%s,%s,%s,%s,%s);
    """)
    values = (video_id, video_title, upload_date, view_count, like_count, dislike_count, comment_count)
    cursor.execute(query_insert_row,values)

def check_vid_not_exist(cursor,video_id):
    query_search_vid_id = ("""
    SELECT * FROM videos_details
    WHERE video_id = %s
    """)
    cursor.execute(query_search_vid_id,(video_id,))
    result = cursor.fetchone() is None
    return result

def insert_in_table(cursor):
    df2=pd.DataFrame(columns=list(df.columns))
    #iterate over all videos
    for i,row in df.iterrows():
        #for each video if it video_id doesn't exist,
        #insert row in table
        if check_vid_not_exist(cursor, row['video_id']):
            insert_row(cursor, row['video_id'], row['video_title'], row['upload_date'], row['view_count']
                          , row['like_count'], row['dislike_count'], row['comment_count'])
        else:
            df2.append(row)
    return df2

def update_row(cursor, video_id, video_title, view_count, like_count, dislike_count, comment_count):
    query_update_row = ("""UPDATE videos_details
            SET video_title = %s,
                view_count = %s,
                like_count = %s,
                dislike_count = %s,
                comment_count = %s
            WHERE video_id = %s;""")
    values = (video_title, view_count, like_count, dislike_count, comment_count, video_id)
    cursor.execute(query_update_row, values)

def update_table(cursor, df):
    for i, row in df.iterrows():
        update_row(row['video_id'], row['video_title'], row['upload_date'], row['view_count']
                          , row['like_count'], row['dislike_count'], row['comment_count'])
    
#connect to db
connection, cursor = connect_to_db()
#create table in db
create_table(cursor)
#insert values one by one into the table:
#1/if they don't exist use INSERT INTO
df2 = insert_in_table(cursor)
connection.commit()
#2/if they exist update video details
update_table(cursor, df2)
connection.commit()
#Fetch all to make sure everything worked
query_fetch_all = ("""
SELECT * FROM videos_details;
 """)
cursor.execute(query_fetch_all)
cursor.fetchall()
#DROP table because of aws charges
query_fetch_all = ("""
DROP TABLE videos_details;
 """)
cursor.execute(query_fetch_all)
connection.commit()
#close the connection
connection.close()