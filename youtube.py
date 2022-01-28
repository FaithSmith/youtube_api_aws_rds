import requests
import time
import pandas as pd

from auth import *

def get_video_statistics(video_id):

    #collecting view, like, dislike, comment counts
    url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id="+video_id+"&part=statistics&key="+API_KEY
    response_video_stats = requests.get(url_video_stats).json()

    view_count = response_video_stats['items'][0]['statistics']['viewCount']
    like_count = response_video_stats['items'][0]['statistics']['likeCount']
    dislike_count = response_video_stats['items'][0]['statistics']['dislikeCount']
    comment_count = response_video_stats['items'][0]['statistics']['commentCount']

    return view_count, like_count, dislike_count, comment_count

def get_videos_details(df):
    pageToken = ""
    while 1:
        url = "https://www.googleapis.com/youtube/v3/search?key="+API_KEY+"&channelId="+CHANNEL_ID+"&part=snippet,id&maxResults=50&"+pageToken

        response = requests.get(url).json()
        time.sleep(1) #give it a second before starting the for loop
        for video in response['items']:
            if video['id']['kind'] == "youtube#video":
                video_id = video['id']['videoId']
                video_title = video['snippet']['title']
                video_title = str(video_title).replace("&amp;","")
                upload_date = video['snippet']['publishedAt']
                upload_date = str(upload_date).split("T")[0]
                view_count, like_count, dislike_count, comment_count = get_video_statistics(video_id)

                df = df.append({'video_id':video_id,'video_title':video_title,
                                "upload_date":upload_date,"view_count":view_count,
                                "like_count":like_count,"dislike_count":dislike_count,
                                "comment_count":comment_count},ignore_index=True)
        try:
            if response['nextPageToken'] != None: #if none, it means it reached the last page and break out of it
                pageToken = "pageToken=" + response['nextPageToken']

        except:
            break

    return df

#build our dataframe
data = pd.DataFrame(columns=["video_id","video_title","upload_date","view_count","like_count","dislike_count","comment_count"]) 
data = get_videos_details(data)
data.to_csv('youtube_vids_2nd_pull.csv')
