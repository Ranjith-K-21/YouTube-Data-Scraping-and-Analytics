# Required Packages
import googleapiclient.discovery
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine,text
import mysql.connector as sql
import numpy as np
from isodate import parse_duration 

# API service info
api_service_name = "youtube"
api_version = "v3"
api_key = "Your API Key"
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)

#Straemlit Sidebar logo (File kept in local system)
st.logo("logo.png")

# Initial section update 
if 'section' not in st.session_state:
    st.session_state.section = "Data Scraping"

# Sidebar Radio Buttons 
st.sidebar.title("Navigation")
section = st.sidebar.radio("Go to", ["Data Scraping", "Data Analytics"])
st.session_state.section = section

# Data Scraping section
if st.session_state.section == "Data Scraping":
    st.header(':red[Youtube] Data Scraping')
    channel_id = st.text_input("Enter a channel id: ")
    Start_Scaraping=st.button("Start Scraping")

    # Getting channel id as input
    if channel_id !="" and Start_Scaraping==True:
        progress_value=0
        progress_bar=st.progress(progress_value)

        # fetching channel information
        def get_channel_data(channel_id):
            data=[]
            channel_request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
            )
            response = channel_request.execute()

            snippet        = response['items'][0]['snippet']
            statistics     = response['items'][0]['statistics']
            contentDetails = response['items'][0]['contentDetails']['relatedPlaylists']

            channel_data={
                "channel_id"    : response['items'][0].get('id'),
                "channel_name"  : snippet.get('title'),
                "channel_desc"   : snippet.get('description'),
                "sub_count"     : statistics.get('subscriberCount'),
                "channel_views" : statistics.get('viewCount'),
                "playlist_id"   : contentDetails.get('uploads')
            }
            data.append(channel_data)
            channel_df = pd.DataFrame(data)
            return channel_df
        channel_info = get_channel_data(channel_id)
        progress_value+=15
        progress_bar.progress(progress_value)


        # fetching all video ids from channel
        playlist_id = channel_info["playlist_id"].iloc[0]
        def get_videos_id(playlist_id):

            videos_id=[]
            page_token=None
            
            while True:
                request = youtube.playlistItems().list(
                part       = "snippet",
                maxResults = 50,
                pageToken  = page_token,
                playlistId = playlist_id
                )
                response = request.execute()
                
                for item in response['items']:
                    videos_id.append(item['snippet']['resourceId']['videoId'])

                if "nextPageToken" in response:
                    page_token=response["nextPageToken"]
                else:
                    break

            return videos_id
        videos_id=get_videos_id(playlist_id)
        progress_value+=10
        progress_bar.progress(progress_value)


        # fectching video information
        def get_videos_info(videos_id):
            
            videos_data=[]

            for item in videos_id:
                video_request = youtube.videos().list(
                    part="contentDetails,snippet,statistics",
                    id=item
                    )
                response = video_request.execute()

                snippet        = response['items'][0]['snippet']
                statistics     = response['items'][0]['statistics']
                contentDetails = response['items'][0]['contentDetails']
                
                data = {
                    "playlist_id"   : playlist_id,
                    "video_id"      : item,
                    "video_name"    : snippet.get('title'),
                    "video_desc"    : snippet.get('description'),
                    "tags"          : snippet.get('tags'),
                    "publishedAt"   : snippet.get('publishedAt'),
                    "thumbnail"     : snippet['thumbnails']['default'].get('url'),
                    "caption"       : contentDetails.get('caption'),
                    "duration"      : contentDetails.get('duration'),
                    "view_count"    : statistics.get('viewCount'),
                    "like_count"    : statistics.get('likeCount'),
                    "fav_count"     : statistics.get('favoriteCount'),
                    "comment_count" : statistics.get('commentCount')

                }
                videos_data.append(data)
            
            videos_df = pd.DataFrame(videos_data)

            return videos_df
        videos_info = get_videos_info(videos_id)
        progress_value+=15
        progress_bar.progress(progress_value)


        # Function to check comment status
        def get_comment_status(video_id):
            request = youtube.videos().list(
                part="statistics",
                id=video_id
            )
            response = request.execute()
            comment_status = response['items'][0]['statistics'].get('commentCount',"commentsDisabled")
            if comment_status != "commentsDisabled" and comment_status != 0: 
                return comment_status
            else:
                return "commentsDisabled"

        #fetching comments data for each video
        def get_comments(videos_id):
            comments=[]
            errors=[]
            for vid_id in videos_id:
                comment_status=get_comment_status(vid_id)
                page_token = None
                if  comment_status != "commentsDisabled":   
                    try:
                        while True:
                            request = youtube.commentThreads().list(
                                part="snippet,replies",
                                videoId=vid_id,
                                maxResults=100,
                                pageToken=page_token
                                )
                            response = request.execute()
                        
                            for item in response['items']:
                                top_level_comments=item['snippet']['topLevelComment']
                                data = {
                                    
                                    "video_id"        : vid_id,
                                    "comment_id"      : top_level_comments.get('id'),
                                    "comment_text"    : top_level_comments['snippet'].get('textDisplay'),
                                    "comment_aurthor" : top_level_comments['snippet'].get('authorDisplayName'),
                                    "publishedAt"      : top_level_comments['snippet'].get('publishedAt')
                                    
                                    }
                            
                                comments.append(data)

                                if 'replies' in item:
                                        for reply in item['replies']['comments']:
                                            reply_comments = {
                                                
                                                "video_id"        : vid_id,
                                                "comment_id"      : reply.get('id'),
                                                "comment_text"    : reply['snippet'].get('textDisplay'),
                                                "comment_aurthor" : reply['snippet'].get('authorDisplayName'),
                                                "publishedAt"      : reply['snippet'].get('publishedAt') 
                                                
                                                }
                                            
                                            comments.append(reply_comments)

                            if "nextPageToken" in response:
                                page_token=response["nextPageToken"]
                            else:
                                break
                    except Exception as e:
                        error = {
                            "video_id"  :   vid_id,
                            "error!"    :   f"An Error occurd: {e}"
                            }
                        errors.append(error)
                        continue
                    
                else:
                    continue

            df_comment = pd.DataFrame(comments)
            df_error   = pd.DataFrame(errors)
            return df_comment, df_error
        comments,errors = get_comments(videos_id)
        progress_value+=15
        progress_bar.progress(progress_value)


        # cleaning data from pandas data frame (Removing duplicates,cleaning null values,format the data to correct format)
        def data_cleaning(channels_df=channel_info,videos_df=videos_info,comments_df=comments):
            channels_df.fillna({'channel_desc':"No Channel Description Available"},inplace=True)
            channels_df.drop_duplicates(subset='channel_id',inplace=True)
            channels_df.reset_index(drop=True,inplace=True)

            channels_df['channel_desc']  =  channels_df['channel_desc'].str.strip()
            channels_df['channel_id']    =  channels_df['channel_id'].str.strip()
            channels_df['channel_name']  =  channels_df['channel_name'].str.strip()
            channels_df['playlist_id']   =  channels_df['playlist_id'].str.strip()

            videos_df.fillna({

                'video_name'   :"Video has no title",
                'video_desc'   :"No Video Description Available",
                'tags'         :"No Tag Available",
                'like_count'   :0,
                'fav_count'    :0,
                'comment_count':0,
                'view_count'   :0,
                'duration'     :0
                
                },inplace=True)
            videos_df.drop_duplicates(subset='video_id',inplace=True)
            videos_df.reset_index(drop=True,inplace=True)
            videos_df['tags'] = videos_df['tags'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)

            videos_df['publishedAt'] = pd.to_datetime(videos_df['publishedAt'])
            videos_df['duration']    = videos_df['duration'].apply(parse_duration).dt.total_seconds()

            videos_df['duration']      = videos_df['duration'].astype(int)
            videos_df['view_count']    = videos_df['view_count'].astype(int)
            videos_df['like_count']    = videos_df['like_count'].astype(int)
            videos_df['fav_count']     = videos_df['fav_count'].astype(int)
            videos_df['comment_count'] = videos_df['comment_count'].astype(int)

            videos_df['playlist_id'] = videos_df['playlist_id'].str.strip()
            videos_df['video_id']    = videos_df['video_id'].str.strip()
            videos_df['video_name']  = videos_df['video_name'].str.strip()
            videos_df['video_desc']  = videos_df['video_desc'].str.strip()
            # videos_df['tags']        = videos_df['tags'].str.strip()

            comments_df.fillna({'comment_text':"No Comment Available",
                                'comment_aurthor':"Comment Aurthor Not Available",
                                'comment_aurthor':"Comment Aurthor Not Available",
                                },inplace=True)
            comments_df.drop_duplicates(subset='comment_id',inplace=True)
            comments_df.reset_index(drop=True,inplace=True)
            comments_df['publishedAt'] = pd.to_datetime(comments_df['publishedAt'])
            comments_df['comment_text']    = comments_df['comment_text'].str.strip()
            comments_df['comment_aurthor'] = comments_df['comment_aurthor'].str.strip()
            
            return channels_df,videos_df,comments_df

        channel_info,videos_info,comments=data_cleaning(
            channels_df=channel_info,
            videos_df=videos_info,
            comments_df=comments)
        progress_value+=10
        progress_bar.progress(progress_value)

        # function to connect with my sql database
        def my_sql_cnx(database_username,database_password):
            try:
                cnx = sql.connect(user=database_username,
                                password=database_password,
                                host='127.0.0.1')
                
                mycursor=cnx.cursor()

                #Database Creation - if required
                try:
                    mycursor.execute("SHOW DATABASES LIKE 'youtube'")
                    if mycursor.fetchone():
                        pass
                    else:
                        mycursor.execute("CREATE DATABASE youtube")
                except Exception as er:
                    print(f'Error occured:{er}')

                cnx_status=cnx.is_connected()

                if cnx_status == True:
                    cnx.database="youtube"
                    return cnx
                else:
                    f"An Error occured {er}"
                    return (f"An Error occured {er}")
            except Exception as er:
                return f"An Error occured {er}"
            
        # cretentials for database
        database_username = "username"
        database_password = "password"

        # Connecting to database
        cnx=my_sql_cnx(database_username,database_password)
        engine = create_engine(f"mysql+mysqlconnector://{database_username}:{database_password}@{"127.0.0.1"}/{cnx.database}")
        mycursor=cnx.cursor()
        progress_value+=10
        progress_bar.progress(progress_value)

        #Creating sql tables - if required
        def create_sql_tables():
            channels_table = "SHOW TABLES LIKE 'channel'"
            videos_table = "SHOW TABLES LIKE 'videos'"
            comments_table = "SHOW TABLES LIKE 'comments'"

            create_channels_table="""CREATE TABLE channels(channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255) NOT NULL,channel_desc TEXT,sub_count INT NOT NULL,
            channel_views INT NOT NULL,playlist_id VARCHAR(255) UNIQUE)"""

            create_videos_table="""CREATE TABLE videos(video_id VARCHAR(255) PRIMARY KEY,
            video_name VARCHAR(255) NOT NULL, video_desc TEXT, tags TEXT, publishedAt DATETIME NOT NULL,
            thumbnail VARCHAR(255), caption VARCHAR(255), duration INT NOT NULL, view_count INT NOT NULL,
            like_count INT NOT NULL, fav_count INT NOT NULL, comment_count INT NOT NULL, playlist_id VARCHAR(255) NOT NULL, 
            FOREIGN KEY (playlist_id) REFERENCES channels(playlist_id));"""

            create_comments_table="""CREATE TABLE comments(video_id VARCHAR(255), comment_id VARCHAR(255) PRIMARY KEY,
            comment_text TEXT, comment_aurthor VARCHAR(255), publishedAt DATETIME NOT NULL,FOREIGN KEY(video_id)
            REFERENCES videos (video_id))"""

            tables_list=[channels_table,videos_table,comments_table]
            table_statements=[create_channels_table,create_videos_table,create_comments_table]

            for item in tables_list:
                try:
                    item_index=tables_list.index(item)
                    mycursor.execute(item)
                    if mycursor.fetchone() == None:
                        mycursor.execute(table_statements[item_index])
                        cnx.commit()
                    else:
                        return f"An Error Occured; {er}"
                except Exception as er:
                    return f"An Error occured; {er}"
        create_sql_tables=create_sql_tables()
        progress_value+=10
        progress_bar.progress(progress_value)

        #Uploading data to MySql Database
        def upload_data_sql(channel_info,videos_info,comments):
            channel_info.to_sql("channels",engine,if_exists='append',index=False)
            videos_info.to_sql("videos",engine,if_exists='append',index=False)
            comments.to_sql("comments",engine,if_exists='append',index=False)

        try:
            update_database = upload_data_sql(channel_info,videos_info,comments)
            progress_value+=15
            progress_bar.progress(progress_value)
            st.success("Data successfully fetched and updated in the database..!")
            st.balloons()
        except Exception as er:
            st.text((f'An error occured; {er}'))
            cnx.close()
            engine.dispose()
            pass


# Data Analytics section
elif st.session_state.section == "Data Analytics":
    st.header(':red[YouTube] Data Analytics')
    user_query=st.selectbox("Select Query to run",
        ("The names of all videos along with their corresponding channel.",
         "The channels with the highest number of videos and their respective counts.",
         "The top 10 most viewed videos along with their corresponding channels.",
         "The number of comments made on each video, along with their corresponding video names.",
         "The videos with the highest number of likes, along with their corresponding channel names.",
         "The total number of likes for each video, along with their corresponding video names.",
         "The total number of views for each channel, along with their corresponding channel names.",
         "The names of all channels that have published videos in the year 2022.",
         "The average duration of all videos in each channel, along with their corresponding channel names.",
         "The videos with the highest number of comments, along with their corresponding channel names.")
         )
    run_query=st.button("Run Query")

    # Creating Sql connection
    database_username = "username"
    database_password = "password"

    def my_sql_cnx(database_username,database_password):
        try:
            cnx = sql.connect(user=database_username,
                            password=database_password,
                            host='127.0.0.1')
            
            mycursor=cnx.cursor()

            try:
                mycursor.execute("SHOW DATABASES LIKE 'youtube'")
                if mycursor.fetchone():
                    pass
                else:
                    mycursor.execute("CREATE DATABASE youtube")
            except Exception as er:
                print(f'Error occured:{er}')

            cnx_status=cnx.is_connected()

            if cnx_status == True:
                cnx.database="youtube"
                return cnx
            else:
                f"An Error occured {er}"
                return (f"An Error occured {er}")
        except Exception as er:
            return f"An Error occured {er}"
    cnx=my_sql_cnx(database_username,database_password)
    engine = create_engine(f"mysql+mysqlconnector://{database_username}:{database_password}@{"127.0.0.1"}/{cnx.database}")
    mycursor=cnx.cursor()
    conn=engine.connect()

    # Required Dictionaries 
    query_dict={
        "query_1":"The names of all videos along with their corresponding channel.",
        "query_2":"The channels with the highest number of videos and their respective counts.",
        "query_3":"The top 10 most viewed videos along with their corresponding channels.",
        "query_4":"The number of comments made on each video, along with their corresponding video names.",
        "query_5":"The videos with the highest number of likes, along with their corresponding channel names.",
        "query_6":"The total number of likes for each video, along with their corresponding video names.",
        "query_7":"The total number of views for each channel, along with their corresponding channel names.",
        "query_8":"The names of all channels that have published videos in the year 2022.",
        "query_9":"The average duration of all videos in each channel, along with their corresponding channel names.",
        "query_10":"The videos with the highest number of comments, along with their corresponding channel names."}
    query_statements={"query_1":"""SELECT v.video_name, c.channel_name FROM videos v JOIN channels c on v.playlist_id = c.playlist_id;""",
                      "query_2":"""SELECT c.channel_name, COUNT(v.video_id) AS videos_count FROM channels c JOIN videos v ON c.playlist_id = v.playlist_id GROUP BY c.channel_name ORDER BY videos_count DESC;""",
                      "query_3":"""SELECT v.video_name, c.channel_name, v.view_count FROM videos v JOIN channels c ON v.playlist_id = c.playlist_id ORDER BY v.view_count DESC LIMIT 10;""",
                      "query_4":"""SELECT v.video_name, count(c.comment_id) AS comment_count FROM videos v JOIN comments c ON v.video_id = c.video_id GROUP BY v.video_name;""",
                      "query_5":"""SELECT v.video_name, c.channel_name, v.like_count FROM videos v JOIN channels c on v.playlist_id=c.playlist_id ORDER BY v.like_count DESC LIMIT 10;""",
                      "query_6":"""SELECT v.video_name, v.like_count FROM videos v;""",
                      "query_7":"""SELECT c.channel_name, c.channel_views FROM channels c;""",
                      "query_8":"""SELECT DISTINCT c.channel_name FROM channels c JOIN videos v ON c.playlist_id = v.playlist_id WHERE YEAR(v.publishedAt) = 2022;""",
                      "query_9":"""SELECT c.channel_name, AVG(v.duration) AS avg_duration FROM channels c JOIN videos v ON c.playlist_id = v.playlist_id GROUP BY c.channel_name;""",
                      "query_10":"""SELECT v.video_name, c.channel_name, COUNT(cm.comment_id) AS num_comments FROM videos v JOIN channels c ON v.playlist_id = c.playlist_id JOIN comments cm ON v.video_id = cm.video_id GROUP BY v.video_name, c.channel_name ORDER BY num_comments DESC;"""
                      }

    #Query for streamlit output
    if user_query==query_dict['query_1'] and run_query==True:
        query=text(query_statements['query_1'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_2'] and run_query==True:
        query=text(query_statements['query_2'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_3'] and run_query==True:
        query=text(query_statements['query_3'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_4'] and run_query==True:
        query=text(query_statements['query_4'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_5'] and run_query==True:
        query=text(query_statements['query_5'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_6'] and run_query==True:
        query=text(query_statements['query_6'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_7'] and run_query==True:
        query=text(query_statements['query_7'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_8'] and run_query==True:
        query=text(query_statements['query_8'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_9'] and run_query==True:
        query=text(query_statements['query_9'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    elif user_query==query_dict['query_10'] and run_query==True:
        query=text(query_statements['query_10'])
        output_to_streamlit=conn.execute(query).fetchall()
        output_df=pd.DataFrame(output_to_streamlit)
        st.write(output_df)
        engine.dispose()
        cnx.close()
    else:
        pass




