import googleapiclient.discovery
import pandas as pd
import pymongo
import psycopg2
import streamlit as st

def Api_connect():
    api_key="AIzaSyC6VZlX27iHE26nGnbqdTYVtd18RxeatdI"
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
youtube = Api_connect()


#Channel_details
def get_channel_info(Channel_id):
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=Channel_id
        )
  response = request.execute()

  for i in response["items"]:
              data= dict(Channel_name= i["snippet"]["title"],
                          Channel_id=i ["id"],
                          Channel_description= i["snippet"]["description"],
                          Subscriber_count= i ["statistics"]["subscriberCount"],
                          Video_count= i ["statistics"]["videoCount"],
                          View_count=i["statistics"]["viewCount"],
                          Playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])

  return data

#get_playlist_details
def get_playlist_details(channel_id):
    playlist_details=[]

    next_page_token=None
    while True:
        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token

        )
        response=request.execute()

        for items in response["items"]:
            data=dict(
                Playlist_id=items["id"],
                Channel_id=items["snippet"]["channelId"],
                Channel_name=items["snippet"]["channelTitle"],
                Channel_title=items["snippet"]["title"],
                published=items["snippet"]["publishedAt"],
                video_count=items["contentDetails"]["itemCount"])
            playlist_details.append(data)
        next_page_token=response.get("nextPageToken")
        if next_page_token is None:
            break
    return playlist_details


#video_ids
def videos_ids(channel_id):
  video_id=[]
  response = youtube.channels().list(
      part="contentDetails",
      id=channel_id
          ).execute()

  Playlist_id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

  next_page_token=None

  while True:
    response1=youtube.playlistItems().list(
        part="snippet",
        playlistId=Playlist_id,
        maxResults=50,
        pageToken=next_page_token
        ).execute()

    for i in range(len(response1["items"])):
      video_id.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])

    next_page_token=response1.get("nextPageToken")

    if next_page_token is None:
      break  
  return(video_id)


#Videos_details
def get_video_info(video_ids):
    videos_data=[]
    for v_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=v_id
        )
        response = request.execute()

        for items in response["items"]:
            data=dict(channel_name=items["snippet"]["channelTitle"],
                    channel_id=items["snippet"]["channelId"],
                    video_id=items["id"],
                    video_title=items["snippet"]["title"],
                    Tag=items["snippet"].get("tags"),
                    thumbnails=items["snippet"]["thumbnails"]["default"]["url"],
                    description=items["snippet"]["description"],
                    published_date=items["snippet"]["publishedAt"],
                    duration=items["contentDetails"]["duration"],
                    views=items["statistics"]["viewCount"],
                    like=items["statistics"].get("likeCount"),
                    comments=items["statistics"].get("commentCount"),
                    favorite_count=items["statistics"]["favoriteCount"],
                    definition=items["contentDetails"]["definition"],
                    caption_status=items["contentDetails"]["caption"]
                    ) 
            
            videos_data.append(data)
    return videos_data


#get comment information
def get_comment_info(Videos_ids):
        Comment_Information = []
        try:
                for video_id in Videos_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response = request.execute()
                        
                        for item in response["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information

#connect with mongodb
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]

#upload to Mongodb
def Channel(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info( vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"Playlist_information":pl_details,
                      "Video_information":vi_details,"Comment_information":com_details})
    
    return "upload completed  successfully"


#connect with postgresql
#table creation for channels,playlist,videos,comments
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kishore22",
                        database="Youtube_data",
                        port="5432")

    cur=mydb.cursor()

    drop_query ="drop table if exists channels"
    cur.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_name varchar(100),
                                                            Channel_id varchar(80) primary key,
                                                            Channel_description text,
                                                            Subscriber_count bigint,
                                                            Video_count int,
                                                            View_count bigint,
                                                            Playlist_id varchar(50))'''
        cur.execute(create_query)
        mydb.commit()
        
    except:
        st.write("channels table already created")

    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_name,
                                            Channel_id,
                                            Channel_description,
                                            Subscriber_count,
                                            Video_count,
                                            View_count,
                                            Playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row["Channel_name"],
                row["Channel_id"],
                row["Channel_description"],
                row["Subscriber_count"],
                row["Video_count"],
                row["View_count"],
                row["Playlist_id"])

        try:
            cur.execute(insert_query,values)
            mydb.commit()

        except:
            st.write("channel values are already inserted")


def playlists_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kishore22",
                        database="Youtube_data",
                        port="5432")

    cur=mydb.cursor()

    drop_query ='''drop table if exists playlists'''
    cur.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists playlists(Playlist_id varchar(100) primary key,
                                                        Channel_id varchar(100),
                                                        Channel_name varchar(100),
                                                        Channel_title varchar(100),
                                                        published timestamp,
                                                        video_count int
                                                        )'''
        cur.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlist table already created")
    
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"Playlist_information":1}):
        for i in range(len(pl_data["Playlist_information"])):
            pl_list.append(pl_data["Playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_id,
                                                Channel_id,
                                                Channel_name,
                                                Channel_title,
                                                published,
                                                video_count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values=(row["Playlist_id"],
                row["Channel_id"],
                row["Channel_name"],
                row["Channel_title"],
                row["published"],
                row["video_count"]
                )
        
        try:
            cur.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("playlist values are already created")

def videos_table():
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kishore22",
                        database="Youtube_data",
                        port="5432")

        cur=mydb.cursor()

        drop_query ="drop table if exists videos"
        cur.execute(drop_query)
        mydb.commit()

        try:
                create_query='''create table if not exists videos(channel_name varchar(150),
                                                                        channel_id varchar(100),
                                                                        video_id varchar(100) primary key,
                                                                        video_title varchar(200),
                                                                        Tag	text,
                                                                        thumbnails varchar(200),
                                                                        description text,
                                                                        published_date timestamp,
                                                                        duration interval,
                                                                        views bigint,
                                                                        likes bigint,
                                                                        comments int,
                                                                        favorite_count int,
                                                                        definition varchar(50),
                                                                        caption_status varchar(50)
                                                                        )'''
                cur.execute(create_query)
                mydb.commit()
        except:
                st.write("Videos table already created")

        vi_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"Video_information":1}):
                for i in range(len(vi_data["Video_information"])):
                        vi_list.append(vi_data["Video_information"][i])
        df2=pd.DataFrame(vi_list)

        for index,row in df2.iterrows():
                insert_query='''insert into videos(channel_name,
                                                        channel_id,
                                                        Video_id,
                                                        video_title,
                                                        Tag,
                                                        thumbnails,
                                                        description,
                                                        published_date,
                                                        duration,
                                                        views,
                                                        likes,
                                                        comments,
                                                        favorite_count,
                                                        definition,
                                                        caption_status)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                values=(row["channel_name"],
                        row["channel_id"],
                        row["video_id"],
                        row["video_title"],
                        row["Tag"],
                        row["thumbnails"],
                        row["description"],
                        row["published_date"],
                        row["duration"],
                        row["views"],
                        row["like"],
                        row["comments"],
                        row["favorite_count"],
                        row["definition"],
                        row["caption_status"]
                        )
                try:
                        cur.execute(insert_query,values)
                        mydb.commit()
                except:
                        st.write("Videos values are already created")

def comments_table():
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kishore22",
                        database="Youtube_data",
                        port="5432")

        cur=mydb.cursor()

        drop_query ='''drop table if exists comments'''
        cur.execute(drop_query)
        mydb.commit()

        try:
                create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                Video_Id varchar(100),
                                                                Comment_Text text,
                                                                Comment_Author varchar(100),
                                                                Comment_Published timestamp
                                                                )'''
                cur.execute(create_query)
                mydb.commit()
        except:
                print("Comments Table already created")

        com_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"Comment_information":1}):
                for i in range(len(com_data["Comment_information"])):
                        com_list.append(com_data["Comment_information"][i])
        df3=pd.DataFrame(com_list)

        for index,row in df3.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published
                                                        )
                                                        
                                                        values(%s,%s,%s,%s,%s)'''

                values=(row["Comment_Id"],
                        row["Video_Id"],
                        row["Comment_Text"],
                        row["Comment_Author"],
                        row["Comment_Published"]
                        )
                try:
                        cur.execute(insert_query,values)
                        mydb.commit()
                except:
                        st.write("Videos values are already created")


def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Table Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"Playlist_information":1}):
        for i in range(len(pl_data["Playlist_information"])):
            pl_list.append(pl_data["Playlist_information"][i])
    df1=st.dataframe(pl_list)
    return df1

def show_videos_table():
        vi_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"Video_information":1}):
                for i in range(len(vi_data["Video_information"])):
                        vi_list.append(vi_data["Video_information"][i])
        df2=st.dataframe(vi_list)
        return df2

def show_comments_table():       
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"Comment_information":1}):
            for i in range(len(com_data["Comment_information"])):
                    com_list.append(com_data["Comment_information"][i])
    df3=st.dataframe(com_list)

    return df3


with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header(":blue[YOUTUBE_API PROJECT]")
    st.caption("Python scripting")
    st.caption("Youtube_Api")
    st.caption("Mongodb")
    st.caption("PostgreSQL")
    st.caption("Streamlit")

channel_id=st.text_input("Enter the Channel_Id :")
channels=channel_id.split(',')
channels=[ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = Channel(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()

#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="kishore22",
            database= "Youtube_data",
            port = "5432"
            )
cur = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))


if question == '1. All the videos and the Channel Name':
     query1 = "select video_title as videos,channel_name as ChannelName from videos;"
     cur.execute(query1)
     mydb.commit()
     t1=cur.fetchall()
     st.write(pd.DataFrame(t1,columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
     query2 = "select channel_name as ChannelName, video_count as N0_Videos from channels order by video_count desc;"
     cur.execute(query2)
     mydb.commit()
     t2=cur.fetchall()
     st.write(pd.DataFrame(t2,columns=["Channel Name","NO of Videos"]))

elif question == '3. 10 most viewed videos':
     query3="select views as MostView, video_title as VideosName,channel_name as ChannelName from videos order by views desc limit 10;"
     cur.execute(query3)
     mydb.commit()
     t3=cur.fetchall()
     st.write(pd.DataFrame(t3,columns=["Views","Video Title","Channel Name"]))

elif question == '4. Comments in each video':
     query4="select comments as Comments,video_title as VideoName,channel_name as ChannelName from videos order by comments desc;"
     cur.execute(query4)
     mydb.commit()
     t4=cur.fetchall()
     st.write(pd.DataFrame(t4,columns=["Comments","Video Title","Channel Name"]))

elif question =='5. Videos with highest likes':
     query5="select likes as MostLikes,video_title as VideoTitle, channel_name as ChannelName from videos order by likes desc;"
     cur.execute(query5)
     mydb.commit()
     t5=cur.fetchall()
     st.write(pd.DataFrame(t5,columns=["Most Likes","Video Name","Channel Name"]))

elif question =='6. likes of all videos':
     query6="select likes as likeCount,video_title as VideoName,channel_name as ChannelName from videos;"
     cur.execute(query6)
     mydb.commit()
     t6=cur.fetchall()
     st.write(pd.DataFrame(t6,columns=["Like Count","Video Name","Channel_Name"]))
    
elif question =='7. views of each channel':
     query7="select view_count as ViewsCount, channel_name as ChannelName from channels;"
     cur.execute(query7)
     mydb.commit()
     t7=cur.fetchall()
     st.write(pd.DataFrame(t7,columns=["View Count","Channel Name"]))
    
elif question =='8. videos published in the year 2022':
     query8='''select published_date as VideoReleased, 
                video_title as VideoTitle, 
                channel_name as ChannelName from videos 
                where extract(year from published_date)=2022;'''
     cur.execute(query8)
     mydb.commit()
     t8=cur.fetchall()
     st.write(pd.DataFrame(t8,columns=["Video Published","Video Title","Channel Name"]))

elif question =='9. average duration of all videos in each channel':
    query9="select channel_name as ChannelName, AVG(duration) as AverageDuration from videos GROUP BY channel_name;"
    cur.execute(query9)
    mydb.commit()
    t9=cur.fetchall()
    t9=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])
    T9=[]
    for item,rows in t9.iterrows():
        channel_title=rows["Channel Name"]
        average_duration=rows["Average Duration"]
        average_duration_str=str(average_duration)
        T9.append({"Channel Name":channel_title,"Average Duration":average_duration_str})
    st.write(pd.DataFrame(T9))

elif question =='10. videos with highest number of comments':
    query10='''select comments as HighestComment,
            video_title as VideoTitle,
            channel_name as ChannelName from videos 
            where comments is not null order by comments desc'''
    cur.execute(query10)
    mydb.commit()
    t10=cur.fetchall()
    st.write(pd.DataFrame(t10,columns=["Highest Comment Count","Video Title","Channel Name"]))

