#!/usr/bin/env python
# coding: utf-8

# # Project out line

# ![image.png](attachment:image.png)

# In[179]:


pip install spotipy


# In[232]:


import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd , numpy as np


# In[181]:


client_credentials_manager = SpotifyClientCredentials(client_id='cdee7bb17efe4e8eab13bbf11aa0ee4b',client_secret='97bd5dc6e7a4415bb59caf8b620e4cf5')


# In[182]:


sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)


# In[183]:


playlist_link="https://open.spotify.com/playlist/7FvCHKzqYNtNCNdZ83bFYm"


# In[184]:


playlist_URI=playlist_link.split("/")[-1].split('?')[0]



# In[185]:


data = sp.playlist_tracks(playlist_URI)
data


# In[186]:


data['items'][0]['track']['album']['id']


# In[187]:


data['items'][0]['track']['album']['name']


# In[188]:


data['items'][0]['track']['album']['release_date']


# In[189]:


data['items'][0]['track']['album']['total_tracks']


# In[190]:


data['items'][0]['track']['album']['external_urls']['spotify']


# In[191]:


#extracting useful album info from data

album_list=[]
for row in data['items']:
    album_id=row['track']['album']['id']
    album_name=row['track']['album']['name']
    album_release_date=row['track']['album']['release_date']
    album_total_tracks=row['track']['album']['total_tracks']
    album_urls=row['track']['album']['external_urls']['spotify']
    album_elements={'album_id':album_id,'name':album_name,'release_date':album_release_date,
                    'total_tracks':album_total_tracks,'urls':album_urls}
    album_list.append(album_elements)

    


# In[192]:


album_list


# In[200]:


data["items"][2]['track']['artists']
data['items'][0]


# In[210]:


#extracting useful artist info from data
artist_list=[]
for row in data['items']:
    for key,value in row.items():
        if key=='track':
            for artist in value['artists']:
                artist_dict= {'artist_id':artist['id'],'artist_name':artist['name'],'external_urls':artist['href']}
                artist_list.append(artist_dict)
                


# In[211]:


artist_list


# In[215]:


data['items'][0]['track']


# In[246]:


#extracting useful song info from data
song_list=[]
for row in data['items']:
    song_id=row['track']['id']
    song_name=row['track']['name']
    song_duration=row['track']['duration_ms']
    song_url=row['track']['external_urls']
    song_popularity=row['track']['popularity']
    song_added=row['added_at']
    album_id=row['track']['album']['id']
    artist_id=row['track']['artists'][0]['id']
    song_elements={'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                  'popularity':song_popularity,'song_added':song_added,'album_id':album_id,'artist_id':artist_id}
    song_list.append(song_elements)


# In[250]:


#convertion of the extacted json data into data frame 
album_df=pd.DataFrame.from_dict(album_list)
artist_df=pd.DataFrame.from_dict(artist_list)
song_df=pd.DataFrame.from_dict(song_list)


# In[251]:


song_df


# In[253]:


#droping duplicates in case of any duplicates
album_df=album_df.drop_duplicates(subset=['album_id'])
artist_df=artist_df.drop_duplicates(subset=['artist_id'])
song_df=song_df.drop_duplicates(subset=['song_id'])


# In[255]:


song_df.head()


# In[256]:


album_df.head()


# In[257]:


artist_df.head()


# In[260]:


#transformation of release data into to_datetime format
album_df['release_date']=pd.to_datetime(album_df['release_date'])
song_df['song_added']=pd.to_datetime(song_df['song_added'])


# In[259]:


album_df.info()


# In[261]:


song_df.info()


# In[ ]:




