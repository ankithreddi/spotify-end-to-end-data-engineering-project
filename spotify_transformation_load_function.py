import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

# Extract album details
def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_urls = row['track']['album']['external_urls']['spotify']
        album_elements = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                          'total_tracks': album_total_tracks, 'urls': album_urls}
        album_list.append(album_elements)
    return album_list

# Extract artist details
def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'external_urls': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list

# Extract song details
def song(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['artists'][0]['id']
        song_elements = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
                         'popularity': song_popularity, 'song_added': song_added, 'album_id': album_id, 'artist_id': artist_id}
        song_list.append(song_elements)
    return song_list

# Lambda function handler
def lambda_handler(event, context):
    s3_client = boto3.client('s3')  # a1 Changed to s3_client to differentiate between boto3.client and boto3.resource
    s3_resource = boto3.resource('s3')  # a1 Added s3_resource for using resource-specific operations
    Bucket = 'spotify-etl-project-ankith'
    Key = 'raw_data/to_processed/'

    spotify_data = []
    spotify_keys = []

    # Process files in the S3 bucket
    for file in s3_client.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3_client.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)

        # Create and process album DataFrame
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])

        # Create and process artist DataFrame
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])

        # Create and process song DataFrame
        song_df = pd.DataFrame.from_dict(song_list)
        song_df = song_df.drop_duplicates(subset=['song_id'])

        # Convert dates to datetime format
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])

        # Save transformed song data to S3
        song_key = f'transformed_data/songs_data/songs_transformed_{datetime.now()}.csv'  # a1 Updated key format to include timestamp
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)  # a1 Added index=False to avoid adding an index column
        song_content = song_buffer.getvalue()
        s3_client.put_object(Bucket=Bucket, Key=song_key, Body=song_content)

        # Save transformed album data to S3
        album_key = f'transformed_data/album_data/album_transformed_{datetime.now()}.csv'  # a1 Updated key format to include timestamp
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)  # a1 Added index=False to avoid adding an index column
        album_content = album_buffer.getvalue()
        s3_client.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        # Save transformed artist data to S3
        artist_key = f'transformed_data/artist_data/artist_transformed_{datetime.now()}.csv'  # a1 Updated key format to include timestamp
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)  # a1 Added index=False to avoid adding an index column
        artist_content = artist_buffer.getvalue()
        s3_client.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    # Move processed files to the "processed" folder
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        processed_key = f'raw_data/processed/{key.split("/")[-1]}'  # a1 Fixed key format to move files to the processed folder
        s3_resource.meta.client.copy(copy_source, Bucket, processed_key)  # a1 Used s3_resource.meta.client to perform copy operation
        s3_resource.Object(Bucket, key).delete()  # a1 Used s3_resource for deleting the original file