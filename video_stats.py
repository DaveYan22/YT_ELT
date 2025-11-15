import requests
import json

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
API_KEY = os.getenv("API_KEY") 
CHANNEL_HANDLE = 'MrBeast'
maxResults = 50
playlistId = 'UUX6OQ3DkcsbYNE6H8uQQuVA'

def get_playlist_id(channel_handle):
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={API_KEY}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        #print(json.dumps(data, indent=4))
        channel_item = data['items'][0]
        channel_playlistId = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        print(channel_playlistId)
        return channel_playlistId
        
    except requests.exceptions.RequestException as e:
        raise e
    


def get_video_ids(playlistId):

    video_ids = []
    pageToken = None
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}'
    try:
        while True:
            url = base_url
            if pageToken:
                url += f'&pageToken={pageToken}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                video_ids.append(item['contentDetails']['videoId'])
                video_ids
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e


if __name__ == "__main__":
    get_playlist_id(CHANNEL_HANDLE)  # Pass the CHANNEL_HANDLE argument
    print(get_video_ids(playlistId))