import requests
import json

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
API_KEY = os.getenv("API_KEY") 
CHANNEL_HANDLE = 'MrBeast'

def get_player_id(channel_handle):
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
    
if __name__ == "__main__":
    get_player_id(CHANNEL_HANDLE)  # Pass the CHANNEL_HANDLE argument