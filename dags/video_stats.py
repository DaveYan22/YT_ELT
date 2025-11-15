import requests
import json
from datetime import date
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
API_KEY = os.getenv("API_KEY") 
CHANNEL_HANDLE = 'MrBeast'
maxResults = 50
playlistId = 'UUX6OQ3DkcsbYNE6H8uQQuVA'
videoId = '0e3GPea1Tyg'

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



def batch_list(videeo_id_list, batch_size):
    for video_id in range(0, len(videeo_id_list), batch_size):
        yield videeo_id_list[video_id:video_id + batch_size]


def extract_video_data(video_ids):
    extracted_data = []
    def batch_list(videeo_id_list, batch_size):
        for video_id in range(0, len(videeo_id_list), batch_size):
            yield videeo_id_list[video_id:video_id + batch_size]

    base_url = "https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id=0e3GPea1Tyg&key=[YOUR_API_KEY]"

    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)
            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={videoId}&key={API_KEY}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
                video_data = {
                    'video_id': video_id,
                    'title': snippet['title'],
                    'publishedAt': snippet['publishedAt'],
                    'duration': contentDetails['duration'],
                    'viewCount': statistics.get('viewCount', None),
                    'likeCount': statistics.get('likeCount', None),
                    'commentCount': statistics.get('commentCount', None)
                }

                
                extracted_data.append(video_data)
        return extracted_data   
    except requests.exceptions.RequestException as e:
        raise e

def save_to_json(extracted_data):
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)

    file_name = f'video_data_{date.today()}.json'
    file_path = os.path.join(data_dir, file_name)
    # --- END MODIFICATION ---
    
    try:
        with open(file_path, 'w', encoding='utf-8') as json_outfile:
            json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
        print(f"Data saved to {file_path}")
    except IOError as e:
        print(f"Error saving data to {file_path}: {e}")


if __name__ == "__main__":
    try:
        uploads_playlist_id = get_playlist_id(CHANNEL_HANDLE) 
        video_ids = get_video_ids(uploads_playlist_id) 
        video_data = extract_video_data(video_ids) 
        save_to_json(video_data) 

    except Exception as e:
        print(f"An error occurred in the main process: {e}")