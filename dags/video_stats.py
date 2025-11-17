import requests
import json
from datetime import date
import os
from airflow.decorators import task
from airflow.models import Variable

# CRITICAL: Do NOT access Variable.get() at module level!
# Only access inside task functions

maxResults = 50


@task
def get_playlist_id(channel_handle):
    """Get the uploads playlist ID for a YouTube channel"""
    # Access API_KEY inside the task
    API_KEY = Variable.get("API_KEY")
    
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={API_KEY}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('items'):
            raise ValueError(f"No channel found for handle: {channel_handle}")
            
        channel_playlistId = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        print(f"✓ Found playlist ID: {channel_playlistId}")
        return channel_playlistId
        
    except requests.exceptions.RequestException as e:
        print(f"✗ API request failed: {e}")
        raise e
    except (KeyError, IndexError) as e:
        print(f"✗ Failed to parse response: {e}")
        raise e


@task
def get_video_ids(playlistId):
    """Fetch all video IDs from a playlist (handles pagination)"""
    API_KEY = Variable.get("API_KEY")
    
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
            
            pageToken = data.get('nextPageToken')
            if not pageToken:
                break
                
        print(f"✓ Found {len(video_ids)} videos")
        return video_ids
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to fetch video IDs: {e}")
        raise e


@task
def extract_video_data(video_ids):
    """Extract detailed statistics for all videos"""
    API_KEY = Variable.get("API_KEY")
    extracted_data = []
    
    def batch_list(video_id_list, batch_size):
        """Split video IDs into batches"""
        for i in range(0, len(video_id_list), batch_size):
            yield video_id_list[i:i + batch_size]
    
    try:
        total_batches = (len(video_ids) + maxResults - 1) // maxResults
        
        for batch_num, batch in enumerate(batch_list(video_ids, maxResults), 1):
            video_ids_str = ",".join(batch)
            
            # FIXED: Use the batch of video IDs, not a hardcoded single ID
            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails,snippet,statistics&id={video_ids_str}&key={API_KEY}'
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get('items', []):
                video_data = {
                    'video_id': item['id'],
                    'title': item['snippet']['title'],
                    'publishedAt': item['snippet']['publishedAt'],
                    'duration': item['contentDetails']['duration'],
                    'viewCount': item['statistics'].get('viewCount'),
                    'likeCount': item['statistics'].get('likeCount'),
                    'commentCount': item['statistics'].get('commentCount')
                }
                extracted_data.append(video_data)
            
            print(f"✓ Processed batch {batch_num}/{total_batches} ({len(batch)} videos)")
                
        print(f"✓ Extracted data for {len(extracted_data)} videos")
        return extracted_data
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to extract video data: {e}")
        raise e


@task
def save_to_json(extracted_data):
    """Save extracted data to JSON file"""
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)

    file_name = f'video_data_{date.today()}.json'
    file_path = os.path.join(data_dir, file_name)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as json_outfile:
            json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
        print(f"✓ Data saved to {file_path} ({len(extracted_data)} videos)")
        return file_path
        
    except IOError as e:
        print(f"✗ Error saving data to {file_path}: {e}")
        raise e


# For local testing only (not used by Airflow)
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=".env")
    
    # When testing locally, set these as environment variables
    channel_handle = os.getenv("CHANNEL_HANDLE", "MrBeast")
    
    try:
        print(f"Testing pipeline for channel: {channel_handle}")
        uploads_playlist_id = get_playlist_id.function(channel_handle)
        video_ids = get_video_ids.function(uploads_playlist_id)
        video_data = extract_video_data.function(video_ids)
        save_to_json.function(video_data)
        print("✓ Test completed successfully!")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")