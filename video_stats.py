import requests
import json
import os 
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_NAME = "Judeinggg"
max_results = 50

def get_channel_id():

    try:
        url = (f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_NAME}&key={API_KEY}")

        response = requests.get(url)

        response.raise_for_status()
        data = response.json()

        print(json.dumps(data, indent=4))

        CHANNEL_ITEMS= data["items"][0]
        CHANNEL_PLAYLISTID= CHANNEL_ITEMS["contentDetails"]["relatedPlaylists"]["uploads"]
        print(CHANNEL_PLAYLISTID)

        return CHANNEL_PLAYLISTID

    except requests.exceptions.RequestException as e:
        raise e




def get_video_id(playlist_id):
    video_ids=[]
    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={API_KEY}" 
    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)

            response.raise_for_status()
            data = response.json()    

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)   

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e

len_video_ids = len(get_video_id(get_channel_id()))
print(f"Total number of videos: {len_video_ids}")

if __name__ == "__main__":
    playlist_id = get_channel_id()  
    print(get_video_id(playlist_id))
