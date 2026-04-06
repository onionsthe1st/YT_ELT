import requests
import json
import os 
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_NAME = "Judeinggg"
max_results = 50
batch_size = 50
def get_channel_id():

    try:
        url = (f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_NAME}&key={API_KEY}")



        response = requests.get(url)

        response.raise_for_status()
        data = response.json()

        # print(json.dumps(data, indent=4))

        CHANNEL_ITEMS= data["items"][0]
        CHANNEL_PLAYLISTID= CHANNEL_ITEMS["contentDetails"]["relatedPlaylists"]["uploads"]
        # print(CHANNEL_PLAYLISTID)

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

            # session = requests.Session()
            # retry = Retry(total=3, backoff_factor=2)
            # adapter = HTTPAdapter(max_retries=retry)
            # session.mount("https://", adapter)

            # response = session.get(url)

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

def extract_video_stats(video_id):
    extracted_stats = []
   
    def batch_video_ids(video_ids, batch_size):
        for video_id in range(0, len(video_ids), batch_size):
            yield video_ids[video_id:video_id + batch_size]
    
    for batch in batch_video_ids(video_id, batch_size):
        video_ids_str = ",".join(batch)
        video_stats_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet&part=contentDetails&part=statistics&id={video_ids_str}&key={API_KEY}"

        try:
            response = requests.get(video_stats_url)

            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id" : video_id,
                    "channel_title" : snippet.get("channelTitle"),
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "duration": contentDetails.get("duration"),
                    "publishedAt": snippet.get("publishedAt"),
                    "view_count": statistics.get("viewCount"),
                    "definition": contentDetails.get("definition"),
                }

                extracted_stats.append(video_data)

            return extracted_stats




        except requests.exceptions.RequestException as e:
            raise e






if __name__ == "__main__":
    playlist_id = get_channel_id()  
    videoID = get_video_id(playlist_id)
    extracted_videos = extract_video_stats(videoID)
    print(extracted_videos)