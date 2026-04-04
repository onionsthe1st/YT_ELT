import requests
import json
import os 
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")
API_KEY = os.getenv("API_KEY")
CHANNEL_NAME = "Judeinggg"

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

if __name__ == "__main__":
    get_channel_id()    