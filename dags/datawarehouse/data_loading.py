import json
from datetime import date
import logging

logger = logging.getLogger(__name___)

def load_path():
    filepath = f"./data/yt_video_stats({date.today()}).json"

    try:
        logger.info(f"Loading data from yt_video_stats({date.today()})")
        with open(filepath, "r", encoding="utf-8") as raw_json_file:
            data = json.load(raw_json_file) 
            return data
    except FileNotFoundError:
        logger.error(f"{filepath} not found.")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {filepath}.")
        raise
