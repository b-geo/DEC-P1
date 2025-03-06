import pandas as pd
from main.assets.helpers import current_round
from datetime import datetime

a = pd.json_normalize(
        [{
        "game_id": 37090,
        "game_data_updated": "2024-11-14 17:08:26",
        "game_time": "2024-03-06 18:50:00",
        "home team": "Brisbane Lions",
        "away team": "Geelong",
        "complete_perc": 0,
        "round": 0,
        "tz":"+11:00"
        },
        {
        "game_id": 37091,
        "game_data_updated": "2024-11-14 17:08:26",
        "game_time": "2024-04-06 18:50:00",
        "home team": "Brisbane Lions",
        "away team": "Geelong",
        "complete_perc": 0,
        "round": 1,
        "tz":"+11:00"
        }])

print(current_round(a))

game_time = "2024-11-14 17:08:26+11:00"
game_time = datetime.strptime(game_time, "%Y-%m-%d %H:%M:%S%z")
date_object = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"+11:00", "%Y-%m-%d %H:%M:%S%z")

print(game_time < date_object)