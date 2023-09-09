import sys
import time

import pymongo
import requests
from pymongo import MongoClient

_client: MongoClient
_connection_str = "mongodb://127.0.0.1:27017/"
_client = MongoClient(_connection_str)
_db = _client.get_database("majob")
_table = _db.get_collection("tziakcha")


class Tziakcha:
    def __init__(self) -> None:
        self._session = requests.Session()

    def fetch_record_page(self, p: int):
        headers = {
            "Referer": "https://www.tziakcha.xyz/history/",
            "Origin": "https://www.tziakcha.xyz"
        }
        resp = self._session.post("https://www.tziakcha.xyz/_qry/history/",
                                  data={"p": p},
                                  headers=headers)
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self._session.close()


_server = Tziakcha()


def convert_game_to_doc(game: dict):
    doc = {"_id": game["id"], **game}
    doc["players"] = []
    for i in range(4):
        doc["players"].append({
            "name": game[f"name{i}"],
            "gender": game[f"gender{i}"],
            "level": game[f"level{i}"],
            "score": game[f"score{i}"],
        })
    return doc


def update_records():
    print(
        f"Start. Current Time: {time.strftime(r'%Y-%m-%d %H:%M:%S', time.localtime())}."
    )

    # Get last match id from db
    cursor = _table.find({}).sort("id", pymongo.DESCENDING).limit(1)
    for doc in cursor:
        last_id = doc["id"]
        break
    else:
        last_id = -1

    # Fetch records
    appended_cnt = 0

    page = _server.fetch_record_page(0)

    docs_to_insert = []
    for game in page["games"]:
        if game["id"] > last_id:
            if game["rd_cnt"] > game["rd_idx"] + 1:
                docs_to_insert.append(convert_game_to_doc(game))
                appended_cnt += 1
        else:
            break
    if docs_to_insert:
        _table.insert_many(docs_to_insert)

    print(f"Finished. Appended {appended_cnt} records.")


def loop():
    while True:
        update_records()
        time.sleep(60)


# update_records()
try:
    loop()
except KeyboardInterrupt:
    pass

_client.close()
_server.close()
