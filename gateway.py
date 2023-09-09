import asyncio
import json
import logging
import sys
import time
from typing import Dict

import pymongo
import requests
import websockets
import websockets.client
import websockets.exceptions
from pymongo import MongoClient

logger = logging.getLogger("tziakcha-gateway")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


with open("config.json") as f:
    config = json.load(f)


_client: MongoClient
_connection_str = "mongodb://127.0.0.1:27017/"
_client = MongoClient(_connection_str)
_db = _client.get_database("majob")
_table = _db.get_collection("tziakcha_room")


class Tziakcha:
    def __init__(self) -> None:
        self._session = requests.Session()
        self.closed = False
        self._account = config["gateway"]["account"]
        self._password = config["gateway"]["password"]
        self.waitings: Dict[int, dict] = {}
        self.heartbeat: int = 0

    def login(self):

        headers = {
            "Referer": "http://www.tziakcha.xyz/login/",
            "Origin": "http://www.tziakcha.xyz"
        }
        resp = self._session.get("http://www.tziakcha.xyz/login/",
                                 headers=headers)
        resp.raise_for_status()
        resp = self._session.post("http://www.tziakcha.xyz/_qry/puzzle/",
                                  headers=headers)
        resp.raise_for_status()
        resp = self._session.post("http://www.tziakcha.xyz/_ops/login/",
                                  data={
                                      "usr": self._account,
                                      "psw": self._password,
                                      "pzl": "",
                                      "sln": ""
                                  },
                                  headers=headers)
        resp.raise_for_status()

    def fetch_record_page(self, p: int):
        headers = {
            "Referer": "http://www.tziakcha.xyz/history/",
            "Origin": "http://www.tziakcha.xyz"
        }
        resp = self._session.post("http://www.tziakcha.xyz/_qry/history/",
                                  data={"p": p},
                                  headers=headers)
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self._session.close()

    async def task(self):
        retries = 0
        while not self.closed:
            try:
                retries += 1
                if retries > 10:
                    sys.exit(1)
                logger.info(f"Starting ... :{retries}:")
                self.waitings.clear()
                async with websockets.client.connect(
                        "ws://www.tziakcha.xyz:5333/ws") as ws:
                    logger.info("Login ...")
                    last_beat_time = time.time()

                    login_info = {
                        "m": 1,
                        "p": self._password,
                        "r": 9,
                        "s": "",
                        "u": self._account,
                        "z": "",
                    }

                    await ws.send(json.dumps(login_info))

                    await ws.send(json.dumps({
                        "m": 1,
                        "r": 2,
                    }))

                    while not self.closed and not ws.closed:
                        try:
                            if time.time() > last_beat_time + 60:
                                if self.heartbeat != 0:
                                    logger.error(
                                        "Heartbeat failed, reconnect ...")
                                    break
                                last_beat_time = time.time()
                                self.heartbeat = int(last_beat_time * 1000)
                                await ws.send(
                                    json.dumps({
                                        "m": 5,
                                        "t": self.heartbeat
                                    }))
                            res = await asyncio.wait_for(ws.recv(), 7)
                            res = await self._on_receive(res)
                            if res == -2:
                                logger.error(
                                    "Mismatched heartbeat, reconnect ...")
                                break

                        except asyncio.TimeoutError:
                            continue
                        except TypeError:
                            continue
                        except Exception as e:
                            raise

                    if not ws.closed:
                        await ws.close()
            except websockets.exceptions.WebSocketException as e:
                logger.error(f"Connection failed. Retrying ... {e}")
                await asyncio.sleep(10)
            except Exception as e:
                self.closed = True
                logger.exception(e)

    def _pack_match(self, data):

        res = {
            "_id": f'{data["t"]}.{data["i"]}',
            "id": data["i"],
            "create_time": data["t"],
            "finish_time": data["e"],
            "title": data["g"]["t"],
            "rd_idx": data["n"] - 1,
            "rd_cnt": data["g"]["n"],
            "cfg_bit": 0,
            "players": []
        }
        for p in data["p"]:
            if p:
                res["players"].append({
                    "name": p["n"],
                    "vip": p.get("v", 0),
                    "level": 0,
                    "gender": 0,
                    "score": 0,
                })
            else:
                res["players"].append(None)
        if data["n"] == 0:
            logger.debug(f"Wait: {res['title']}")
            self.waitings[res["id"]] = res
            return
        res['start_time'] = int(time.time() * 1000)
        logger.info(
            f"Add: {res['title']} ({', '.join(p['name'] for p in res['players'])})"
        )
        return res

    def _join_match(self, data):
        mid = data["t"]["i"]
        if mid in self.waitings:
            self.waitings[mid]["players"][data["t"]["s"]] = {
                "name": data["t"]["n"],
                "vip": data["t"].get("v", 0),
                "level": 0,
                "gender": 0,
                "score": 0,
            }

    def _exit_match(self, data):
        mid = data["t"]["i"]
        if mid in self.waitings:
            self.waitings[mid]["players"][data["t"]["s"]] = None

    def _dissmis_match(self, data):
        mid = data["t"]["i"]
        if mid in self.waitings:
            self.waitings.pop(mid)

    def _start_match(self, data):
        mid = data["i"]
        if mid in self.waitings:
            if all(self.waitings[mid]["players"]):
                res = self.waitings.pop(mid)
                res["rd_idx"] = 0
                res['start_time'] = int(time.time() * 1000)
                logger.info(
                    f"Add: {res['title']} ({', '.join(p['name'] for p in res['players'])})"
                )
                return res

    async def _on_receive(self, data) -> int:
        obj: dict = json.loads(data)
        method = obj.get('m')
        # logger.debug(f"Recv: {obj.get('m', 0)}:{obj.get('r', 0)}")
        if method == 5:
            if obj.get("t") == self.heartbeat:
                # Heartbeat recv
                # logger.debug("Heartbeat recv")
                self.heartbeat = 0
            else:
                return -2
        elif method == 1 and obj.get("r") == 2:
            # Fetch match list resp
            livelist = list(
                filter(lambda x: x, map(self._pack_match, obj.get("t", []))))
            try:
                _table.insert_many(livelist, ordered=False)
            except Exception as e:
                pass
        elif method == 1 and obj.get("r") == 3:
            # Create match
            livelist = list(
                filter(lambda x: x, map(self._pack_match, obj.get("t", []))))
            try:
                _table.insert_many(livelist, ordered=False)
            except Exception as e:
                pass
        elif method == 1 and obj.get("r") == 4:
            # Join match
            self._join_match(obj)
        elif method == 1 and obj.get("r") == 5:
            # Exit match
            self._exit_match(obj)
        elif method == 1 and obj.get("r") == 6:
            # Ready
            pass
        elif method == 1 and obj.get("r") == 7:
            # Dismiss match
            self._dissmis_match(obj)
        elif method == 1 and obj.get("r") == 13 and obj.get("p") == 1:
            # Start match
            res = self._start_match(obj)
            if res:
                _table.insert_one(res)
        return 0


_server = Tziakcha()

asyncio.run(_server.task())

_client.close()
_server.close()
