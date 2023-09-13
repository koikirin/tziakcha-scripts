import asyncio
import json
import sys
import time
from typing import Any, Coroutine, List

import httpx
import pymongo
import pymongo.errors
from pymongo import MongoClient


with open("config.json") as f:
    config = json.load(f)


_client: MongoClient = MongoClient(config["database"])
_db = _client.get_database(config["updater"]["database"])
_table = _db.get_collection(config["updater"]["collection"])


async def gather(*coroutines: Coroutine,
                 num_workers: int = 0,
                 return_exceptions: bool = ...):
    if not num_workers:
        return await asyncio.gather(*coroutines,
                                    return_exceptions=return_exceptions)

    queue = asyncio.Queue(maxsize=num_workers)
    # lock = asyncio.Lock()
    index = 0
    result: List[Any] = [None] * len(coroutines)
    future = asyncio.get_event_loop().create_future()

    async def worker():
        while True:
            nonlocal index
            work = await queue.get()
            # async with lock:
            i = index
            index += 1
            try:
                result[i] = await work
            except Exception as e:
                if return_exceptions:
                    result[i] = e
                else:
                    future.set_exception(e)
                    queue.task_done()

            queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]

    for coroutine in coroutines:
        await asyncio.wait([asyncio.create_task(queue.put(coroutine)), future],
                           return_when=asyncio.FIRST_COMPLETED)

    await asyncio.wait([asyncio.create_task(queue.join()), future],
                       return_when=asyncio.FIRST_COMPLETED)

    future.cancel()

    for w in workers:
        w.cancel()

    return result



def parse_cfg_bit_to_time_limit(cfg_num):
    cfg_bit = f'{cfg_num:016x}'
    cfg_l = int(cfg_bit[8:], 16)
    cfg_h = int(cfg_bit[:8], 16)

    time_limit = ((cfg_l >> 20) & 0xf) * 5
    return time_limit


def convert_game_to_doc(resp_json):
    def cvt_json(j):
        cfg_bit = f'{j["cfg_bit"]:016x}'
        cfg_l = int(cfg_bit[8:], 16)
        cfg_h = int(cfg_bit[:8], 16)
        g = {
            "t": j["title"],
            "n": j["rd_cnt"],
            "d": bool(cfg_l & 0x80),
            "i": j["init_score"],
            "l": (cfg_h >> 24) & 0xff,
            "b": (cfg_h >> 16) & 0xff,
            "r12": (cfg_h >> 8) & 0xff,
            "r30": (cfg_h) & 0xff,
            "e": (cfg_l >> 24) & 0xff,
            "lt": (cfg_l >> 20) & 0xf,
            "d12": bool(cfg_l & 0x8000),
            "fa": bool(cfg_l & 0x2000),
            "fc": bool(cfg_l & 0x1000),
            "s": bool(cfg_l & 0x800),
            "o": bool(cfg_l & 0x400),
            "a": bool(cfg_l & 0x200),
            "r": bool(cfg_l & 0x100),
        }
        u = list({
                "i": j[f"uid{i}"],
                "n": j[f"name{i}"],
                "g": j[f"gender{i}"],
                "s": j[f"score{i}"],
                "r": j.get(f"rank{i}"),
            } for i in range(4))
        rd = list({
            "f": j[f"rid{i:02}"],   
            "rbf": j[f"rbf{i:02}"],
            "rnk": j.get(f"rnk{i:02}"),
        } for i in range(j["rd_idx"] + 1))

        return {
            "g": g,
            "rd": rd,
            "u": u,
            "st": j["start_time"],
            "et": j["finish_time"]
        }

    def calc_rs(v, b):
        a = [0, 0, 0, 0]
        w, c, t = None, None, None
        f = v >> 16
        if f:
            for i in range(4):
                if v & (1 << i): w = i
                if v & (1 << (i + 4)): c = i
            if w != c:
                s = -b
                for i in range(4):
                    if (i == w): a[i] = f + b * 3
                    elif i == c: a[i] = s - f
                    else: a[i] = s
            else:
                s = b + f
                t = s * 3
                s = -s
                for i in range(4):
                    a[i] = s if i != w else t
        return a

    def calc_rp(v, f):
        a = [0, 0, 0, 0]
        c = 0
        for i in range(4):
            if (v & (1 << (i + 8))):
                a[i] -= 40
                c += 1
        if (c and f):
            t = c * 10
            for i in range(4):
                a[i] += t
        return a


    def calc_rounds(j):
        rounds = []
        bs = j["g"].get("b", 8)
        fa = j["g"]["fa"]
        for rd in j["rd"]:
            rv = rd["rbf"]
            rs = calc_rs(rv, bs)
            rp = calc_rp(rv, fa)
            rounds.append({
                "rs": rs,
                "rp": rp
            })
        return rounds

    doc = cvt_json(resp_json)
    doc["_id"] = resp_json["id"]
    # doc["rounds"] = calc_rounds(doc)

    return doc


def check_game_finished(game):
    time_limit = parse_cfg_bit_to_time_limit(game["cfg_bit"])
    time_used = game["finish_time"] - game["start_time"]
    ret = game["rd_cnt"] == game["rd_idx"] + 1 or (
            time_limit and time_used >= time_limit * 1000 * 60)
    return bool(ret)


class Tziakcha:
    def __init__(self) -> None:
        self._session = httpx.AsyncClient()
        self._login = False
        self._account = config["updater"]["account"]
        self._password = config["updater"]["password"]

    async def login(self):
        if self._login: return
        print("Login...")
        headers = {
            "Referer": "http://www.tziakcha.xyz/login/",
            "Origin": "http://www.tziakcha.xyz"
        }
        resp = await self._session.get("http://www.tziakcha.xyz/login/",
                                 headers=headers)
        resp.raise_for_status()
        resp = await self._session.post("http://www.tziakcha.xyz/_qry/puzzle/",
                                  headers=headers)
        resp.raise_for_status()
        resp = await self._session.post("http://www.tziakcha.xyz/_ops/login/",
                                  data={
                                      "usr": self._account,
                                      "psw": self._password,
                                      "pzl": "",
                                      "sln": ""
                                  },
                                  headers=headers)
        resp.raise_for_status()
        self._login = True

    async def fetch_record_page(self, p: int):
        headers = {
            "Referer": "https://www.tziakcha.xyz/history/",
            "Origin": "https://www.tziakcha.xyz"
        }
        resp = await self._session.post("https://www.tziakcha.xyz/_qry/history/",
                                  data={"p": p},
                                  headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def fetch_record(self, game_id: int):
        # self.login()

        headers = {
            "Referer": f"https://www.tziakcha.xyz/game/?id={game_id}",
            "Origin": "https://www.tziakcha.xyz"
        }
        resp = await self._session.post(f"https://www.tziakcha.xyz/_qry/game/?id={game_id}", 
                                  headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._session.aclose()


async def update_record(game_id, force_unfinished: bool = False):
    try:
        game = await _server.fetch_record(game_id)
    except:
        print(f"Error: {game_id}")
        return None
    if not check_game_finished(game):
        print(f"Found: {game_id}")
        if not force_unfinished:
            return None
    return convert_game_to_doc(game)


async def update_records_hole(last_id=10001):
    global _server
    _server = Tziakcha()
    await _server.login()
    cursor = _table.aggregate([
        { "$addFields": { "aSize": { "$size": "$rd"}, "bSize": "$g.n" }},
        { "$addFields": { "aEq": {"$eq": ["$aSize","$bSize"]}}},
        { "$match": { "aEq": False }},
    ])
    holes = list(d["_id"] for d in cursor if d["_id"] >= last_id)
    # return
    # ids = _table.distinct("_id")
    # holes = set(range(10001, 62528)) - set(ids)
    print(sorted(holes))
    coros = (update_record(gid) for gid in holes)
    docs = await gather(*coros, num_workers=10)
    docs = list(filter(lambda x: x, docs))
    print(len(docs))
    if not docs: return
    requests = []
    for doc in docs:
        requests.append(pymongo.UpdateOne({"_id": doc["_id"]}, {"$set":  doc}))
    # _table.bulk_write()
    try:
        r = _table.bulk_write(requests, ordered=False)
        print(r.bulk_api_result)
    except pymongo.errors.BulkWriteError as e:
        assert isinstance(e.details, dict)
        errd = {}
        for key in e.details:
            if key.startswith("n"):
                errd[key] = e.details[key]
        print(errd)
    await _server.close()


async def update_records_fix(last_id=10001):
    global _server
    _server = Tziakcha()
    await _server.login()
    ids = _table.distinct("_id")
    holes = set(range(last_id, max(ids))) - set(ids)
    print(sorted(holes))
    coros = (update_record(gid) for gid in holes)
    docs = await gather(*coros, num_workers=10)
    docs = list(filter(lambda x: x, docs))
    print(len(docs))
    if not docs: return
    requests = []
    for doc in docs:
        requests.append(pymongo.UpdateOne({"_id": doc["_id"]}, {"$set":  doc}, upsert=True))
    # _table.bulk_write()
    try:
        r = _table.bulk_write(requests, ordered=False)
        print(r.bulk_api_result)
    except pymongo.errors.BulkWriteError as e:
        assert isinstance(e.details, dict)
        errd = {}
        for key in e.details:
            if key.startswith("n"):
                errd[key] = e.details[key]
        print(errd)
    await _server.close()


async def update_records_parallel():
    global _server
    _server = Tziakcha()
    await _server.login()
    r = range(40000, 48000)
    coros = (update_record(gid) for gid in r)
    docs = await gather(*coros, num_workers=10)
    docs = list(filter(lambda x: x, docs))
    try:
        _table.insert_many(docs, ordered=False)
    except pymongo.errors.BulkWriteError as e:
        assert isinstance(e.details, dict)
        errd = {}
        for key in e.details:
            if key.startswith("n"):
                errd[key] = e.details[key]
        print(errd)
        
    await _server.close()

async def update_records():
    print(
        f"--- Start. Current Time: {time.strftime(r'%Y-%m-%d %H:%M:%S', time.localtime())}."
    )
    global _server
    _server = Tziakcha()

    # Get last match id from db
    cursor = _table.find({}).sort("_id", pymongo.DESCENDING).limit(20)
    last_ids = list(doc["_id"] for doc in cursor)
    if not last_ids:
        last_ids = [-1]

    # Fetch records
    appended_cnt = 0
    p = 0
    stop_flag = False
    while True:

        if p == 1:
            await _server.login()

        page = await _server.fetch_record_page(p)
        print(
            f"Fetching page {p}. {page['p']} - {page['cnt']} - {page['total']}"
        )
        if page["p"] != p:
            print(f"Fetch {p} but get {page['p']}, exit.")
            break
        up_id = page["games"][0]["id"]
        if up_id <= last_ids[-1]:
            break
        else:
            docs_to_insert = []
            for game in page["games"]:
                if game["id"] > last_ids[-1]:
                    if game["id"] in last_ids:
                        continue
                    time_limit = parse_cfg_bit_to_time_limit(game["cfg_bit"])
                    time_used = game["finish_time"] - game["start_time"]
                    if game["rd_cnt"] == game["rd_idx"] + 1 or (
                            time_limit
                            and time_used >= time_limit * 1000 * 60):
                        game2 = await _server.fetch_record(game["id"])
                        docs_to_insert.append(convert_game_to_doc(game2))
                        appended_cnt += 1
                else:
                    stop_flag = True
                    break
            if page["total"] <= (page["p"] + 1) * page["cnt"]:
                stop_flag = True
            if docs_to_insert:
                try:
                    _table.insert_many(docs_to_insert, ordered=False)
                except pymongo.errors.BulkWriteError as e:
                    assert isinstance(e.details, dict)
                    errd = {}
                    for key in e.details:
                        if key.startswith("n"):
                            errd[key] = e.details[key]
                    print(errd)
        if stop_flag:
            break
        p += 1

    print(f"[{appended_cnt}] Finished. Appended {appended_cnt} records.")
    await _server.close()


if sys.argv[-1] == "holes":
    asyncio.run(update_records_hole())
elif len(sys.argv) >= 2 and sys.argv[-2] == "holes":
    asyncio.run(update_records_hole(int(sys.argv[-1])))
elif sys.argv[-1] == "fix":
    asyncio.run(update_records_fix())
elif len(sys.argv) >= 2 and sys.argv[-2] == "fix":
    asyncio.run(update_records_fix(int(sys.argv[-1])))
else:
    asyncio.run(update_records())


_client.close()
