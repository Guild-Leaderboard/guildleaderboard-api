import asyncio
import json
import time
from typing import TYPE_CHECKING

import redis.asyncio as redis

if TYPE_CHECKING:
    from database2 import Database2

PWD = "Soj/v1twd66n8xtH1Pd8lMbDrVzYqwn4JdKHfvXtv/aYvK3NjyY0hhSNDgwLldJqj3HIDahmWMT5bKV2"


class Cache:
    def __init__(self, app=None):
        self.app = app
        self.db: Database2 = app.db
        self.rd = redis.Redis(host='195.201.43.165', port=6379, password=PWD, decode_responses=True)

        self.update_times = {
            "guilds": [300, 0],
            "stats": [3600, 0],
        }

    async def jget(self, key):
        return json.loads(await self.rd.get(key))


    async def update_cache(self):
        while True:
            if time.time() - self.update_times["guilds"][1] >= self.update_times["guilds"][0]:
                await self.rd.set("guilds", json.dumps(self.db.get_guilds()))
                self.update_times["guilds"][1] = time.time()

            if time.time() - self.update_times["stats"][1] >= self.update_times["stats"][0]:
                r = await self.jget("guilds")
                if time.time() - self.app.patreon_last_get >= 3600:
                    self.app.patrons = await self.app.get_patreon_members()
                    self.app.patreon_last_get = time.time()

                await self.rd.set("stats", json.dumps({
                    "guilds_tracked": len(r),
                    "players_tracked": sum([i["members"] for i in r]),
                    "patrons": self.app.patrons,
                    "top_guilds": sorted(r, key=lambda x: x["weighted_stats"].split(",")[6], reverse=True)[:3],
                }))
                self.update_times["stats"][1] = time.time()

            await asyncio.sleep(5)


if __name__ == "__main__":
    c = Cache("1")
    t = time.time()
    c.rd.set("test", "test")
    print(c.rd.get("test"))
    print(time.time() - t)
